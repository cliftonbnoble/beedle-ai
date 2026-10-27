import { z } from "zod";
import type { Env } from "../lib/types";
import { RequestQuotaError, ServiceUnavailableError } from "../lib/http";

const limitSchema = z.object({ perMinute:z.number().int().min(1).max(1_000_000), perDay:z.number().int().min(1).max(10_000_000) }).strict();
const policySchema = z.object({llm:limitSchema,embedding:limitSchema,vector_write:limitSchema}).strict();
export const defaultComputeBudgets = {
  llm:{perMinute:20,perDay:500},
  embedding:{perMinute:400,perDay:50_000},
  vector_write:{perMinute:1600,perDay:100_000}
} as const;
export type ComputeCategory = keyof typeof defaultComputeBudgets;

function policy(env:Env) {
  try {
    // Deployed environments must declare their own reviewed limits. Defaults
    // also let isolated service fixtures exercise the real counter behavior.
    if (env.APP_ENV && !env.COMPUTE_BUDGETS_JSON) throw new Error("Missing compute budget configuration");
    return policySchema.parse(env.COMPUTE_BUDGETS_JSON ? JSON.parse(env.COMPUTE_BUDGETS_JSON) : defaultComputeBudgets);
  } catch { throw new ServiceUnavailableError("Compute budget policy unavailable"); }
}

export function computeBudgetBatchLimit(env:Env,category:ComputeCategory,requested:number) {
  const limits=policy(env)[category];
  return Math.min(requested,limits.perMinute,limits.perDay);
}

export async function reserveComputeBudget(env:Env,category:ComputeCategory,units=1) {
  const limits=policy(env)[category];
  if(!Number.isSafeInteger(units)||units<1||units>1000) throw new ServiceUnavailableError("Invalid compute reservation");
  if(units>limits.perMinute || units>limits.perDay) throw new ServiceUnavailableError("Compute batch exceeds configured policy");
  const scope=env.APP_ENV || "local";
  try {
    // D1's clock and a single conditional write prevent competing requests or
    // isolates from overspending either window. No read-then-write reservation.
    const result=await env.DB.prepare(`INSERT INTO compute_budget_counters
      (scope,category,minute_bucket,day_bucket,minute_used,day_used,updated_at)
      SELECT ?1,?2,CAST(strftime('%s','now') AS INTEGER)/60,CAST(strftime('%s','now') AS INTEGER)/86400,?3,?3,strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE ?3<=?4 AND ?3<=?5
      ON CONFLICT(scope,category) DO UPDATE SET minute_bucket=excluded.minute_bucket,day_bucket=excluded.day_bucket,
        minute_used=CASE WHEN compute_budget_counters.minute_bucket=excluded.minute_bucket THEN compute_budget_counters.minute_used+excluded.minute_used ELSE excluded.minute_used END,
        day_used=CASE WHEN compute_budget_counters.day_bucket=excluded.day_bucket THEN compute_budget_counters.day_used+excluded.day_used ELSE excluded.day_used END,
        updated_at=excluded.updated_at
      WHERE (CASE WHEN compute_budget_counters.minute_bucket=excluded.minute_bucket THEN compute_budget_counters.minute_used ELSE 0 END)+excluded.minute_used<=?4
        AND (CASE WHEN compute_budget_counters.day_bucket=excluded.day_bucket THEN compute_budget_counters.day_used ELSE 0 END)+excluded.day_used<=?5`)
      .bind(scope,category,units,limits.perMinute,limits.perDay).run();
    if(Number(result.meta.changes)===1) return;
    const current=await env.DB.prepare(`SELECT day_bucket,day_used,CAST(strftime('%s','now') AS INTEGER) AS clock
      FROM compute_budget_counters WHERE scope=? AND category=?`).bind(scope,category).first<{day_bucket:number;day_used:number;clock:number}>();
    const clock=current?.clock ?? Math.floor(Date.now()/1000);
    const dayBlocked=units>limits.perDay || (current?.day_bucket===Math.floor(clock/86400) && current.day_used+units>limits.perDay);
    throw new RequestQuotaError("Application compute budget reached. Please retry after the indicated reset.",Math.max(1,(dayBlocked?86400:60)-clock%(dayBlocked?86400:60)));
  } catch(error) {
    if(error instanceof RequestQuotaError){env.TELEMETRY?.tag("quota","compute_limited");throw error;}
    env.TELEMETRY?.tag("quota","budget_unavailable");
    throw new ServiceUnavailableError("Compute budget storage unavailable");
  }
}

export async function inspectComputeBudgets(env:Env) {
  const configured=policy(env);
  try {
    const counters=(await env.DB.prepare(`SELECT category,minute_bucket AS minuteBucket,day_bucket AS dayBucket,minute_used AS minuteUsed,day_used AS dayUsed,updated_at AS updatedAt
      FROM compute_budget_counters WHERE scope=? ORDER BY category`).bind(env.APP_ENV || "local").all()).results || [];
    return {scope:env.APP_ENV || "local",policy:configured,counters,units:{llm:"provider requests",embedding:"embedded texts",vector_write:"submitted vectors"}};
  } catch { throw new ServiceUnavailableError("Compute budget inspection unavailable"); }
}
