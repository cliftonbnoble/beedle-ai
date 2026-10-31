import type {Env} from '../lib/types';
import type {VectorJobMessage} from './vector-jobs';

type Profile=Pick<Env,'AI_EMBEDDING_MODEL'|'VECTOR_NAMESPACE'|'AI_EMBEDDING_DIMENSIONS'>;
// Core staging retains its verified outbox. This adapter has no provider or
// Queue binding and makes attempted delivery fail explicitly rather than
// fabricating a successful enqueue. Public request limiters always deny.
export function restorationReprocessEnv(database:D1Database,targetOriginals:R2Bucket,profile:Profile):Env{
  if(!profile.AI_EMBEDDING_MODEL||profile.AI_EMBEDDING_MODEL.length>200||
    !/^[A-Za-z0-9_-]{1,128}$/.test(profile.VECTOR_NAMESPACE)||!/^[1-9]\d{0,3}$/.test(profile.AI_EMBEDDING_DIMENSIONS)||
    Number(profile.AI_EMBEDDING_DIMENSIONS)>4096)throw Error('Explicit restoration vector profile required');
  const denyDelivery=()=>Promise.reject(new Error('Restoration vector dispatch is intentionally deferred; verified outbox retained'));
  const queueAdapter={send:denyDelivery,sendBatch:denyDelivery,
    metrics:()=>Promise.reject(new Error('Restoration has no Queue metrics capability'))};
  const queue:Queue<VectorJobMessage>=queueAdapter;
  const limiter:RateLimit={limit:()=>Promise.resolve({success:false})};
  return{
    DB:database,SOURCE_BUCKET:targetOriginals,...profile,VECTOR_JOBS_QUEUE:queue,
    SEARCH_RATE_LIMIT:limiter,INGEST_RATE_LIMIT:limiter,LLM_RATE_LIMIT:limiter,
    ADMIN_WRITE_RATE_LIMIT:limiter,REVIEW_READ_RATE_LIMIT:limiter,EXPORT_RATE_LIMIT:limiter,
    APP_ENV:'production',DEPLOYMENT_READY:'false',COMPUTE_BUDGETS_JSON:'{}',
    CORS_ALLOWED_ORIGINS:'',R2_PUBLIC_BASE_URL:'',SOURCE_PROXY_BASE_URL:'',
    VECTOR_LIFECYCLE_READY:'false',VECTOR_FILTERS_READY:'false',TELEMETRY_SUCCESS_SAMPLE_RATE:'0'
  };
}
