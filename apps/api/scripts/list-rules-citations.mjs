const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const params = new URLSearchParams();

if (process.env.RULE_CITATION) params.set("citation", process.env.RULE_CITATION);
if (process.env.RULE_NORMALIZED) params.set("normalized", process.env.RULE_NORMALIZED);
if (process.env.RULE_BARE) params.set("bare", process.env.RULE_BARE);
if (process.env.RULE_PREFIX) params.set("prefix", process.env.RULE_PREFIX);
params.set("limit", process.env.RULE_LIMIT || "100");

async function main() {
  const endpoint = `${apiBase}/admin/references/rules${params.toString() ? `?${params.toString()}` : ""}`;
  const response = await fetch(endpoint);
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  if (!response.ok) {
    throw new Error(`Failed rules inventory: ${response.status} ${JSON.stringify(body)}`);
  }
  console.log(JSON.stringify(body, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
