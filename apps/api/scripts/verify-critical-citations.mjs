const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const citations = (process.env.CRITICAL_CITATIONS || "37.2(g),37.3(a)(1),37.15,1.11,6.13,10.10(c)(3),13.14")
  .split(",")
  .map((item) => item.trim())
  .filter(Boolean);

async function main() {
  const response = await fetch(`${apiBase}/admin/references/verify-citations`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ citations })
  });
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  if (!response.ok) {
    throw new Error(`verification failed: ${response.status} ${JSON.stringify(body)}`);
  }

  const summary = {
    total: body.checks.length,
    resolved: body.checks.filter((item) => item.status === "resolved").length,
    ambiguous: body.checks.filter((item) => item.status === "ambiguous").length,
    unresolved: body.checks.filter((item) => item.status === "unresolved").length
  };
  console.log("Critical citation summary:");
  console.log(JSON.stringify(summary, null, 2));
  console.log("Checks:");
  console.log(JSON.stringify(body.checks, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
