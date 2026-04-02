const apiBase = process.env.API_BASE_URL || "http://127.0.0.1:8787";
const limit = Number(process.env.BACKFILL_LIMIT || "1000");

async function main() {
  const response = await fetch(`${apiBase}/admin/references/backfill`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ limit })
  });
  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text };
  }
  if (!response.ok) {
    throw new Error(`Backfill failed: ${response.status} ${JSON.stringify(body)}`);
  }
  console.log("Reference backfill complete:");
  console.log(JSON.stringify(body, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
