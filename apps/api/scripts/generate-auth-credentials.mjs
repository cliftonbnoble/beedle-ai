import { pbkdf2Sync, randomBytes } from "node:crypto";
import readline from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";

const iterations = Number(process.env.AUTH_HASH_ITERATIONS || "120000");
if (!Number.isInteger(iterations) || iterations < 100000) {
  throw new Error("AUTH_HASH_ITERATIONS must be an integer >= 100000");
}

const pipedLines = input.isTTY ? null : await readPipedLines();
const rl = input.isTTY ? readline.createInterface({ input, output }) : null;

try {
  const username = (pipedLines ? pipedLines[0] || "" : await rl.question("Username: ")).trim();
  if (!username) throw new Error("Username is required");
  const password = pipedLines ? pipedLines[1] || "" : await questionHidden("Password: ");
  if (!password) throw new Error("Password is required");

  const salt = randomBytes(16);
  const hash = pbkdf2Sync(password, salt, iterations, 32, "sha256");
  const passwordHash = `pbkdf2_sha256$${iterations}$${base64Url(salt)}$${base64Url(hash)}`;
  const sessionSecret = base64Url(randomBytes(32));

  output.write("\nSet these Cloudflare Worker secrets:\n\n");
  output.write(`AUTH_USERNAME=${username}\n`);
  output.write(`AUTH_PASSWORD_HASH=${passwordHash}\n`);
  output.write(`AUTH_SESSION_SECRET=${sessionSecret}\n\n`);
  output.write("Example:\n");
  output.write("pnpm --filter @beedle/api exec wrangler secret put AUTH_USERNAME\n");
  output.write("pnpm --filter @beedle/api exec wrangler secret put AUTH_PASSWORD_HASH\n");
  output.write("pnpm --filter @beedle/api exec wrangler secret put AUTH_SESSION_SECRET\n");
} finally {
  rl?.close();
}

function base64Url(bytes) {
  return Buffer.from(bytes).toString("base64url");
}

function questionHidden(prompt) {
  return new Promise((resolve) => {
    output.write(prompt);
    const chunks = [];
    const wasRaw = input.isRaw;
    if (input.isTTY) input.setRawMode(true);
    input.resume();
    input.setEncoding("utf8");

    function onData(char) {
      if (char === "\u0003") {
        cleanup();
        process.exit(130);
      }
      if (char === "\r" || char === "\n") {
        output.write("\n");
        cleanup();
        resolve(chunks.join(""));
        return;
      }
      if (char === "\u007f") {
        chunks.pop();
        return;
      }
      chunks.push(char);
    }

    function cleanup() {
      input.off("data", onData);
      if (input.isTTY) input.setRawMode(Boolean(wasRaw));
      input.pause();
    }

    input.on("data", onData);
  });
}

function readPipedLines() {
  return new Promise((resolve, reject) => {
    let body = "";
    input.setEncoding("utf8");
    input.on("data", (chunk) => {
      body += chunk;
    });
    input.on("end", () => {
      resolve(body.split(/\r?\n/));
    });
    input.on("error", reject);
  });
}
