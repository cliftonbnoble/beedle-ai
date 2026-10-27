export async function sha256Bytes(bytes: Uint8Array): Promise<string> {
  return Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes as BufferSource)), byte => byte.toString(16).padStart(2, "0")).join("");
}
