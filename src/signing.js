import { SignatureV4 } from "@smithy/signature-v4";

/**
 * WebCrypto-based SHA-256/HMAC-SHA-256 compatible with @smithy/signature-v4's hash interface.
 * When constructed with a secret, operates in HMAC mode.
 */
class Sha256 {
  constructor(secret) {
    this._chunks = [];
    this._secret = secret;
  }
  update(data) {
    if (typeof data === "string") {
      this._chunks.push(new TextEncoder().encode(data));
    } else {
      this._chunks.push(data);
    }
  }
  async digest() {
    const totalLen = this._chunks.reduce((sum, c) => sum + c.byteLength, 0);
    const buf = new Uint8Array(totalLen);
    let offset = 0;
    for (const chunk of this._chunks) {
      buf.set(chunk, offset);
      offset += chunk.byteLength;
    }
    if (this._secret) {
      const keyData = typeof this._secret === "string"
        ? new TextEncoder().encode(this._secret)
        : this._secret;
      const key = await crypto.subtle.importKey(
        "raw", keyData, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
      );
      const sig = await crypto.subtle.sign("HMAC", key, buf);
      return new Uint8Array(sig);
    }
    const hash = await crypto.subtle.digest("SHA-256", buf);
    return new Uint8Array(hash);
  }
}

/**
 * Compute SHA-256 hex hash of a body string (for pre-computing payload hash).
 */
export async function hashBody(body) {
  const data = typeof body === "string" ? new TextEncoder().encode(body) : body;
  const hash = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(hash), b => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Sign an S3-compatible HTTP request for Linode Object Storage using AWS Signature V4.
 *
 * @param {object} params
 * @param {string} params.method   - HTTP method (e.g. "PUT")
 * @param {string} params.hostname - Full bucket hostname (e.g. "surewrite-3.us-iad-10.linodeobjects.com")
 * @param {string} params.path     - Object key path (e.g. "/oficial/ele2024/...")
 * @param {string} params.region   - S3-compatible region (e.g. "us-iad-1")
 * @param {string} params.body     - Request body
 * @param {object} credentials     - { accessKeyId, secretAccessKey }
 * @returns {Promise<{ url: string, headers: Record<string, string> }>}
 */
export async function signRequest({ method, hostname, path, region, body, payloadHash }, credentials) {

  const signer = new SignatureV4({
    service: "s3",
    region,
    credentials,
    sha256: Sha256,
    applyChecksum: !payloadHash,
  });

  const headers = {
    host: hostname,
    "content-type": "application/json",
  };
  if (payloadHash) {
    headers["x-amz-content-sha256"] = payloadHash;
  }

  const request = {
    method,
    protocol: "https:",
    hostname,
    path,
    headers,
    body
  };

  const signed = await signer.sign(request);

  return {
    url: `https://${hostname}${path}`,
    headers: signed.headers,
  };
}
