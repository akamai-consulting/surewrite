import { SignatureV4 } from "@smithy/signature-v4";
import { Sha256 } from "@aws-crypto/sha256-js";

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
export async function signRequest({ method, hostname, path, region, body }, credentials) {
  const signer = new SignatureV4({
    service: "s3",
    region,
    credentials,
    sha256: Sha256,
  });

  const request = {
    method,
    protocol: "https:",
    hostname,
    path,
    headers: {
      host: hostname,
      "content-type": "application/json",
    },
    body,
  };

  const signed = await signer.sign(request);

  return {
    url: `https://${hostname}${path}`,
    headers: signed.headers,
  };
}
