
import { jenkinsHash } from "./jenkins.js";
import { signRequest } from "./signing.js";
import { get as getVariable } from "@spinframework/spin-variables";
import {
  POOL_SIZE,
  BUCKET_PREFIX,
  SHARDS,
  SHARD_RULES,
  DEFAULT_SHARD,
} from "./config.js";

const MAX_BODY_SIZE = 5 * 1024 * 1024; // 5 MB
const ALLOWED_PATH_PREFIX = "/oficial/";
const S3_TIMEOUT_MS = 5_000;     // 5s per S3 PUT
const ORIGIN_TIMEOUT_MS = 10_000; // 10s for origin fetch

/**
 * Generate a short random request ID for log correlation.
 */
function generateRequestId() {
  const bytes = new Uint8Array(6);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Load S3 credentials from Spin variables.
 */
function getCredentials() {
  return {
    accessKeyId: getVariable("s3_access_key_id"),
    secretAccessKey: getVariable("s3_secret_access_key"),
  };
}

/**
 * Read the ingest mode config flag.
 * "relay" — data is included in the POST body (default).
 * "fetch" — function pulls the object from a configured origin.
 */
function getIngestMode() {
  const mode = (getVariable("ingest_mode") || "relay").toLowerCase();
  return mode === "fetch" ? "fetch" : "relay";
}

/**
 * Fetch object content from the configured origin.
 */
async function fetchFromOrigin(rid, objectPath) {
  const origin = getVariable("origin_hostname");
  if (!origin) {
    throw new Error("origin_hostname is not configured");
  }
  const url = `https://${origin}${objectPath}`;
  console.log(`[${rid}][origin] fetching ${url}`);
  const res = await fetch(url, { signal: AbortSignal.timeout(ORIGIN_TIMEOUT_MS) });
  if (!res.ok) {
    throw new Error(`origin responded HTTP ${res.status} for ${url}`);
  }
  const body = await res.text();
  console.log(`[${rid}][origin] fetched ${body.length} bytes`);
  return body;
}

/**
 * Resolve the shard key (A, B, or C) for a given request path.
 */
function resolveShard(path) {
  for (const rule of SHARD_RULES) {
    if (rule.pattern.test(path)) {
      return rule.shard;
    }
  }
  return DEFAULT_SHARD;
}

/**
 * Extract the filename (last segment) from the path, stripping query strings.
 */
function extractFilename(path) {
  const clean = path.split("?")[0];
  const segments = clean.split("/");
  return segments[segments.length - 1];
}

/**
 * Execute a signed PUT to a Linode E3 bucket endpoint.
 */
async function signedPut(host, region, bucketIndex, objectPath, body, credentials) {
  const hostname = `${BUCKET_PREFIX}-${bucketIndex}.${host}`;
  const signed = await signRequest(
    { method: "PUT", hostname, path: objectPath, region, body },
    credentials
  );
  return fetch(signed.url, {
    method: "PUT",
    headers: signed.headers,
    body,
    signal: AbortSignal.timeout(S3_TIMEOUT_MS),
  });
}

/**
 * Random jittered delay (50–300ms) to avoid thundering-herd retries.
 */
function jitteredDelay() {
  const ms = 50 + Math.floor(Math.random() * 250);
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Wrap a fetch so it resolves with a tagged result object instead of rejecting.
 * A non-2xx response is treated as a failure (ok: false).
 * Retries once with a randomized delay on failure or timeout.
 */
async function taggedFetch(rid, label, makeFetch) {
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const res = await makeFetch();
      if (res.ok) {
        console.log(`[${rid}][dualWrite] ${label} succeeded HTTP ${res.status} (attempt ${attempt})`);
        return { ok: true, label, status: res.status };
      }
      console.error(`[${rid}][dualWrite] ${label} failed HTTP ${res.status} (attempt ${attempt})`);
      if (attempt < 2) {
        await jitteredDelay();
        continue;
      }
      return { ok: false, label, status: res.status };
    } catch (err) {
      console.error(`[${rid}][dualWrite] ${label} rejected: ${err} (attempt ${attempt})`);
      if (attempt < 2) {
        await jitteredDelay();
        continue;
      }
      return { ok: false, label, error: String(err) };
    }
  }
}

/**
 * Tolerant Dual-Write: PUT to primary and failover in parallel.
 * Awaits both writes to ensure replication completes before the function exits.
 * Returns 200 if at least one succeeds.
 */
async function dualWrite(rid, shard, bucketIndex, objectPath, body) {
  const { primary, failover } = SHARDS[shard];
  const credentials = getCredentials();

  console.log(`[${rid}][dualWrite] shard=${shard} bucket=${BUCKET_PREFIX}-${bucketIndex}`);
  console.log(`[${rid}][dualWrite] primary=${primary.host} failover=${failover.host}`);

  const [primaryResult, failoverResult] = await Promise.all([
    taggedFetch(
      rid,
      "primary",
      () => signedPut(primary.host, primary.region, bucketIndex, objectPath, body, credentials)
    ),
    taggedFetch(
      rid,
      "failover",
      () => signedPut(failover.host, failover.region, bucketIndex, objectPath, body, credentials)
    ),
  ]);

  if (!primaryResult.ok && !failoverResult.ok) {
    return {
      ok: false,
      message: `Both writes failed — primary: ${primaryResult.error || `HTTP ${primaryResult.status}`}, failover: ${failoverResult.error || `HTTP ${failoverResult.status}`}`,
    };
  }

  return {
    ok: true,
    primaryOk: primaryResult.ok,
    failoverOk: failoverResult.ok,
  };
}

/**
 * Main request handler.
 */
async function handle(request) {
  const rid = generateRequestId();
  console.log(`[${rid}][handle] ${request.method} ${request.url}`);

  // Verify Bearer token (injected by the CDN layer)
  const authHeader = request.headers.get("authorization") || "";
  const expectedToken = getVariable("auth_token");
  if (!authHeader.startsWith("Bearer ") || authHeader.slice(7) !== expectedToken) {
    console.error(`[${rid}][handle] unauthorized request`);
    return new Response("Unauthorized", { status: 401 });
  }

  // Determine allowed methods based on ingest mode
  const mode = getIngestMode();
  const allowedMethods = mode === "fetch"
    ? ["GET"]
    : ["POST"];

  if (!allowedMethods.includes(request.method)) {
    console.error(`[${rid}][handle] rejected method: ${request.method} (mode=${mode})`);
    return new Response("Method Not Allowed", { status: 405 });
  }

  // Guard against oversized payloads (relay mode only)
  if (mode !== "fetch") {
    const contentLength = parseInt(request.headers.get("content-length") || "0", 10);
    if (contentLength > MAX_BODY_SIZE) {
      console.error(`[${rid}][handle] payload too large: ${contentLength} bytes`);
      return new Response("Payload Too Large", { status: 413 });
    }
  }

  // Object key comes from the URL path
  const url = new URL(request.url);
  const cleanPath = url.pathname.split("?")[0];

  // Validate the object key to prevent path traversal
  if (!cleanPath.startsWith(ALLOWED_PATH_PREFIX) || cleanPath.includes("..")) {
    console.error(`[${rid}][handle] rejected path: ${cleanPath}`);
    return new Response("Forbidden path", { status: 403 });
  }

  // 1. Determine shard from path
  const shardKey = resolveShard(cleanPath);

  // 2. Extract filename and compute bucket index via Jenkins hash
  const filename = extractFilename(cleanPath);
  const hash = jenkinsHash(filename);
  const bucketIndex = (hash % POOL_SIZE) + 1;

  console.log(`[${rid}][handle] mode=${mode} path=${cleanPath} file=${filename} hash=${hash} shard=${shardKey} bucket=${bucketIndex}`);

  // 3. Resolve the object body based on ingest mode
  let objectBody;
  if (mode === "fetch") {
    // Fetch mode: pull content from origin using the URL path
    try {
      objectBody = await fetchFromOrigin(rid, cleanPath);
    } catch (err) {
      console.error(`[${rid}][handle] origin fetch failed: ${err}`);
      return new Response(JSON.stringify({ error: String(err) }), {
        status: 502,
        headers: { "Content-Type": "application/json" },
      });
    }
  } else {
    // Relay mode: POST body IS the election results data
    try {
      objectBody = await request.text();
    } catch {
      console.error(`[${rid}][handle] failed to read request body`);
      return new Response("Bad Request", { status: 400 });
    }
    if (!objectBody) {
      console.error(`[${rid}][handle] empty body`);
      return new Response("Empty body", { status: 400 });
    }
  }

  // 4. Tolerant dual-write
  const result = await dualWrite(rid, shardKey, bucketIndex, cleanPath, objectBody);

  if (!result.ok) {
    console.error(`[${rid}][handle] dual-write FAILED: ${result.message}`);
    return new Response(JSON.stringify({ error: result.message }), {
      status: 502,
      headers: { "Content-Type": "application/json" },
    });
  }

  console.log(`[${rid}][handle] dual-write OK primary=${result.primaryOk} failover=${result.failoverOk}`);

  return new Response(
    JSON.stringify({
      status: "ok",
      request_id: rid,
      shard: shardKey,
      bucket: `${BUCKET_PREFIX}-${bucketIndex}`,
      primary_written: result.primaryOk,
      failover_written: result.failoverOk,
    }),
    {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }
  );
}

addEventListener("fetch", (event) => {
  event.respondWith(handle(event.request));
});
