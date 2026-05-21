import http from "k6/http";
import { check } from "k6";
import { Rate, Counter, Trend } from "k6/metrics";

// ─── Custom Metrics ─────────────────────────────────────────────────
const errorRate    = new Rate("ingest_errors");
const shardACount  = new Counter("shard_A_writes");
const shardBCount  = new Counter("shard_B_writes");
const shardCCount  = new Counter("shard_C_writes");
const dualWriteMs  = new Trend("dual_write_duration_ms");
const http4xxRate  = new Rate("http_4xx_errors");
const http5xxRate  = new Rate("http_5xx_errors");

// Failure-body fingerprinting — partition every failure into a category so we can
// tell function-origin errors apart from CDN-origin errors in the summary.
const failEmpty        = new Counter("fail_empty_body");
const failDualWrite    = new Counter("fail_dual_write");      // function returned its dual-write 502
const failOriginFetch  = new Counter("fail_origin_fetch");    // function origin-fetch 502
const failUnauthorized = new Counter("fail_unauthorized");    // function 401
const failRejected     = new Counter("fail_rejected");        // function 4xx (path/method/payload)
const failCdnHtml      = new Counter("fail_cdn_html");        // looks like an Akamai/CDN HTML error page
const failJsonOther    = new Counter("fail_json_other");
const failOther        = new Counter("fail_other");

const FAILURE_SAMPLE_RATE = 0.01; // log full body for ~1% of failures across the whole run

// ─── Configuration ──────────────────────────────────────────────────
const BASE_URL   = __ENV.INGEST_URL || "https://brazil-poc-ingest.akamaized.net";
const TOKEN      = __ENV.INGEST_TOKEN || "";  // sent as X-SureWrite-TA header

if (!TOKEN) {
  throw new Error("INGEST_TOKEN environment variable is required");
}

// ─── Shard-to-States Mapping ────────────────────────────────────────
const SHARD_STATES = {
  A: ["sp", "pe", "am", "pi", "df", "se", "rr"],
  B: ["mg", "ba", "pr", "ce", "pb", "mt", "ms", "ro", "ac"],
  C: ["rj", "rs", "pa", "sc", "ma", "go", "es", "rn", "al", "to", "ap"],
};
const ALL_STATES = [
  ...SHARD_STATES.A,
  ...SHARD_STATES.B,
  ...SHARD_STATES.C,
];

// ─── Load Profiles ──────────────────────────────────────────────────
// Selectable via PROFILE env var (default | low | high). Each profile has its own
// stage ramp + VU sizing so we can sweep load levels without editing the file.
const PROFILE = (__ENV.PROFILE || "default").toLowerCase();
// HOLD overrides the peak-hold duration so we can let HPA-style horizontal
// scaling reach steady state. Default 1m; bump to 5m+ when measuring scaled latency.
const HOLD = __ENV.HOLD || "1m";

// VU sizing assumes ~1s service time under load (observed p95 ~1.5s at 1k RPS),
// not the unloaded 360ms baseline — VUs need to cover the whole request lifetime.
const PROFILES = {
  low: {
    desc: "500 RPS peak (1,000 outbound RPS demand, ~111 RPS/bucket)",
    preAllocatedVUs: 800,
    maxVUs: 1500,
    startRate: 125,
    stages: [
      { duration: "1m", target: 125 },
      { duration: "1m", target: 500 },
      { duration: HOLD, target: 500 },
      { duration: "1m", target: 125 },
      { duration: "1m", target: 0 },
    ],
  },
  mid: {
    desc: "1,000 RPS peak (2,000 outbound RPS demand, ~222 RPS/bucket)",
    preAllocatedVUs: 1500,
    maxVUs: 3000,
    startRate: 250,
    stages: [
      { duration: "1m", target: 250 },
      { duration: "1m", target: 1000 },
      { duration: HOLD, target: 1000 },
      { duration: "1m", target: 250 },
      { duration: "1m", target: 0 },
    ],
  },
  default: {
    desc: "2,000 RPS peak (4,000 outbound RPS demand, ~444 RPS/bucket)",
    preAllocatedVUs: 3000,
    maxVUs: 5000,
    startRate: 500,
    stages: [
      { duration: "1m", target: 500 },
      { duration: "1m", target: 2000 },
      { duration: HOLD, target: 2000 },
      { duration: "1m", target: 500 },
      { duration: "1m", target: 0 },
    ],
  },
  high: {
    desc: "3,000 RPS peak (6,000 outbound RPS demand, ~667 RPS/bucket)",
    preAllocatedVUs: 4000,
    maxVUs: 8000,
    startRate: 750,
    stages: [
      { duration: "1m", target: 750 },
      { duration: "1m", target: 3000 },
      { duration: HOLD, target: 3000 },
      { duration: "1m", target: 750 },
      { duration: "1m", target: 0 },
    ],
  },
};
if (!PROFILES[PROFILE]) {
  throw new Error(`Unknown PROFILE='${PROFILE}'. Use one of: ${Object.keys(PROFILES).join(", ")}`);
}
const ACTIVE_PROFILE = PROFILES[PROFILE];
console.log(`[profile=${PROFILE} hold=${HOLD}] ${ACTIVE_PROFILE.desc}`);

// ─── Test Stages ────────────────────────────────────────────────────
export const options = {
  // Round-robin across IPs returned by DNS. No-op when DNS returns a single IP;
  // when multiple are returned (Akamai staging returns 2), spreads new connections
  // across them. ttl=0 forces fresh resolution per connection so the roundRobin
  // selector actually distributes instead of latching on the first result.
  dns: {
    ttl: "0",
    select: "roundRobin",
    policy: "preferIPv4",
  },
  scenarios: {
    ingest_stress: {
      executor: "ramping-arrival-rate",
      startRate: ACTIVE_PROFILE.startRate,
      timeUnit: "1s",
      preAllocatedVUs: ACTIVE_PROFILE.preAllocatedVUs,
      maxVUs: ACTIVE_PROFILE.maxVUs,
      stages: ACTIVE_PROFILE.stages,
    },
  },
  thresholds: {
    http_req_duration:   ["p(95)<2000", "p(99)<4000"],  // P95 < 2s, P99 < 4s (dual-write baseline ~750ms)
    http_req_failed:     ["rate<0.05"],                   // < 5% HTTP failures
    ingest_errors:       ["rate<0.05"],                   // < 5% application errors
  },
};

// ─── Helpers ────────────────────────────────────────────────────────
function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function zeroPad(n, width) {
  return String(n).padStart(width, "0");
}

const SHARD_KEYS = Object.keys(SHARD_STATES);

function randomStateBalanced() {
  const shard = SHARD_KEYS[Math.floor(Math.random() * SHARD_KEYS.length)];
  const states = SHARD_STATES[shard];
  return states[Math.floor(Math.random() * states.length)];
}

function shardForState(uf) {
  for (const [shard, states] of Object.entries(SHARD_STATES)) {
    if (states.includes(uf)) return shard;
  }
  return "A"; // default
}

function classifyFailure(res) {
  const body = res.body == null ? "" : String(res.body);
  if (body.length === 0) { failEmpty.add(1); return "empty"; }
  if (body.includes("Both writes failed"))   { failDualWrite.add(1);    return "dual_write"; }
  if (body.includes("origin fetch failed") ||
      body.includes("origin responded HTTP")) { failOriginFetch.add(1); return "origin_fetch"; }
  if (body === "Unauthorized")               { failUnauthorized.add(1); return "unauthorized"; }
  if (body.startsWith("Forbidden") ||
      body.startsWith("Method Not Allowed") ||
      body.startsWith("Payload Too Large") ||
      body.startsWith("Empty body") ||
      body.startsWith("Bad Request"))        { failRejected.add(1);     return "rejected"; }
  if (body.trimStart().startsWith("<"))      { failCdnHtml.add(1);      return "cdn_html"; }
  try { JSON.parse(body); failJsonOther.add(1); return "json_other"; } catch {}
  failOther.add(1);
  return "other";
}

/**
 * Generate a realistic election results filename.
 * Pattern: {uf}{election_code}-c{section}-e000620-u.json
 */
function generateFilename(uf) {
  const electionCode = randomInt(10000, 99999);
  const section = zeroPad(randomInt(1, 9999), 4);
  return `${uf}${electionCode}-c${section}-e000620-u.json`;
}

/**
 * Build a minimal but realistic election result JSON payload (~500 bytes).
 */
function generatePayload(uf, filename) {
  return JSON.stringify({
    uf: uf.toUpperCase(),
    filename,
    seq: randomInt(1, 999),
    timestamp: new Date().toISOString(),
    turno: 1,
    candidates: [
      { id: randomInt(10, 99), votes: randomInt(0, 500000) },
      { id: randomInt(10, 99), votes: randomInt(0, 500000) },
      { id: randomInt(10, 99), votes: randomInt(0, 300000) },
    ],
    sections_counted: randomInt(1, 5000),
    sections_total: 5000,
    nullified: randomInt(0, 10000),
    blank: randomInt(0, 10000),
  });
}

// ─── Main Test Function ─────────────────────────────────────────────
export default function () {
  const uf = randomStateBalanced();
  const filename = generateFilename(uf);
  const objectPath = `/oficial/ele2026/620/dados/${uf}/${filename}`;
  const payload = generatePayload(uf, filename);
  const shard = shardForState(uf);

  const url = `${BASE_URL}${objectPath}`;

  const params = {
    headers: {
      "Content-Type": "application/json",
      "X-SureWrite-TA": TOKEN,
    },
    tags: { name: "POST /oficial/ele2026/620/dados/{uf}/{file}.json", shard },
  };

  const res = http.post(url, payload, params);

  // Track shard distribution
  if (shard === "A") shardACount.add(1);
  else if (shard === "B") shardBCount.add(1);
  else shardCCount.add(1);

  // Validate response
  const success = check(res, {
    "status is 200": (r) => r.status === 200,
    "response is JSON": (r) => {
      try { JSON.parse(r.body); return true; } catch { return false; }
    },
    "status field is ok": (r) => {
      try { return JSON.parse(r.body).status === "ok"; } catch { return false; }
    },
    "has request_id": (r) => {
      try { return /^[0-9a-f]{12}$/.test(JSON.parse(r.body).request_id); } catch { return false; }
    },
    "shard matches expected": (r) => {
      try { return JSON.parse(r.body).shard === shard; } catch { return false; }
    },
    "bucket is 1-3": (r) => {
      try {
        const b = JSON.parse(r.body).bucket;
        return /^surewrite-[123]$/.test(b);
      } catch { return false; }
    },
  });

  // Classify and tally every failure across the whole run, and sample ~1% with full body.
  // Per-VU iter-based sampling missed everything past warm-up because high-load failures
  // happen well after each VU's first few iterations.
  if (!success) {
    const cls = classifyFailure(res);
    if (Math.random() < FAILURE_SAMPLE_RATE) {
      const body = String(res.body || "").slice(0, 500).replace(/\s+/g, " ");
      const ts = Math.round(Date.now() / 1000);
      console.log(`FAIL VU=${__VU} ITER=${__ITER} t=${ts}s status=${res.status} class=${cls} body="${body}"`);
    }
  }

  // Track HTTP error category distribution to distinguish 4xx (rate-limit/auth) from 5xx (overload)
  http4xxRate.add(res.status >= 400 && res.status < 500);
  http5xxRate.add(res.status >= 500);

  errorRate.add(!success);

  if (res.timings.duration) {
    dualWriteMs.add(res.timings.duration);
  }
}

// ─── Summary Handler ────────────────────────────────────────────────
export function handleSummary(data) {
  const shardA = data.metrics.shard_A_writes ? data.metrics.shard_A_writes.values.count : 0;
  const shardB = data.metrics.shard_B_writes ? data.metrics.shard_B_writes.values.count : 0;
  const shardC = data.metrics.shard_C_writes ? data.metrics.shard_C_writes.values.count : 0;
  const total = shardA + shardB + shardC;

  const http4xx = data.metrics.http_4xx_errors ? (data.metrics.http_4xx_errors.values.rate * 100).toFixed(1) : "0.0";
  const http5xx = data.metrics.http_5xx_errors ? (data.metrics.http_5xx_errors.values.rate * 100).toFixed(1) : "0.0";

  console.log(`\n═══ Profile ═══`);
  console.log(`  ${PROFILE}: ${ACTIVE_PROFILE.desc}\n`);

  console.log("═══ Shard Distribution ═══");
  console.log(`  Shard A: ${shardA} (${((shardA / total) * 100).toFixed(1)}%)`);
  console.log(`  Shard B: ${shardB} (${((shardB / total) * 100).toFixed(1)}%)`);
  console.log(`  Shard C: ${shardC} (${((shardC / total) * 100).toFixed(1)}%)`);
  console.log(`  Total:   ${total}\n`);

  console.log("═══ HTTP Error Breakdown ═══");
  console.log(`  4xx (rate-limit/auth): ${http4xx}%`);
  console.log(`  5xx (overload/fault):  ${http5xx}%\n`);

  const failClasses = [
    ["empty body          (no response body)",                "fail_empty_body"],
    ["dual_write          (function 502 — both S3 PUTs failed)", "fail_dual_write"],
    ["origin_fetch        (function 502 — origin pull failed)",  "fail_origin_fetch"],
    ["unauthorized        (function 401)",                    "fail_unauthorized"],
    ["rejected            (function 4xx — path/method/size)", "fail_rejected"],
    ["cdn_html            (CDN/edge HTML error page)",        "fail_cdn_html"],
    ["json_other          (other JSON body)",                 "fail_json_other"],
    ["other               (non-JSON, non-HTML, unknown)",     "fail_other"],
  ];
  const totalFails = failClasses.reduce((sum, [, k]) =>
    sum + (data.metrics[k] ? data.metrics[k].values.count : 0), 0);
  console.log(`═══ Failure Body Classification (${totalFails} total) ═══`);
  for (const [label, key] of failClasses) {
    const c = data.metrics[key] ? data.metrics[key].values.count : 0;
    if (c === 0) continue;
    const pct = totalFails > 0 ? ((c / totalFails) * 100).toFixed(1) : "0.0";
    console.log(`  ${label}: ${c} (${pct}%)`);
  }
  console.log("");

  return {
    stdout: JSON.stringify(data, null, 2),
  };
}

// k6's JS runtime can't shell out, so cleanup lives in a sibling script.
// Print the invocation reminder right after the run so the operator sees it.
export function teardown() {
  console.log("");
  console.log("To delete test data from all 9 SureWrite buckets, run:");
  console.log("  bash k6/cleanup-buckets.sh");
}
