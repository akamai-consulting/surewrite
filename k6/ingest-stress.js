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

// ─── Configuration ──────────────────────────────────────────────────
const BASE_URL   = __ENV.INGEST_URL || "https://brazil-poc-ingest.akamaized.net";
const TOKEN_QS   = __ENV.INGEST_TOKEN || "";  // __token__ query string for CDN routing

if (!TOKEN_QS) {
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

// ─── Test Stages ────────────────────────────────────────────────────
export const options = {
  scenarios: {
    ingest_stress: {
      executor: "ramping-arrival-rate",
      startRate: 100,
      timeUnit: "1s",
      preAllocatedVUs: 200,
      maxVUs: 1000,
      stages: [
        { duration: "1m",  target: 100 },  // hold 100 RPS — warm up
        { duration: "1m",  target: 250 },  // ramp to 250 RPS
        { duration: "1m",  target: 250 },  // hold 500 RPS — ~167 RPS/bucket (limit: 2 000 RPS/bucket × 3 buckets = 6 000 RPS; failures here indicate upstream bottleneck)
        { duration: "1m",  target: 100 },  // cool down
        { duration: "1m",  target: 0 },    // drain
      ],
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

  const queryString = TOKEN_QS ? `?__token__=${TOKEN_QS}` : "";
  const url = `${BASE_URL}${objectPath}${queryString}`;

  const params = {
    headers: {
      "Content-Type": "application/json",
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

  // Log first 3 failures per VU with timestamp to correlate errors against ramp stage
  if (!success && __ITER < 3) {
    console.log(`DEBUG VU=${__VU} ITER=${__ITER} t=${Math.round(new Date().getTime() / 1000)}s status=${res.status} body=${String(res.body).slice(0, 300)}`);
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

  console.log("\n═══ Shard Distribution ═══");
  console.log(`  Shard A: ${shardA} (${((shardA / total) * 100).toFixed(1)}%)`);
  console.log(`  Shard B: ${shardB} (${((shardB / total) * 100).toFixed(1)}%)`);
  console.log(`  Shard C: ${shardC} (${((shardC / total) * 100).toFixed(1)}%)`);
  console.log(`  Total:   ${total}\n`);

  console.log("═══ HTTP Error Breakdown ═══");
  console.log(`  4xx (rate-limit/auth): ${http4xx}%`);
  console.log(`  5xx (overload/fault):  ${http5xx}%\n`);

  return {
    stdout: JSON.stringify(data, null, 2),
  };
}
