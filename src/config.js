/**
 * SureWrite Ingest Configuration
 * Shard topology, bucket pool, and routing rules for the 2026 Brazilian Election.
 */

export const POOL_SIZE = 3;
export const BUCKET_PREFIX = "surewrite";

// Ring Topology: Primary + Failover per shard
export const SHARDS = {
  A: {
    primary:  { host: "us-iad-18.linodeobjects.com", region: "us-iad-18" }, // Washington, DC
    failover: { host: "us-ord-10.linodeobjects.com", region: "us-ord-1" }, // Chicago, IL
  },
  B: {
    primary:  { host: "us-ord-10.linodeobjects.com", region: "us-ord-1" }, // Chicago, IL
    failover: { host: "us-lax-4.linodeobjects.com",  region: "us-lax-4" }, // Los Angeles, CA
  },
  C: {
    primary:  { host: "us-lax-4.linodeobjects.com",  region: "us-lax-4" }, // Los Angeles, CA
    failover: { host: "us-iad-18.linodeobjects.com", region: "us-iad-18" }, // Washington, DC
  },
};

// Shard routing rules — balanced by electorate (~50M voters per shard).
// Evaluated in order, first match wins. Pattern tested against the full request path.
export const SHARD_RULES = [
  { pattern: /\/dados\/(sp|pe|am|pi|df|se|rr)\//i,                       shard: "A" }, // ~50.3M voters
  { pattern: /\/dados\/(mg|ba|pr|ce|pb|mt|ms|ro|ac)\//i,                 shard: "B" }, // ~50.2M voters
  { pattern: /\/dados\/(rj|rs|pa|sc|ma|go|es|rn|al|to|ap)\//i,          shard: "C" }, // ~50.8M voters
];

// Default shard when no rule matches
export const DEFAULT_SHARD = "A";
