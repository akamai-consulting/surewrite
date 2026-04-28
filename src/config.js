/**
 * SureWrite Ingest Configuration
 * Shard topology, bucket pool, and routing rules for the 2026 Brazilian Election.
 */

export const POOL_SIZE = 10;
export const BUCKET_PREFIX = "surewrite";

// Ring Topology: Primary + Failover per shard
export const SHARDS = {
  A: {
    primary:  { host: "us-iad-10.linodeobjects.com", region: "us-iad-1" }, // Washington, DC
    failover: { host: "us-ord-10.linodeobjects.com", region: "us-ord-1" }, // Chicago, IL
  },
  B: {
    primary:  { host: "us-ord-10.linodeobjects.com", region: "us-ord-1" }, // Chicago, IL
    failover: { host: "us-lax-4.linodeobjects.com",  region: "us-lax-1" }, // Los Angeles, CA
  },
  C: {
    primary:  { host: "us-lax-4.linodeobjects.com",  region: "us-lax-1" }, // Los Angeles, CA
    failover: { host: "us-iad-10.linodeobjects.com", region: "us-iad-1" }, // Washington, DC
  },
};

// Shard routing rules — evaluated in order, first match wins.
// Pattern is tested against the full request path.
export const SHARD_RULES = [
  { pattern: /\/dados\/(ac|am|ap|pa|ro|rr|to)\//i,                       shard: "C" }, // North
  { pattern: /\/dados\/(al|ba|ce|ma|pb|pe|pi|rn|se)\//i,                 shard: "B" }, // Northeast
  { pattern: /\/dados\/(df|es|go|mg|ms|mt|pr|rj|rs|sc|sp)\//i,           shard: "A" }, // South/Southeast/Central-West
];

// Default shard when no rule matches
export const DEFAULT_SHARD = "A";
