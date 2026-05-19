# Ingest Routing Architecture

Notes on the custom domain + Weighted Load Balancing (WLB) ALB setup for the
SureWrite ingest endpoint. Intended as source material for formal documentation.

## Hostnames

| Environment | URL                                          |
|-------------|----------------------------------------------|
| Production  | `brazil-poc-ingest.akamaized.net`            |
| Staging     | `brazil-poc-ingest.akamaized-staging.net`    |

Both hostnames are CNAMEs into Akamai's CDN, which fronts the Function clusters
via a custom ALB configuration cloned from the default `*.fwf.app` setup.

## Load Balancing

The default `*.fwf.app` ALB selects a single Function cluster based on the
requester's source region. For South American egress paths this typically lands
all traffic on `us-sea`, producing single-cluster concentration.

The custom ALB replaces that behavior with WLB across multiple US-based
Function clusters. Confirmed clusters observed in Function logs during testing:

- `us-iad`  (Washington, DC)
- `us-ord`  (Chicago, IL)
- `us-east` (Virginia)
- `us-lax`  (Los Angeles, CA)

### Why multi-cluster

The single-cluster default exposed three problems uncovered during load testing
in May 2026:

1. **Per-cluster capacity ceiling.** Sustained throughput plateaued at ~620 RPS
   per cluster regardless of incoming demand, with latency degrading sharply
   beyond that point (p95 from ~700ms to >5s).
2. **Cross-tenant interference.** During saturation testing against `us-sea`,
   an unrelated tenant service (`ui.fwf.dev`) returned 500 errors that
   recovered as soon as the test load dropped — clear evidence that overload
   was not isolated within a tenant.
3. **Single-cluster failure domain.** Any incident on the routed-to cluster
   would have affected 100% of ingest traffic.

WLB across four clusters addresses all three: aggregate capacity scales with
cluster count, per-cluster load stays well below the saturation point, and a
single-cluster incident affects only ~1/N of traffic.

Note: the per-cluster ~620 RPS ceiling itself is suspected to stem from
JS-on-WASM compute overhead (no JIT for hot SHA256/HMAC loops in the SigV4
signing path). Engineering is investigating; WLB is the architectural mitigation
that lets us ship safely while the per-pod throughput question is resolved.

## Authentication

Both auth layers are preserved on the WLB-fronted endpoint:

1. **mTLS at the edge.** Client certificate required and validated by Akamai
   before the request reaches the Function.
2. **Bearer token in the Function.** The `__token__=...` query string parameter
   (HMAC-signed, 24-hour validity) is converted to an `Authorization: Bearer`
   header at the edge and validated by `handle()` in `src/index.js`.

mTLS was deliberately retained as defense-in-depth for election-critical
traffic. A leaked bearer token alone represents a single credential factor;
mTLS adds a "something you have" factor (private key bound to a trusted client
cert) so a stolen token cannot, by itself, be used to inject fabricated
election results.

## Failover Behavior

**TBD — confirm with Akamai:**

- Is cluster-level failover automatic when one cluster degrades, or does it
  require manual intervention?
- What health-check criteria does the ALB use (HTTP probe path, success codes,
  latency thresholds)?
- What is the time-to-detect and time-to-shift when a cluster goes unhealthy?
- Does WLB support a Brazilian / LATAM cluster if one becomes available, or is
  the pool US-only by configuration?

## Election-Day Operator Notes

- **Monitor cluster distribution** in the Akamai Functions dashboard. Each of
  the 4 US clusters should be carrying roughly 1/4 of total traffic during
  steady state.
- **Watch per-cluster latency** independently. A single cluster's p95 climbing
  past 1s while others stay flat indicates that cluster is approaching its
  ceiling and may need to be drained from the WLB pool.
- **Cross-tenant interference is no longer a blast-radius concern**, but watch
  cluster-wide degradation as a *symptom* — if one cluster shows it, others may
  follow under correlated load.
- **mTLS cert rotation** must be coordinated with Akamai well in advance of
  election night. A failed handshake at the edge means 100% failure for traffic
  routed to any affected cluster.

## Related Files

- [`src/index.js`](../src/index.js) — Function handler and dual-write logic
- [`src/config.js`](../src/config.js) — shard topology and S3 bucket mapping
- [`k6/ingest-stress.js`](../k6/ingest-stress.js) — load-test rig used to
  validate per-cluster ceiling and WLB distribution
