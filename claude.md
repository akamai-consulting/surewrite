# Akamai Functions Ingest: High-Velocity Election Results Router

This document serves as the technical specification and context for the Akamai WASM Function responsible for ingesting, sharding, and replicating election results JSON files for the 2026 Brazilian Election.

## 1. Overview
The ingest function recieves POST/PUT requests from the TSE Backend. Its primary role is to ensure high availability and horizontal scalability of the storage layer by sharding writes across multiple Linode E3 Object Storage buckets and replicating data across geographic regions.

## 2. Technical Architecture
- **Runtime**: Akamai Functions (WASM/JavaScript).
- **Storage Tier**: Linode E3 Object Storage (NVMe-backed).
- **Regions**: 
    - US East (Washington, DC)
    - US Central (Chicago, IL)
    - US West (Los Angeles, CA)
- **Consistency Model**: PA/EL (Partition Tolerance/Availability, Else Latency).

## 3. Sharding Logic (Jenkins Hash)
To bypass the per-bucket RPS limits of Object Storage, the ingest function implements micro-sharding using the **Jenkins Hash** algorithm.

### Algorithm Characteristics:
- **Non-Cryptographic**: Optimized for speed and low CPU overhead within the WASM sandbox.
- **Avalanche Effect**: Ensures uniform distribution even with highly similar filenames (e.g., `results_01.json`, `results_02.json`).
- **Deterministic**: The same filename always maps to the same bucket index.

### Logic Flow:
1. Receive POST from TSE backend with json that includes the full request path.
2. Extract the `filename` from the request path.
3. Identify the shard based on the request path.
4. Calculate `hash = JenkinsHash(filename)`.
5. Map to bucket: `target_bucket_index = hash % POOL_SIZE` (where `POOL_SIZE` is a static constant, typically 10).

## 4. Replication Strategy: Active-Active Dual-Write
The function performs a synchronous **Tolerant Dual-Write** to ensure regional failover without relying on legacy background sync tools (like rclone).

### The Ring Topology:
- **Shard A**: Primary (DC) + Failover (Chicago)
- **Shard B**: Primary (Chicago) + Failover (Los Angeles)
- **Shard C**: Primary (Los Angeles) + Failover (Washington, DC)

### Execution Flow:
1. Identify the shard based on the request path (e.g., `/oficial/ele2024/620/dados/pb/pb20516-c0011-e000620-u.json`). Use a configurable object or file to specify the rules to determine the shard
2. Calculate the specific bucket index within the pool via Jenkins Hash and the filename.
3. Initiate parallel HTTP PUT requests to the **Primary** and **Failover** E3 endpoints using the hostname: us-lax-4.linodeobjects.com, us-iad-10.linodeobjects.com, or us-ord-10.linodeobjects.com and prepending the bucket name which includes the hash index.
4. Return `200 OK` once at least one write succeeds (Tolerant Write).

## 5. Performance & Safety Constraints
- **Timeout Management**: The function must respect the backend timeout (estimated < 500ms). US-West (LA) latency from Brazil is ~160ms-180ms; the WASM environment handles this via asynchronous fetch.
- **Static Pool Size**: `POOL_SIZE` must not change during the election period. Changing this value will re-index all files, causing 404s on the read path.
- **No Query Strings**: The function should ignore or strip query strings to prevent cache-busting during the subsequent read path revalidation.

## 6. Development Guidelines for Claude
When generating code for this function:
- Use the `fetch` API for parallel origin requests.
- Implement the 32-bit Jenkins One-at-a-Time hash logic.
- Ensure efficient memory usage to stay within Akamai WASM Function resource limits.
- When necessary use efficient and highly optimized libraries.