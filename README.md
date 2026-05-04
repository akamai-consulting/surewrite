# SureWrite

High-velocity election results ingest function for the 2026 Brazilian Election. Runs on Akamai WASM Functions (Spin) and replicates election result JSON files across geographically distributed Linode E3 Object Storage buckets.

## Architecture

```
TSE Backend → CDN (Token Auth 2.0) → SureWrite WASM Function → Linode E3 Object Storage
                                          │
                                          ├── Primary region   (signed PUT)
                                          └── Failover region  (signed PUT)
```

### Sharding

Writes are micro-sharded across a configurable pool of buckets per region using a **Jenkins One-at-a-Time hash** of the filename. This distributes load to bypass per-bucket RPS limits while keeping routing deterministic. Bucket indices are 1-based.

```
bucket_index = (JenkinsHash(filename) % POOL_SIZE) + 1
```

### Replication (Ring Topology)

Each request is written to two regions in parallel (tolerant dual-write):

| Shard | States | ~Voters | Primary | Failover |
|-------|--------|---------|---------|----------|
| A | SP, PE, AM, PI, DF, SE, RR | 50.3M | Washington, DC | Chicago, IL |
| B | MG, BA, PR, CE, PB, MT, MS, RO, AC | 50.2M | Chicago, IL | Los Angeles, CA |
| C | RJ, RS, PA, SC, MA, GO, ES, RN, AL, TO, AP | 50.8M | Los Angeles, CA | Washington, DC |

A `200 OK` is returned if **at least one** write succeeds. Both writes are always awaited to ensure replication completes before the function exits.

## Ingest Modes

Controlled by the `ingest_mode` variable. The mode also determines which HTTP methods are accepted, reducing the attack surface.

| Mode | HTTP Method | Behavior |
|------|-------------|----------|
| `relay` (default) | `POST` | The request body is the raw election results data. Written directly to S3. |
| `fetch` | `GET` | No body required. The function pulls the object from `origin_hostname` using the URL path. |

In both modes, the **URL path** is the S3 object key (e.g. `/oficial/ele2026/620/dados/sp/sp71072-c0011-e000620-u.json`).

## API Documentation

Live interactive API documentation (Redoc) is served directly by the deployed application:

| URL | Description |
|-----|-------------|
| `https://<hostname>/docs/` | Rendered Redoc API reference |
| `https://<hostname>/docs/openapi.yaml` | Raw OpenAPI 3.1.0 spec |

Access requires a valid Token Auth 2.0 token, the same as all other endpoints.

## Request Format

### Relay mode (POST)

```
POST /oficial/ele2026/620/dados/sp/sp71072-c0011-e000620-u.json HTTP/1.1
Authorization: Bearer <auth_token>
Content-Type: application/json

{"votos": [{"candidato": "001", "total": 15234}]}
```

### Fetch mode (GET)

```
GET /oficial/ele2026/620/dados/sp/sp71072-c0011-e000620-u.json HTTP/1.1
Authorization: Bearer <auth_token>
```

### Response

```json
{
  "status": "ok",
  "request_id": "a1b2c3d4e5f6",
  "shard": "B",
  "bucket": "surewrite-7",
  "primary_written": true,
  "failover_written": true
}
```

## Security

- **Bearer token authentication** — Every request must include an `Authorization: Bearer <token>` header. The CDN layer validates the POSTer via Token Auth 2.0 and injects the token before forwarding.
- **Method restriction** — Relay mode only accepts `POST`; fetch mode only accepts `GET`. All other methods return `405`.
- **Path validation** — Object keys must start with `/oficial/` and cannot contain `..`.
- **Body size limit** — Relay mode requests over 5 MB are rejected with `413`.
- **S3 Signature V4** — All PUTs to Linode Object Storage are signed using `@smithy/signature-v4`.

## Configuration

All configuration is via Spin variables, set at deploy time or through environment variables:

| Variable | Required | Secret | Description |
|----------|----------|--------|-------------|
| `s3_access_key_id` | yes | no | Linode Object Storage access key |
| `s3_secret_access_key` | yes | yes | Linode Object Storage secret key |
| `auth_token` | yes | yes | Bearer token for request authentication |
| `ingest_mode` | no | no | `relay` (default) or `fetch` |
| `origin_hostname` | no | no | Origin hostname for `fetch` mode (e.g. `resultados.tse.jus.br`) |

### Environment Variables (local dev)

```sh
export SPIN_VARIABLE_S3_ACCESS_KEY_ID="..."
export SPIN_VARIABLE_S3_SECRET_ACCESS_KEY="..."
export SPIN_VARIABLE_AUTH_TOKEN="..."
export SPIN_VARIABLE_INGEST_MODE="relay"
export SPIN_VARIABLE_ORIGIN_HOSTNAME=""
```

## Development

### Prerequisites

- Node.js 24+
- [Spin CLI](https://developer.fermyon.com/spin/install) 3.5+

### Build

```sh
npm install
spin build
```

### Run locally

```sh
spin up
```

### Deploy

```sh
spin aka login --token <PAT>
spin aka deploy --app-id <APP_ID> --no-confirm \
  --variable s3_access_key_id=<KEY> \
  --variable s3_secret_access_key=<SECRET> \
  --variable auth_token=<TOKEN> \
  --variable ingest_mode=relay \
  --variable origin_hostname=""
```

## Debugging

1. Install the [StarlingMonkey Debugger](https://marketplace.visualstudio.com/items?itemName=BytecodeAlliance.starlingmonkey-debugger) extension.
2. Build with `npm run build:debug`.
3. Uncomment `tcp://127.0.0.1:*` in `allowed_outbound_hosts` in `spin.toml`.
4. Start the debugger in VS Code (restart for each HTTP call).

## Project Structure

```
src/
  index.js      Main request handler
  config.js     Shard topology, routing rules, constants
  jenkins.js    Jenkins One-at-a-Time hash
  signing.js    S3 Signature V4 wrapper
docs/
  index.html    Redoc API documentation viewer (served at /docs/)
openapi.yaml    OpenAPI 3.1.0 specification (served at /docs/openapi.yaml)
spin.toml       Spin application manifest
build.mjs       esbuild configuration
.github/
  ci.yaml       CI/CD pipeline
```

## CI/CD

Pushes to `main` trigger the GitHub Actions workflow in `.github/ci.yaml`, which builds the WASM component and deploys to Akamai WASM Functions. Secrets (`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `AUTH_TOKEN`) and variables (`INGEST_MODE`, `ORIGIN_HOSTNAME`) are configured in the repository settings.