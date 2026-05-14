#!/usr/bin/env bash
# Delete test objects from every SureWrite bucket across every region.
# Scoped to PREFIX so it can NEVER touch non-test data even if mis-invoked.
#
# Uses your existing s3cmd config (~/.s3cfg) for credentials.
# Per-region --host / --host-bucket overrides retarget the endpoint per call.
#
# Usage:
#   bash k6/cleanup-buckets.sh
set -euo pipefail

# Hard-coded test prefix — must match the path the k6 script POSTs to
# (see generateFilename / objectPath in ingest-stress.js). Scoping here is the
# only thing standing between this script and the rest of the bucket.
PREFIX="oficial/ele2026/620/dados/"

BUCKETS=(surewrite-1 surewrite-2 surewrite-3)
REGIONS=(us-iad-18 us-ord-10 us-lax-4)

if ! command -v s3cmd >/dev/null 2>&1; then
  echo "s3cmd not found — install via 'brew install s3cmd' or 'pip install s3cmd'" >&2
  exit 1
fi

echo "Will recursively delete s3://<bucket>/${PREFIX}* from:"
for region in "${REGIONS[@]}"; do
  for bucket in "${BUCKETS[@]}"; do
    echo "  s3://${bucket}  (${region}.linodeobjects.com)"
  done
done
echo

failed=()
for region in "${REGIONS[@]}"; do
  for bucket in "${BUCKETS[@]}"; do
    echo "==> ${bucket} @ ${region}"
    if ! s3cmd --quiet \
        --host="${region}.linodeobjects.com" \
        --host-bucket="%(bucket)s.${region}.linodeobjects.com" \
        del --recursive --force "s3://${bucket}/${PREFIX}"; then
      echo "    (no objects to delete, or s3cmd error — continuing)"
      failed+=("${bucket}@${region}")
    fi
  done
done

echo
if [[ ${#failed[@]} -eq 0 ]]; then
  echo "Cleanup complete — all 9 bucket/region combos processed."
else
  echo "Cleanup finished with ${#failed[@]} non-fatal issue(s):"
  printf '  - %s\n' "${failed[@]}"
fi
