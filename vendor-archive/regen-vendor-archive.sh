#!/usr/bin/env bash
# Regenerates the vendored dependency archive after go.mod or go.sum changed.
# See README-VENDOR.md for details.
set -euo pipefail

cd "$(dirname "$0")/.."

go mod vendor

COPYFILE_DISABLE=1 tar czf vendor-archive/tsbs-vendor.tar.gz vendor

git add vendor-archive/tsbs-vendor.tar.gz
echo "Staged vendor-archive/tsbs-vendor.tar.gz. Commit it together with the go.mod/go.sum change."
