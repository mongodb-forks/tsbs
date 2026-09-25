#!/usr/bin/env bash
# Regenerates the vendored dependency archive after go.mod or go.sum changed.
# See README-VENDOR.md for details.
set -euo pipefail

cd "$(dirname "$0")"

go mod vendor

old_archives=$(ls tsbs-vendor-*.tar.gz 2>/dev/null || true)
new_archive="tsbs-vendor-$(sha256sum go.mod | cut -c1-12).tar.gz"

COPYFILE_DISABLE=1 tar czf "$new_archive" vendor

for old in $old_archives; do
  if [ "$old" != "$new_archive" ]; then
    git rm "$old"
  fi
done

git add "$new_archive"
echo "Staged $new_archive. Commit it together with the go.mod/go.sum change."
