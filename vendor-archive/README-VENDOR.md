# Vendored dependency archive

vendor-archive/tsbs-vendor.tar.gz holds the vendored dependency tree for this
repository. Sys-perf workload setup extracts the archive before building, so
module dependencies are not fetched from the Go module proxy at task time.

Rebuild the archive whenever go.mod or go.sum changes, which covers dependency
additions, updates, and removals:

```
./vendor-archive/regen-vendor-archive.sh
git commit
```

The script runs `go mod vendor` and repacks the archive. Done by hand this is:

```
go mod vendor
COPYFILE_DISABLE=1 tar czf vendor-archive/tsbs-vendor.tar.gz vendor
git add vendor-archive/tsbs-vendor.tar.gz
git commit
```

A stale archive is caught by the vendor-archive CI check, which compares it
against a fresh `go mod vendor` run. Setup fails immediately when the archive
is missing, so that surfaces in the task log rather than depending on the Go
module proxy.

The archive must be generated with the same Go version the sys-perf tasks
use, currently the 1.24.x toolchain shipped in the TSBS folder of the
dsi-donot-remove S3 bucket.
