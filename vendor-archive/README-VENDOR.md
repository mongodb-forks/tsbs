# Vendored dependency archive

tsbs-vendor-<hash>.tar.gz holds the vendored dependency tree for this
repository. The <hash> in the file name is the first 12 hex characters of
`sha256sum go.mod`. Sys-perf workload setup extracts the archive matching the
current go.mod before building, so module dependencies are not fetched from
the Go module proxy at task time.

Rebuild the archive whenever go.mod or go.sum changes, which covers dependency
additions, updates, and removals:

```
./vendor-archive/regen-vendor-archive.sh
git commit
```

The script runs `go mod vendor`, packs the archive with the correct name, and
removes the archive for the old hash. Done by hand this is:

```
go mod vendor
COPYFILE_DISABLE=1 tar czf vendor-archive/tsbs-vendor-$(sha256sum go.mod | cut -c1-12).tar.gz vendor
git add vendor-archive/tsbs-vendor-*.tar.gz
git commit
```

Delete the archive for the old hash after committing the new one. Setup fails
immediately when no archive matches the current go.mod, so a missing rebuild
surfaces in the task log rather than depending on the Go module proxy.

The archive must be generated with the same Go version the sys-perf tasks
use, currently the 1.24.x toolchain shipped in the TSBS folder of the
dsi-donot-remove S3 bucket.
