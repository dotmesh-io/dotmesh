# citools

Some shared CI utilities for running [dotmesh](https://dotmesh.com) acceptance tests.

## usage

The `testtools.go` library should be imported into a test like follows:

```bash
dep init
dep ensure -add github.com/dotmesh-io/citools github.com/dotmesh-io/citools
```

Then within the testing file:

```go
import (
  "github.com/dotmesh-io/citools"
)


func TestDefaultDot(t *testing.T) {
  // Test default dot select on a totally fresh cluster
  citools.teardownFinishedTestRuns()

  f := citools.Federation{citools.NewCluster(1)}

  citools.startTiming()
}
```

## Updating the dind script

Download the most latest into `dind-cluster-original.sh`:

```bash
./download-latest-script.sh
```

Apply the patch in `dind-cluster.sh.patch` to turn it into `dind-cluster-patched.sh`:

```bash
./apply-patch.sh
```

If it doesn't apply, get in there and hack stuff by hand. We keep
`dind-cluster-original.sh` in git so you can diff the previous version
to the new version you just downloaded to try and work out what
changes have happened.

Embed it into `dindscript.go`:

```bash
./embed-script.sh
```

Test it. If it doesn't work, hand-hack `dind-cluster-patched.sh` and
rerun `./embed-script.sh` until it works.

Then before you commit, save your changes back to
`dind-cluster.sh.patch`:

```bash
./update-patch.sh
```
