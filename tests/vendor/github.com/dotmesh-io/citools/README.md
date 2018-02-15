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


