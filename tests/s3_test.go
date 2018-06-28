package main

import (
	"github.com/dotmesh-io/citools"
	"strings"
	"testing"
)

func TestS3Remote(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewCluster(1)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Error(err)
	}
	node1 := f[0].GetNode(0).Container
	// TODO not sure the s3 mock used here actually cares for authentication so this may not be enough. Also probably doesn't support versioning...
	citools.RunOnNode(t, node1, "docker run --name my_s3 -p 4569:4569 -d lphoward/fake-s3")
	citools.RunOnNode(t, node1, "dm s3 remote add test-s3 FAKEKEY:FAKESECRET@http://127.0.0.1:4569")

	t.Run("remote", func(t *testing.T) {
		resp := citools.OutputFromRunOnNode(t, node1, "dm remote")
		if !strings.Contains(resp, "test-s3") {
			t.Error("Unable to find remote in output")
		}
	})

	t.Run("remote switch", func(t *testing.T) {
		_, err := citools.RunOnNodeErr(node1, "dm remote switch test-s3")
		// TODO check the error is what we expect
		if err == nil {
			t.Error("Command did not error")
		}
	})
	t.Run("Clone", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm clone s3 test")
		resp := citools.OutputFromRunOnNode(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}
	})

	t.Run("Push", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm push test-s3 ")
		resp := citools.OutputFromRunOnNode(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}
	})

	t.Run("Pull", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm push test-s3 ")
		resp := citools.OutputFromRunOnNode(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}
	})
}
