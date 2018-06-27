package main

import (
	"testing"

	"github.com/dotmesh-io/citools"
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
	citools.RunOnNode(t, node1, "export TESTING=1")
	citools.RunOnNode(t, node1, "docker run -d --publish 9000:80 --env S3PROXY_AUTHORIZATION=none andrewgaul/s3proxy")
	citools.RunOnNode(t, node1, "dm s3 remote add s3 KEY:SECRET@127.0.0.1:9000")

	// t.Run("Clone", func(t *testing.T) {
	// 	fsname := citools.UniqName()
	// 	citools.RunOnNode(t, node1, "dm clone s3 test")
	// 	resp := citools.OutputFromRunOnNode(t, node1, "dm list")
	// 	if !strings.Contains(resp, fsname) {
	// 		t.Error("unable to find volume name in ouput")
	// 	}
	// })
}
