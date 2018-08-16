package main

import (
	"fmt"
	"github.com/dotmesh-io/citools"
	"strings"
	"testing"
)

func TestS3Api(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewCluster(1)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	host := f[0].GetNode(0)
	node1 := host.Container

	t.Run("Put", func(t *testing.T) {
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm dot init "+dotName)
		cmd := fmt.Sprintf("curl -T newfile.txt -u admin:%s %s:32607/s3/admin-%s/newfile", host.ApiKey, host.IP, dotName)
		citools.RunOnNode(t, node1, "echo helloworld > newfile.txt")
		citools.RunOnNode(t, node1, cmd)
		resp := citools.OutputFromRunOnNode(t, node1, citools.DockerRun(dotName)+" ls /foo/")
		if !strings.Contains(resp, "newfile") {
			t.Error("failed to create file")
		}
		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(dotName)+" cat /foo/newfile")
		if !strings.Contains(resp, "helloworld") {
			t.Error("failed to upload file")
		}
	})

	t.Run("PutDotDoesntExist", func(t *testing.T) {
		dotName := citools.UniqName()
		cmd := fmt.Sprintf("curl -T newfile.txt -u admin:%s %s:32607/s3/admin-%s/newfile", host.ApiKey, host.IP, dotName)
		citools.RunOnNode(t, node1, "echo helloworld > newfile.txt")
		resp := citools.OutputFromRunOnNode(t, node1, cmd)
		if !strings.Contains(resp, fmt.Sprintf("Bucket admin-%s does not exist", dotName)) {
			t.Error("Did not respond with error msg")
		}
	})
}
