package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dotmesh-io/citools"
)

func TestS3ApiMultiNode(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewCluster(2)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	host1 := f[0].GetNode(0)
	node1 := host1.Container

	host2 := f[0].GetNode(1)
	node2 := host2.Container

	err = citools.RegisterUser(host1, "bob", "bob@bob.com", "password")
	if err != nil {
		t.Errorf("failed to register user: %s", err)
	}

	t.Run("Put", func(t *testing.T) {
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)

		// creating file on node2
		cmd := fmt.Sprintf("curl -T newfile.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/newfile", host1.Password, dotName)
		citools.RunOnNode(t, node2, "echo helloworld > newfile.txt")
		citools.RunOnNode(t, node2, cmd)
		resp := citools.OutputFromRunOnNode(t, node2, citools.DockerRun(dotName)+" ls /foo/")
		if !strings.Contains(resp, "newfile") {
			t.Error("failed to create file")
		}
		resp = citools.OutputFromRunOnNode(t, node2, citools.DockerRun(dotName)+" cat /foo/newfile")
		if !strings.Contains(resp, "helloworld") {
			t.Error("failed to upload file")
		}
		resp = citools.OutputFromRunOnNode(t, node2, "dm log")
		if !strings.Contains(resp, "author: admin") {
			t.Error("Did not set author correctly")
		}
	})

	t.Run("Put 10MB", func(t *testing.T) {
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)
		cmd := fmt.Sprintf("curl -T newfile.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/largefile", host1.Password, dotName)
		citools.RunOnNode(t, node1, `yes "Some text" | head -n 1000000 > largefile.txt`)
		citools.RunOnNode(t, node1, cmd)
		resp := citools.OutputFromRunOnNode(t, node1, citools.DockerRun(dotName)+" ls /foo/")
		if !strings.Contains(resp, "largefile") {
			t.Error("failed to create large file")
		}
		resp = citools.OutputFromRunOnNode(t, node1, "dm log")
		if !strings.Contains(resp, "author: admin") {
			t.Error("Did not set author correctly")
		}
	})
}
