package main

import (
	"fmt"
	"github.com/dotmesh-io/citools"
	"strings"
	"testing"
)

type ForkRequest struct {
	MasterBranchID string
	ForkNamespace  string
	ForkName       string
}

func TestForks(t *testing.T) {
	// single node tests
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewCluster(1)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatalf("failed to start cluster, error: %s", err)
	}
	node1Name := f[0].GetNode(0).Container
	bobKey := "bob is great"

	// Create user bob on the first node
	err = citools.RegisterUser(f[0].GetNode(0), "bob", "bob@bob.com", bobKey)
	if err != nil {
		t.Error(err)
	}

	aliceKey := "alice is also great"
	err = citools.RegisterUser(f[0].GetNode(0), "alice", "alice@alice.com", aliceKey)
	if err != nil {
		t.Error(err)
	}

	t.Run("CreateThenFork", func(t *testing.T) {
		fsname := citools.UniqName()
		fsname2 := citools.UniqName()
		createResp := false
		err = citools.DoRPC(f[0].GetNode(0).IP, "bob", bobKey,
			"DotmeshRPC.Create",
			VolumeName{Namespace: "bob", Name: fsname},
			&createResp)
		if err != nil {
			t.Error(err)
		}
		var lookupResp string
		err = citools.DoRPC(f[0].GetNode(0).IP, "bob", bobKey,
			"DotmeshRPC.Lookup",
			VolumeName{Namespace: "bob", Name: fsname},
			&lookupResp)
		if err != nil {
			t.Error(err)
		}
		forkResp := false
		err = citools.DoRPC(f[0].GetNode(0).IP, "admin", f[0].GetNode(0).ApiKey,
			"DotmeshRPC.Fork",
			ForkRequest{
				MasterBranchID: lookupResp,
				ForkNamespace:  "alice",
				ForkName:       fsname2,
			},
			&forkResp)
		if err != nil {
			t.Error(err)
		}
		citools.RunOnNode(t, node1Name, fmt.Sprintf("echo %s | dm remote add alice alice@localhost", aliceKey))
		citools.RunOnNode(t, node1Name, "dm remote switch alice")
		output := citools.OutputFromRunOnNode(t, node1Name, "dm list")
		if !strings.Contains(output, fsname2) {
			t.Errorf("Did not find dot %s in output. Got: %s", fsname2, output)
		}
	})
}
