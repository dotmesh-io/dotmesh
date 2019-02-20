package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dotmesh-io/citools"

	"github.com/dotmesh-io/dotmesh/pkg/types"
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

	// Create user bob on the first node
	bobKey := "bob is great"
	err = citools.RegisterUser(f[0].GetNode(0), "bob", "bob@bob.com", bobKey)
	if err != nil {
		t.Error(err)
	}
	citools.RunOnNode(t, node1Name, fmt.Sprintf("echo %s | dm remote add bob bob@localhost", bobKey))

	aliceKey := "alice is also great"
	err = citools.RegisterUser(f[0].GetNode(0), "alice", "alice@alice.com", aliceKey)
	if err != nil {
		t.Error(err)
	}
	citools.RunOnNode(t, node1Name, fmt.Sprintf("echo %s | dm remote add alice alice@localhost", aliceKey))

	t.Run("CreateThenFork", func(t *testing.T) {
		fsname := citools.UniqName()

		// Bob makes a dot
		citools.RunOnNode(t, node1Name, "dm remote switch bob")
		citools.RunOnNode(t, node1Name, "dm init bob/"+fsname)
		citools.RunOnNode(t, node1Name, "dm switch bob/"+fsname)
		citools.RunOnNode(t, node1Name, "dm commit -m 'Nice Commit'")

		// Admin forks the dot
		fsname2 := citools.UniqName()

		var lookupResp string
		err = citools.DoRPC(f[0].GetNode(0).IP, "bob", bobKey,
			"DotmeshRPC.Lookup",
			VolumeName{Namespace: "bob", Name: fsname},
			&lookupResp)
		if err != nil {
			t.Error(err)
		}
		forkResp := ""
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

		// Alice looks for it
		citools.RunOnNode(t, node1Name, "dm remote switch alice")
		output := citools.OutputFromRunOnNode(t, node1Name, "dm list")
		if !strings.Contains(output, fsname2) {
			t.Errorf("Did not find dot %s in output. Got: %s", fsname2, output)
		}
		citools.RunOnNode(t, node1Name, "dm switch alice/"+fsname2)
		output = citools.OutputFromRunOnNode(t, node1Name, "dm log")
		if !strings.Contains(output, "Nice Commit") {
			t.Errorf("Did not find commit in output. Expected '%s', got: '%s'", "Nice Commit", output)
		}

		// Bob deletes the original
		citools.RunOnNode(t, node1Name, "dm remote switch bob")
		citools.RunOnNode(t, node1Name, "dm dot delete -f bob/"+fsname)

		// Alice still sees it
		citools.RunOnNode(t, node1Name, "dm remote switch alice")
		output = citools.OutputFromRunOnNode(t, node1Name, "dm list")
		if !strings.Contains(output, fsname2) {
			t.Errorf("Did not find dot %s in output. Got: %s", fsname2, output)
		}
		citools.RunOnNode(t, node1Name, "dm switch alice/"+fsname2)
		output = citools.OutputFromRunOnNode(t, node1Name, "dm log")
		if !strings.Contains(output, "Nice Commit") {
			t.Errorf("Did not find commit in output. Got: %s", output)
		}

		// Check List returns appropriate metadata
		var listResp map[string]map[string]types.DotmeshVolume
		err = citools.DoRPC(f[0].GetNode(0).IP, "alice", aliceKey,
			"DotmeshRPC.List",
			VolumeName{Namespace: "alice", Name: fsname2},
			&listResp)
		if err != nil {
			t.Error(err)
		}
		if listResp["alice"][fsname2].ForkParentId != lookupResp {
			t.Errorf("Expected fork parent %s, got %s", lookupResp, listResp["alice"][fsname2].ForkParentId)
		}

		// Check Get returns appropriate metadata
		var getResp types.DotmeshVolume
		err = citools.DoRPC(f[0].GetNode(0).IP, "alice", aliceKey,
			"DotmeshRPC.Get",
			listResp["alice"][fsname2].Id,
			&getResp)
		if err != nil {
			t.Error(err)
		}
		if getResp.ForkParentId != lookupResp {
			t.Errorf("Expected fork parent %s, got %s", lookupResp, getResp.ForkParentId)
		}
	})
}
