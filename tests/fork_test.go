package main

import (
	"testing"

	"github.com/dotmesh-io/citools"
)

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
		createResp := false
		err = citools.DoRPC(f[0].GetNode(0).IP, "bob", bobKey,
			"DotmeshRPC.Create",
			VolumeName{Namespace: "bob", Name: fsname},
			&createResp)
		if err != nil {
			t.Error(err)
		}
		lookupResp := ""
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
			lookupResp,
			&forkResp)
		if err != nil {
			t.Error(err)
		}
	})

}
