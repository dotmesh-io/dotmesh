package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

/*

Take a look at docs/dev-commands.md to see how to run these tests.

*/

func TestTeardownFinished(t *testing.T) {
	teardownFinishedTestRuns()
}

func TestSingleNode(t *testing.T) {
	// single node tests
	teardownFinishedTestRuns()

	f := Federation{NewCluster(1)}

	startTiming()
	err := f.Start(t)
	defer testMarkForCleanup(f)
	if err != nil {
		t.Error(err)
	}
	node1 := f[0].GetNode(0).Container

	// Sub-tests, to reuse common setup code.
	t.Run("Init", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, "dm init "+fsname)
		resp := s(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}
	})

	t.Run("InitDuplicate", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, "dm init "+fsname)

		resp := s(t, node1, "if dm init "+fsname+"; then false; else true; fi ")
		if !strings.Contains(resp, fmt.Sprintf("Error: %s exists already", fsname)) {
			t.Error("Didn't get an error when attempting to re-create the volume")
		}

		resp = s(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}
	})

	t.Run("InitCrashSafety", func(t *testing.T) {
		fsname := uniqName()

		_, err := doSetDebugFlag(f[0].GetNode(0).IP, "admin", f[0].GetNode(0).ApiKey, "PartialFailCreateFilesystem", "true")
		if err != nil {
			t.Error(err)
		}

		resp := s(t, node1, "if dm init "+fsname+"; then false; else true; fi ")
		if !strings.Contains(resp, "Injected fault") {
			t.Error("Couldn't inject fault into CreateFilesystem")
		}

		_, err = doSetDebugFlag(f[0].GetNode(0).IP, "admin", f[0].GetNode(0).ApiKey, "PartialFailCreateFilesystem", "false")
		if err != nil {
			t.Error(err)
		}

		// Now try again, and check it recovers and creates the volume
		d(t, node1, "dm init "+fsname)

		resp = s(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}
	})

	t.Run("Version", func(t *testing.T) {
		var parsedResponse []string
		var validVersion = regexp.MustCompile(`[A-Za-z-.0-9]+`)

		serverResponse := s(t, node1, "dm version")

		lines := strings.Split(serverResponse, "\n")
		for index, _ := range lines {
			parsedResponse = append(parsedResponse, strings.Fields(strings.TrimSpace(lines[index]))...)
		}

		if (parsedResponse[0] != "Client:") || parsedResponse[1] != "Version:" {
			t.Errorf("unable to find all version params in ouput: %v %v", parsedResponse[0], parsedResponse[1])
		}
		if !validVersion.MatchString(parsedResponse[2]) {
			t.Errorf("unable to find all version params in ouput: %v", serverResponse)
		}
		if parsedResponse[3] != "Server:" || parsedResponse[4] != "Version:" {
			t.Errorf("unable to find all version params in ouput: %v %v", parsedResponse[3], parsedResponse[4])
		}

		if !validVersion.MatchString(parsedResponse[5]) {
			t.Errorf("unable to find valid server version in ouput: %v", parsedResponse[5])
		}

		if contains(parsedResponse, "uninitialized") {
			t.Errorf("Version was uninitialized: %v", parsedResponse)
		}
	})

	t.Run("Commit", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, dockerRun(fsname)+" touch /foo/X")
		d(t, node1, "dm switch "+fsname)
		d(t, node1, "dm commit -m 'hello'")
		resp := s(t, node1, "dm log")
		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message in log output")
		}
	})

	t.Run("Branch", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, dockerRun(fsname)+" touch /foo/X")
		d(t, node1, "dm switch "+fsname)
		d(t, node1, "dm commit -m 'hello'")
		d(t, node1, "dm checkout -b branch1")
		d(t, node1, dockerRun(fsname)+" touch /foo/Y")
		d(t, node1, "dm commit -m 'there'")
		resp := s(t, node1, "dm log")
		if !strings.Contains(resp, "there") {
			t.Error("unable to find commit message in log output")
		}
		d(t, node1, "dm checkout master")
		resp = s(t, node1, dockerRun(fsname)+" ls /foo/")
		if strings.Contains(resp, "Y") {
			t.Error("failed to switch filesystem")
		}
		d(t, node1, "dm checkout branch1")
		resp = s(t, node1, dockerRun(fsname)+" ls /foo/")
		if !strings.Contains(resp, "Y") {
			t.Error("failed to switch filesystem")
		}
	})

	t.Run("Reset", func(t *testing.T) {
		fsname := uniqName()
		// Run a container in the background so that we can observe it get
		// restarted.
		d(t, node1,
			dockerRun(fsname, "-d --name sleeper")+" sleep 100",
		)
		initialStart := s(t, node1,
			"docker inspect sleeper |jq .[0].State.StartedAt",
		)

		d(t, node1, dockerRun(fsname)+" touch /foo/X")
		d(t, node1, "dm switch "+fsname)
		d(t, node1, "dm commit -m 'hello'")
		resp := s(t, node1, "dm log")
		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message in log output")
		}
		d(t, node1, dockerRun(fsname)+" touch /foo/Y")
		d(t, node1, "dm commit -m 'again'")
		resp = s(t, node1, "dm log")
		if !strings.Contains(resp, "again") {
			t.Error("unable to find commit message in log output")
		}
		d(t, node1, "dm reset --hard HEAD^")
		resp = s(t, node1, "dm log")
		if strings.Contains(resp, "again") {
			t.Error("found 'again' in dm log when i shouldn't have")
		}
		// check filesystem got rolled back
		resp = s(t, node1, dockerRun(fsname)+" ls /foo/")
		if strings.Contains(resp, "Y") {
			t.Error("failed to roll back filesystem")
		}
		newStart := s(t, node1,
			"docker inspect sleeper |jq .[0].State.StartedAt",
		)
		if initialStart == newStart {
			t.Errorf("container was not restarted during rollback (initialStart %v == newStart %v)", strings.TrimSpace(initialStart), strings.TrimSpace(newStart))
		}

	})

	t.Run("RunningContainersListed", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, dockerRun(fsname, "-d --name tester")+" sleep 100")
		err := tryUntilSucceeds(func() error {
			resp := s(t, node1, "dm list")
			if !strings.Contains(resp, "tester") {
				return fmt.Errorf("container running not listed")
			}
			return nil
		}, "listing containers")
		if err != nil {
			t.Error(err)
		}
	})

	// TODO test AllDotsAndBranches
	t.Run("AllDotsAndBranches", func(t *testing.T) {
		resp := s(t, node1, "dm debug AllDotsAndBranches")
		fmt.Printf("AllDotsAndBranches response: %v\n", resp)
	})

	// Exercise the import functionality which should already exists in Docker
	t.Run("ImportDockerImage", func(t *testing.T) {
		fsname := uniqName()
		resp := s(t, node1,
			// Mount the volume at /etc in the container. Docker should copy the
			// contents of /etc in the image over the top of the new blank volume.
			dockerRun(fsname, "--name import-test", "busybox", "/etc")+" cat /etc/passwd",
		)
		// "root" normally shows up in /etc/passwd
		if !strings.Contains(resp, "root") {
			t.Error("unable to find 'root' in expected output")
		}
		// If we reuse the volume, we should find the contents of /etc
		// imprinted therein.
		resp = s(t, node1,
			dockerRun(fsname, "--name import-test-2", "busybox", "/foo")+" cat /foo/passwd",
		)
		// "root" normally shows up in /etc/passwd
		if !strings.Contains(resp, "root") {
			t.Error("unable to find 'root' in expected output")
		}
	})
	// XXX This test doesn't fail on Docker 1.12.6, which is used
	// by dind, but it does fail without using
	// `fs.StringWithoutAdmin()` in docker.go due to manual testing
	// on docker 17.06.2-ce. Need to improve the test suite to use
	// a variety of versions of docker in dind environments.
	t.Run("RunningContainerTwice", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, dockerRun(fsname)+" touch /foo/HELLO")
		st := s(t, node1, dockerRun(fsname)+" ls /foo/HELLO")
		if !strings.Contains(st, "HELLO") {
			t.Errorf("Data did not persist between two instanciations of the same volume on the same host: %v", st)
		}
	})

	t.Run("BranchPinning", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, dockerRun(fsname)+" touch /foo/HELLO-ORIGINAL")
		d(t, node1, "dm switch "+fsname)
		d(t, node1, "dm commit -m original")

		d(t, node1, "dm checkout -b branch1")
		d(t, node1, dockerRun(fsname)+" touch /foo/HELLO-BRANCH1")
		d(t, node1, "dm commit -m branch1commit1")

		d(t, node1, "dm checkout master")
		d(t, node1, "dm checkout -b branch2")
		d(t, node1, dockerRun(fsname)+" touch /foo/HELLO-BRANCH2")
		d(t, node1, "dm commit -m branch2commit1")

		d(t, node1, "dm checkout master")
		d(t, node1, "dm checkout -b branch3")
		d(t, node1, dockerRun(fsname)+" touch /foo/HELLO-BRANCH3")
		d(t, node1, "dm commit -m branch3commit1")

		st := s(t, node1, dockerRun(fsname+"@branch1")+" ls /foo")
		if st != "HELLO-BRANCH1\nHELLO-ORIGINAL\n" {
			t.Errorf("Wrong content in branch 1: '%s'", st)
		}

		st = s(t, node1, dockerRun(fsname+"@branch2")+" ls /foo")
		if st != "HELLO-BRANCH2\nHELLO-ORIGINAL\n" {
			t.Errorf("Wrong content in branch 2: '%s'", st)
		}

		st = s(t, node1, dockerRun(fsname+"@branch3")+" ls /foo")
		if st != "HELLO-BRANCH3\nHELLO-ORIGINAL\n" {
			t.Errorf("Wrong content in branch 3: '%s'", st)
		}
	})

	t.Run("Subdots", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, dockerRun(fsname+".frogs")+" touch /foo/HELLO-FROGS")
		d(t, node1, dockerRun(fsname+".eat")+" touch /foo/HELLO-EAT")
		d(t, node1, dockerRun(fsname+".flies")+" touch /foo/HELLO-FLIES")
		d(t, node1, dockerRun(fsname+".__root__")+" touch /foo/HELLO-ROOT")
		st := s(t, node1, dockerRun(fsname+".__root__")+" find /foo -type f | sort")
		if st != "/foo/HELLO-ROOT\n/foo/eat/HELLO-EAT\n/foo/flies/HELLO-FLIES\n/foo/frogs/HELLO-FROGS\n" {
			t.Errorf("Subdots didn't work out: %s", st)
		}
	})

	t.Run("DefaultSubdot", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, dockerRun(fsname)+" touch /foo/HELLO-DEFAULT")
		d(t, node1, dockerRun(fsname+".__root__")+" touch /foo/HELLO-ROOT")
		st := s(t, node1, dockerRun(fsname+".__root__")+" find /foo -type f | sort")
		if st != "/foo/HELLO-ROOT\n/foo/__default__/HELLO-DEFAULT\n" {
			t.Errorf("Subdots didn't work out: %s", st)
		}
	})

	t.Run("ConcurrentSubdots", func(t *testing.T) {
		fsname := uniqName()

		d(t, node1, dockerRunDetached(fsname+".frogs")+" sh -c 'touch /foo/HELLO-FROGS; sleep 30'")
		d(t, node1, dockerRunDetached(fsname+".eat")+" sh -c 'touch /foo/HELLO-EAT; sleep 30'")
		d(t, node1, dockerRunDetached(fsname+".flies")+" sh -c 'touch /foo/HELLO-FLIES; sleep 30'")
		d(t, node1, dockerRunDetached(fsname+".__root__")+" sh -c 'touch /foo/HELLO-ROOT; sleep 30'")
		// Let everything get started
		time.Sleep(5)

		st := s(t, node1, "dm list")
		matched, err := regexp.MatchString("/[a-z]+_[a-z]+,/[a-z]+_[a-z]+,/[a-z]+_[a-z]+,/[a-z]+_[a-z]+", st)
		if err != nil {
			t.Error(err)
		}
		if !matched {
			t.Errorf("Couldn't find four containers attached to volume: %+v", st)
		}

		// Check combined state
		st = s(t, node1, dockerRun(fsname+".__root__")+" find /foo -type f | sort")
		if st != "/foo/HELLO-ROOT\n/foo/eat/HELLO-EAT\n/foo/flies/HELLO-FLIES\n/foo/frogs/HELLO-FROGS\n" {
			t.Errorf("Subdots didn't work out: %s", st)
		}

		// Check commits and branches work
		d(t, node1, "dm switch "+fsname)
		d(t, node1, "dm commit -m pod-commit")
		d(t, node1, "dm checkout -b branch") // Restarts the containers
		d(t, node1, dockerRun(fsname+"@branch.again")+" touch /foo/HELLO-AGAIN")
		d(t, node1, "dm commit -m branch-commit")

		// Check branch state
		st = s(t, node1, dockerRun(fsname+"@branch.__root__")+" find /foo -type f | sort")
		if st != "/foo/HELLO-ROOT\n/foo/again/HELLO-AGAIN\n/foo/eat/HELLO-EAT\n/foo/flies/HELLO-FLIES\n/foo/frogs/HELLO-FROGS\n" {
			t.Errorf("Subdots didn't work out on branch: %s", st)
		}

		// Check master state
		d(t, node1, "dm checkout master") // Restarts the containers
		st = s(t, node1, dockerRun(fsname+".__root__")+" find /foo -type f | sort")
		if st != "/foo/HELLO-ROOT\n/foo/eat/HELLO-EAT\n/foo/flies/HELLO-FLIES\n/foo/frogs/HELLO-FROGS\n" {
			t.Errorf("Subdots didn't work out back on master: %s", st)
		}

		// Check containers all got restarted
		st = s(t, node1, "docker ps | grep 'touch /foo' | wc -l")
		if st != "4\n" {
			t.Errorf("Subdot containers didn't get restarted")
			d(t, node1, "docker ps")
		}
	})

	t.Run("ApiKeys", func(t *testing.T) {
		apiKey := f[0].GetNode(0).ApiKey
		password := f[0].GetNode(0).Password

		var resp struct {
			ApiKey string
		}

		err := doRPC(f[0].GetNode(0).IP, "admin", apiKey,
			"DotmeshRPC.GetApiKey",
			struct {
			}{},
			&resp)
		if err != nil {
			t.Error(err)
		}
		if resp.ApiKey != apiKey {
			t.Errorf("Got API key %v, expected %v", resp.ApiKey, apiKey)
		}

		err = doRPC(f[0].GetNode(0).IP, "admin", apiKey,
			"DotmeshRPC.ResetApiKey",
			struct {
			}{},
			&resp)
		if err == nil {
			t.Errorf("Was able to reset API key without a password")
		}

		err = doRPC(f[0].GetNode(0).IP, "admin", password,
			"DotmeshRPC.ResetApiKey",
			struct {
			}{},
			&resp)
		if err != nil {
			t.Error(err)
		}
		if resp.ApiKey == apiKey {
			t.Errorf("Got API key %v, expected a new one!", resp.ApiKey, apiKey)
		}

		var user struct {
			Id          string
			Name        string
			Email       string
			EmailHash   string
			CustomerId  string
			CurrentPlan string
		}

		fmt.Printf("About to expect failure...\n")
		// Use old API key, expect failure
		err = doRPC(f[0].GetNode(0).IP, "admin", apiKey,
			"DotmeshRPC.CurrentUser",
			struct {
			}{},
			&resp)
		if err == nil {
			t.Errorf("Successfully used old API key")
		}

		fmt.Printf("About to expect success...\n")
		// Use new API key, expect success
		err = doRPC(f[0].GetNode(0).IP, "admin", resp.ApiKey,
			"DotmeshRPC.CurrentUser",
			struct {
			}{},
			&user)
		if err != nil {
			t.Error(err)
		}

		// UGLY HACK: This test must be LAST in the suite, as it leaves
		// the API key out of synch with what's in .dotmesh/config and
		// f[0].GetNode(0).ApiKey

		// FIXME: Update GetNode(0).ApiKey and on-disk remote API key so
		// later tests don't fail! We can do a "d(t, node1, sed -i
		// s/old/new/ /root/.dotmesh/config)" but we can't mutate
		// GetNode(0).ApiKey from here.
	})

}

func checkDeletionWorked(t *testing.T, fsname string, delay time.Duration, node1 string, node2 string) {
	// We return after the first failure, as there's little point
	// continuing (it just makes it hard to scroll back to the point of
	// initial failure).
	fmt.Printf("Sleeping for %d seconds. See comments in acceptance_test.go for why.\n", delay/time.Second)
	time.Sleep(delay)

	st := s(t, node1, "dm list")
	if strings.Contains(st, fsname) {
		t.Error(fmt.Sprintf("The volume is still in 'dm list' on node1 (after %d seconds)", delay/time.Second))
		return
	}

	st = s(t, node2, "dm list")
	if strings.Contains(st, fsname) {
		t.Error(fmt.Sprintf("The volume is still in 'dm list' on node2 (after %d seconds)", delay/time.Second))
		return
	}

	st = s(t, node1, dockerRun(fsname)+" cat /foo/HELLO || true")
	if strings.Contains(st, "WORLD") {
		t.Error(fmt.Sprintf("The volume name wasn't reusable %d seconds after delete on node 1...", delay/time.Second))
		return
	}

	st = s(t, node2, dockerRun(fsname)+" cat /foo/HELLO || true")
	if strings.Contains(st, "WORLD") {
		t.Error(fmt.Sprintf("The volume name wasn't reusable %d seconds after delete on node 2...", delay/time.Second))
		return
	}

	st = s(t, node1, "dm list")
	if !strings.Contains(st, fsname) {
		t.Error(fmt.Sprintf("The re-use of the deleted volume name failed in 'dm list' on node1 (after %d seconds)", delay/time.Second))
		return
	}

	st = s(t, node2, "dm list")
	if !strings.Contains(st, fsname) {
		t.Error(fmt.Sprintf("The re-use of the deleted volume name failed in 'dm list' on node2 (after %d seconds)", delay/time.Second))
		return
	}
}

func TestDeletionSimple(t *testing.T) {
	teardownFinishedTestRuns()

	clusterEnv := make(map[string]string)
	clusterEnv["FILESYSTEM_METADATA_TIMEOUT"] = "5"

	// Our cluster gets a metadata timeout of 5s
	f := Federation{NewClusterWithEnv(2, clusterEnv)}

	startTiming()
	err := f.Start(t)
	defer testMarkForCleanup(f)
	if err != nil {
		t.Error(err)
	}
	logTiming("setup")

	node1 := f[0].GetNode(0).Container
	node2 := f[0].GetNode(1).Container

	t.Run("DeleteNonexistantFails", func(t *testing.T) {
		fsname := uniqName()

		st := s(t, node1, "if dm dot delete -f "+fsname+"; then false; else true; fi")
		if !strings.Contains(st, "No such filesystem") {
			t.Error(fmt.Sprintf("Deleting a nonexistant volume didn't fail"))
		}
	})

	t.Run("DeleteInUseFails", func(t *testing.T) {
		fsname := uniqName()
		go func() {
			d(t, node1, dockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO; sleep 10'")
		}()

		// Give time for the container to start
		time.Sleep(5 * time.Second)

		// Delete, while the container is running! Which should fail!
		st := s(t, node1, "if dm dot delete -f "+fsname+"; then false; else true; fi")
		if !strings.Contains(st, "cannot delete the volume") {
			t.Error(fmt.Sprintf("The presence of a running container failed to suppress volume deletion"))
		}
	})

	t.Run("DeleteInstantly", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, dockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")
		d(t, node1, "dm dot delete -f "+fsname)

		st := s(t, node1, "dm list")
		if strings.Contains(st, fsname) {
			t.Error(fmt.Sprintf("The volume is still in 'dm list' on node1 (immediately after deletion)"))
			return
		}

		st = s(t, node1, dockerRun(fsname)+" cat /foo/HELLO || true")
		if strings.Contains(st, "WORLD") {
			t.Error(fmt.Sprintf("The volume name wasn't immediately reusable after deletion on node 1..."))
			return
		}

		/*
				         We don't try and guarantee immediate deletion on other nodes.
				         So the following may or may not fail, we can't test for it.

				   		st = s(t, node2, dockerRun(fsname)+" cat /foo/HELLO || true")
				   		if strings.Contains(st, "WORLD") {
				   			t.Error(fmt.Sprintf("The volume didn't get deleted on node 2..."))
			          		return
				   		}
		*/

	})

	t.Run("DeleteQuickly", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, dockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")
		d(t, node1, "dm dot delete -f "+fsname)

		// Ensure the initial delete has happened, but the metadata is
		// still draining. This is less than half the metadata timeout
		// configured above; the system should be in the process of
		// cleaning up after the volume, but it should be fine to reuse
		// the name by now.

		checkDeletionWorked(t, fsname, 2*time.Second, node1, node2)
	})

	t.Run("DeleteSlowly", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, dockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")
		d(t, node1, "dm dot delete -f "+fsname)

		// Ensure the delete has happened completely This is twice the
		// metadata timeout configured above, so all traces of the
		// volume should be gone and we get to see the result of the
		// "cleanupDeletedFilesystems" logic (has it ruined the
		// cluster?)

		checkDeletionWorked(t, fsname, 10*time.Second, node1, node2)
	})
}

func setupBranchesForDeletion(t *testing.T, fsname string, node1 string, node2 string) {
	// Set up some branches:
	//
	// Master -> branch1 -> branch2
	//   |
	//   \-> branch3

	d(t, node1, dockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")
	d(t, node1, "dm switch "+fsname)
	d(t, node1, "dm commit -m 'On master'")

	d(t, node1, "dm checkout -b branch1")
	d(t, node1, dockerRun(fsname)+" sh -c 'echo WORLD > /foo/GOODBYE'")
	d(t, node1, "dm commit -m 'On branch1'")

	d(t, node1, "dm checkout -b branch2")
	d(t, node1, dockerRun(fsname)+" sh -c 'echo WORLD > /foo/GOODBYE_CRUEL'")
	d(t, node1, "dm commit -m 'On branch2'")

	d(t, node1, "dm checkout master")
	d(t, node1, "dm checkout -b branch3")
	d(t, node1, dockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO_CRUEL'")
	d(t, node1, "dm commit -m 'On branch3'")
}

func TestDeletionComplex(t *testing.T) {
	teardownFinishedTestRuns()

	clusterEnv := make(map[string]string)
	clusterEnv["FILESYSTEM_METADATA_TIMEOUT"] = "5"

	// Our cluster gets a metadata timeout of 5s
	f := Federation{NewClusterWithEnv(2, clusterEnv)}

	startTiming()
	err := f.Start(t)
	defer testMarkForCleanup(f)
	if err != nil {
		t.Error(err)
	}
	logTiming("setup")

	node1 := f[0].GetNode(0).Container
	node2 := f[0].GetNode(1).Container

	t.Run("DeleteBranchesQuickly", func(t *testing.T) {
		fsname := uniqName()
		setupBranchesForDeletion(t, fsname, node1, node2)

		// Now kill the lot, right?
		d(t, node1, "dm dot delete -f "+fsname)

		// Test after two seconds, the state where the registry is cleared out but
		// the metadata remains.
		checkDeletionWorked(t, fsname, 2*time.Second, node1, node2)
	})

	t.Run("DeleteBranchesSlowly", func(t *testing.T) {

		fsname := uniqName()
		setupBranchesForDeletion(t, fsname, node1, node2)

		// Now kill the lot, right?
		d(t, node1, "dm dot delete -f "+fsname)

		// Test after tens econds, when all the metadata should be cleared out.
		checkDeletionWorked(t, fsname, 10*time.Second, node1, node2)
	})
}

func TestTwoNodesSameCluster(t *testing.T) {
	teardownFinishedTestRuns()

	f := Federation{NewCluster(2)}

	startTiming()
	err := f.Start(t)
	defer testMarkForCleanup(f)
	if err != nil {
		t.Error(err)
	}
	logTiming("setup")

	node1 := f[0].GetNode(0).Container
	node2 := f[0].GetNode(1).Container

	t.Run("Move", func(t *testing.T) {
		fsname := uniqName()
		d(t, node1, dockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")
		st := s(t, node2, dockerRun(fsname)+" cat /foo/HELLO")
		if !strings.Contains(st, "WORLD") {
			t.Error(fmt.Sprintf("Unable to find world in transported data capsule, got '%s'", st))
		}
	})
}

func TestTwoSingleNodeClusters(t *testing.T) {
	teardownFinishedTestRuns()

	f := Federation{
		NewCluster(1), // cluster_0_node_0
		NewCluster(1), // cluster_1_node_0
	}
	startTiming()
	err := f.Start(t)
	defer testMarkForCleanup(f)
	if err != nil {
		t.Error(err)
	}
	node1 := f[0].GetNode(0).Container
	node2 := f[1].GetNode(0).Container

	t.Run("PushCommitBranchExtantBase", func(t *testing.T) {
		fsname := uniqName()
		d(t, node2, dockerRun(fsname)+" touch /foo/X")
		d(t, node2, "dm switch "+fsname)
		d(t, node2, "dm commit -m 'hello'")
		d(t, node2, "dm push cluster_0")

		d(t, node1, "dm switch "+fsname)
		resp := s(t, node1, "dm log")
		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message remote's log output")
		}
		// test incremental push
		d(t, node2, "dm commit -m 'again'")
		d(t, node2, "dm push cluster_0")

		resp = s(t, node1, "dm log")
		if !strings.Contains(resp, "again") {
			t.Error("unable to find commit message remote's log output")
		}
		// test pushing branch with extant base
		d(t, node2, "dm checkout -b newbranch")
		d(t, node2, "dm commit -m 'branchy'")
		d(t, node2, "dm push cluster_0")

		d(t, node1, "dm checkout newbranch")
		resp = s(t, node1, "dm log")
		if !strings.Contains(resp, "branchy") {
			t.Error("unable to find commit message remote's log output")
		}
	})
	t.Run("PushCommitBranchNoExtantBase", func(t *testing.T) {
		fsname := uniqName()
		d(t, node2, dockerRun(fsname)+" touch /foo/X")
		// test pushing branch with no base on remote
		d(t, node2, "dm switch "+fsname)
		d(t, node2, "dm commit -m 'master'")
		d(t, node2, "dm checkout -b newbranch")
		d(t, node2, "dm commit -m 'branchy'")
		d(t, node2, "dm checkout -b newbranch2")
		d(t, node2, "dm commit -m 'branchy2'")
		d(t, node2, "dm checkout -b newbranch3")
		d(t, node2, "dm commit -m 'branchy3'")
		d(t, node2, "dm push cluster_0")

		d(t, node1, "dm switch "+fsname)
		d(t, node1, "dm checkout newbranch3")
		resp := s(t, node1, "dm log")
		if !strings.Contains(resp, "branchy3") {
			t.Error("unable to find commit message remote's log output")
		}
	})
	t.Run("DirtyDetected", func(t *testing.T) {
		fsname := uniqName()
		d(t, node2, dockerRun(fsname)+" touch /foo/X")
		d(t, node2, "dm switch "+fsname)
		d(t, node2, "dm commit -m 'hello'")
		d(t, node2, "dm push cluster_0")

		d(t, node1, "dm switch "+fsname)
		resp := s(t, node1, "dm log")
		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message remote's log output")
		}
		// now dirty the filesystem on node1 w/1MB before it can be received into
		d(t, node1, dockerRun(""+fsname+"")+" dd if=/dev/urandom of=/foo/Y bs=1024 count=1024")

		for i := 0; i < 10; i++ {
			dirty, err := strconv.Atoi(strings.TrimSpace(
				s(t, node1, "dm list -H |grep "+fsname+" |cut -f 7"),
			))
			if err != nil {
				t.Error(err)
			}
			if dirty > 0 {
				break
			}
			fmt.Printf("Not dirty yet, waiting...\n")
			time.Sleep(time.Duration(i) * time.Second)
		}

		// test incremental push
		d(t, node2, "dm commit -m 'again'")
		result := s(t, node2, "dm push cluster_0 || true") // an error code is ok

		if !strings.Contains(result, "uncommitted") {
			t.Error("pushing didn't fail when there were known uncommited changes on the peer")
		}
	})
	t.Run("DirtyImmediate", func(t *testing.T) {
		fsname := uniqName()
		d(t, node2, dockerRun(fsname)+" touch /foo/X")
		d(t, node2, "dm switch "+fsname)
		d(t, node2, "dm commit -m 'hello'")
		d(t, node2, "dm push cluster_0")

		d(t, node1, "dm switch "+fsname)
		resp := s(t, node1, "dm log")
		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message remote's log output")
		}
		// now dirty the filesystem on node1 w/1MB before it can be received into
		d(t, node1, dockerRun(""+fsname+"")+" dd if=/dev/urandom of=/foo/Y bs=1024 count=1024")

		// test incremental push
		d(t, node2, "dm commit -m 'again'")
		result := s(t, node2, "dm push cluster_0 || true") // an error code is ok

		if !strings.Contains(result, "has been modified") {
			t.Error(
				"pushing didn't fail when there were known uncommited changes on the peer",
			)
		}
	})
	t.Run("Diverged", func(t *testing.T) {
		fsname := uniqName()
		d(t, node2, dockerRun(fsname)+" touch /foo/X")
		d(t, node2, "dm switch "+fsname)
		d(t, node2, "dm commit -m 'hello'")
		d(t, node2, "dm push cluster_0")

		d(t, node1, "dm switch "+fsname)
		resp := s(t, node1, "dm log")
		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message remote's log output")
		}
		// now make a commit that will diverge the filesystems
		d(t, node1, "dm commit -m 'node1 commit'")

		// test incremental push
		d(t, node2, "dm commit -m 'node2 commit'")
		result := s(t, node2, "dm push cluster_0 || true") // an error code is ok

		if !strings.Contains(result, "diverged") && !strings.Contains(result, "hello") {
			t.Error(
				"pushing didn't fail when there was a divergence",
			)
		}
	})
	t.Run("ResetAfterPushThenPushMySQL", func(t *testing.T) {
		fsname := uniqName()
		d(t, node2, dockerRun(
			fsname, "-d -e MYSQL_ROOT_PASSWORD=secret", "mysql:5.7.17", "/var/lib/mysql",
		))
		time.Sleep(10 * time.Second)
		d(t, node2, "dm switch "+fsname)
		d(t, node2, "dm commit -m 'hello'")
		d(t, node2, "dm push cluster_0")

		d(t, node1, "dm switch "+fsname)
		resp := s(t, node1, "dm log")
		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message remote's log output")
		}
		// now make a commit that will diverge the filesystems
		d(t, node1, "dm commit -m 'node1 commit'")

		// test resetting a commit made on a pushed volume
		d(t, node2, "dm commit -m 'node2 commit'")
		d(t, node1, "dm reset --hard HEAD^")
		resp = s(t, node1, "dm log")
		if strings.Contains(resp, "node1 commit") {
			t.Error("found 'node1 commit' in dm log when i shouldn't have")
		}
		d(t, node2, "dm push cluster_0")
		resp = s(t, node1, "dm log")
		if !strings.Contains(resp, "node2 commit") {
			t.Error("'node2 commit' didn't make it over to node1 after reset-and-push")
		}
	})
	t.Run("PushToAuthorizedUser", func(t *testing.T) {
		// TODO
		// create a user on the second cluster. on the first cluster, push a
		// volume that user's account.
	})
	t.Run("NoPushToUnauthorizedUser", func(t *testing.T) {
		// TODO
		// a user can't push to a volume they're not authorized to push to.
	})
	t.Run("PushToCollaboratorVolume", func(t *testing.T) {
		// TODO
		// after adding another user as a collaborator, it's possible to push
		// to their volume.
	})
	t.Run("Clone", func(t *testing.T) {
		fsname := uniqName()
		d(t, node2, dockerRun(fsname)+" touch /foo/X")
		d(t, node2, "dm switch "+fsname)
		d(t, node2, "dm commit -m 'hello'")
		// XXX 'dm clone' currently tries to pull the named filesystem into the
		// _current active filesystem name_. instead, it should pull it into a
		// new filesystem with the same name. if the same named filesystem
		// already exists, it should error (and instruct the user to 'dm switch
		// foo; dm pull foo' instead).
		d(t, node1, "dm clone cluster_1 "+fsname)
		d(t, node1, "dm switch "+fsname)
		resp := s(t, node1, "dm log")
		if !strings.Contains(resp, "hello") {
			// TODO fix this failure by sending prelude in intercluster case also
			t.Error("unable to find commit message remote's log output")
		}
		// test incremental pull
		d(t, node2, "dm commit -m 'again'")
		d(t, node1, "dm pull cluster_1 "+fsname)

		resp = s(t, node1, "dm log")
		if !strings.Contains(resp, "again") {
			t.Error("unable to find commit message remote's log output")
		}
		// test pulling branch with extant base
		d(t, node2, "dm checkout -b newbranch")
		d(t, node2, "dm commit -m 'branchy'")
		d(t, node1, "dm pull cluster_1 "+fsname+" newbranch")

		d(t, node1, "dm checkout newbranch")
		resp = s(t, node1, "dm log")
		if !strings.Contains(resp, "branchy") {
			t.Error("unable to find commit message remote's log output")
		}
	})

	t.Run("Bug74MissingMetadata", func(t *testing.T) {
		fsname := uniqName()

		// Commit 1
		d(t, node1, dockerRun(fsname)+" touch /foo/X")
		d(t, node1, "dm switch "+fsname)
		d(t, node1, "dm commit -m 'hello'")

		// Commit 2
		d(t, node1, dockerRun(fsname)+" touch /foo/Y")
		d(t, node1, "dm commit -m 'again'")

		// Ahhh, push it! https://www.youtube.com/watch?v=vCadcBR95oU
		d(t, node1, "dm push cluster_1 "+fsname)

		// What do we get on node2?
		d(t, node2, "dm switch "+fsname)
		resp := s(t, node2, "dm log")
		if !strings.Contains(resp, "hello") ||
			!strings.Contains(resp, "again") {
			t.Error("Some history went missing (if it's bug#74 again, probably the 'hello')")
		}
	})

	t.Run("VolumeNameValidityChecking", func(t *testing.T) {
		// 1) dm init
		resp := s(t, node1, "if dm init @; then false; else true; fi ")
		if !strings.Contains(resp, "Invalid dot name") {
			t.Error("Didn't get an error when attempting to dm init an invalid volume name: %s", resp)
		}

		// 2) pull/clone it
		fsname := uniqName()

		d(t, node2, dockerRun(fsname)+" touch /foo/X")
		d(t, node2, "dm switch "+fsname)
		d(t, node2, "dm commit -m 'hello'")

		resp = s(t, node1, "if dm clone cluster_0 "+fsname+" --local-name @; then false; else true; fi ")
		if !strings.Contains(resp, "Invalid dot name") {
			t.Error("Didn't get an error when attempting to dm clone to an invalid volume name: %s", resp)
		}

		resp = s(t, node1, "if dm pull cluster_0 @ --remote-name "+fsname+"; then false; else true; fi ")
		if !strings.Contains(resp, "Invalid dot name") {
			t.Error("Didn't get an error when attempting to dm pull to an invalid volume name: %s", resp)
		}

		// 3) push it
		resp = s(t, node2, "if dm push cluster_0 "+fsname+" --remote-name @; then false; else true; fi ")
		if !strings.Contains(resp, "Invalid dot name") {
			t.Error("Didn't get an error when attempting to dm push to an invalid volume name: %s", resp)
		}
	})
}

func TestFrontend(t *testing.T) {
	teardownFinishedTestRuns()

	if os.Getenv("STRIPE_PUBLIC_KEY") == "" {
		t.Error("STRIPE_PUBLIC_KEY variable needed")
		return
	}

	if os.Getenv("STRIPE_SECRET_KEY") == "" {
		t.Error("STRIPE_SECRET_KEY variable needed")
		return
	}

	f := Federation{NewCluster(1)}

	userLogin := uniqLogin()

	startTiming()
	err := f.Start(t)
	defer testMarkForCleanup(f)
	if err != nil {
		t.Error(err)
	}

	node1 := f[0].GetNode(0).Container

	t.Run("Frontend", func(t *testing.T) {

		// create a docker-net so we can join the dotmesh-server with billing & communications
		createDockerNetwork(t, node1)

		startFrontend(t, node1)
		// start auxillary services
		startBilling(t, node1, f[0].GetNode(0).ApiKey)
		startCommunications(t, node1)
		startGotty(t, node1)
		startChromeDriver(t, node1)
		startRouter(t, node1)

		time.Sleep(5000 * time.Millisecond)

		// we don't clean up the auxillary containers because the entire
		// dind environment is marked for cleanup and seeing the logs is useful
		defer copyMedia(node1)

		FRONTEND_TEST_NAME := os.Getenv("FRONTEND_TEST_NAME")

		if FRONTEND_TEST_NAME != "" {
			runFrontendTest(t, node1, FRONTEND_TEST_NAME, userLogin)
		} else {
			runFrontendTest(t, node1, "specs/auth.js", userLogin)
			runFrontendTest(t, node1, "specs/updatepassword.js", userLogin)
			runFrontendTest(t, node1, "specs/branches.js", userLogin)
			runFrontendTest(t, node1, "specs/endtoend.js", userLogin)
			runFrontendTest(t, node1, "specs/payment.js", userLogin)
			runFrontendTest(t, node1, "specs/registeremail.js", userLogin)
		}
	})
}

func TestThreeSingleNodeClusters(t *testing.T) {
	teardownFinishedTestRuns()

	f := Federation{
		NewCluster(1), // cluster_0_node_0 - common
		NewCluster(1), // cluster_1_node_0 - alice
		NewCluster(1), // cluster_2_node_0 - bob
	}
	startTiming()
	err := f.Start(t)
	defer testMarkForCleanup(f)
	if err != nil {
		t.Error(err)
	}
	commonNode := f[0].GetNode(0)
	aliceNode := f[1].GetNode(0)
	bobNode := f[2].GetNode(0)

	bobKey := "bob is great"
	aliceKey := "alice is great"

	// Create users bob and alice on the common node
	err = registerUser(commonNode, "bob", "bob@bob.com", bobKey)
	if err != nil {
		t.Error(err)
	}

	err = registerUser(commonNode, "alice", "alice@bob.com", aliceKey)
	if err != nil {
		t.Error(err)
	}

	t.Run("TwoUsersSameNamedVolume", func(t *testing.T) {

		// bob and alice both push to the common node
		d(t, aliceNode.Container, dockerRun("apples")+" touch /foo/alice")
		d(t, aliceNode.Container, "dm switch apples")
		d(t, aliceNode.Container, "dm commit -m'Alice commits'")
		d(t, aliceNode.Container, "dm push cluster_0 apples --remote-name alice/apples")

		d(t, bobNode.Container, dockerRun("apples")+" touch /foo/bob")
		d(t, bobNode.Container, "dm switch apples")
		d(t, bobNode.Container, "dm commit -m'Bob commits'")
		d(t, bobNode.Container, "dm push cluster_0 apples --remote-name bob/apples")

		// bob and alice both clone from the common node
		d(t, aliceNode.Container, "dm clone cluster_0 bob/apples --local-name bob-apples")
		d(t, bobNode.Container, "dm clone cluster_0 alice/apples --local-name alice-apples")

		// Check they get the right volumes
		resp := s(t, commonNode.Container, "dm list -H | cut -f 1 | grep apples")
		if resp != "alice/apples\nbob/apples\n" {
			t.Error("Didn't find alice/apples and bob/apples on common node")
		}

		resp = s(t, aliceNode.Container, "dm list -H | cut -f 1 | grep apples")
		if resp != "apples\nbob-apples\n" {
			t.Error("Didn't find apples and bob-apples on alice's node")
		}

		resp = s(t, bobNode.Container, "dm list -H | cut -f 1 | grep apples")
		if resp != "alice-apples\napples\n" {
			t.Error("Didn't find apples and alice-apples on bob's node")
		}

		// Check the volumes actually have the contents they should
		resp = s(t, aliceNode.Container, dockerRun("bob-apples")+" ls /foo/")
		if !strings.Contains(resp, "bob") {
			t.Error("Filesystem bob-apples had the wrong content")
		}

		resp = s(t, bobNode.Container, dockerRun("alice-apples")+" ls /foo/")
		if !strings.Contains(resp, "alice") {
			t.Error("Filesystem alice-apples had the wrong content")
		}

		// bob commits again
		d(t, bobNode.Container, dockerRun("apples")+" touch /foo/bob2")
		d(t, bobNode.Container, "dm switch apples")
		d(t, bobNode.Container, "dm commit -m'Bob commits again'")
		d(t, bobNode.Container, "dm push cluster_0 apples --remote-name bob/apples")

		// alice pulls it
		d(t, aliceNode.Container, "dm pull cluster_0 bob-apples --remote-name bob/apples")

		// Check we got the change
		resp = s(t, aliceNode.Container, dockerRun("bob-apples")+" ls /foo/")
		if !strings.Contains(resp, "bob2") {
			t.Error("Filesystem bob-apples had the wrong content")
		}
	})

	t.Run("ShareBranches", func(t *testing.T) {
		// Alice pushes
		d(t, aliceNode.Container, dockerRun("cress")+" touch /foo/alice")
		d(t, aliceNode.Container, "dm switch cress")
		d(t, aliceNode.Container, "dm commit -m'Alice commits'")
		d(t, aliceNode.Container, "dm push cluster_0 cress --remote-name alice/cress")

		// Generate branch and push
		d(t, aliceNode.Container, "dm checkout -b mustard")
		d(t, aliceNode.Container, dockerRun("cress")+" touch /foo/mustard")
		d(t, aliceNode.Container, "dm commit -m'Alice commits mustard'")
		d(t, aliceNode.Container, "dm push cluster_0 cress mustard --remote-name alice/cress")
		/*
		   COMMON
		   testpool-1508755395558066569-0-node-0/dmfs/61f356f0-39a4-4e0e-6286-e04d25744344                                         19K  9.63G    19K  legacy
		   testpool-1508755395558066569-0-node-0/dmfs/61f356f0-39a4-4e0e-6286-e04d25744344@b8b3c196-2caa-4562-6995-51df3b4bc494      0      -    19K  -

		   ALICE
		   testpool-1508755395558066569-1-node-0/dmfs/05652ba4-acb8-4349-4711-956bd0c88c8c                                          9K  9.63G    19K  legacy
		   testpool-1508755395558066569-1-node-0/dmfs/61f356f0-39a4-4e0e-6286-e04d25744344                                         19K  9.63G    19K  legacy
		   testpool-1508755395558066569-1-node-0/dmfs/61f356f0-39a4-4e0e-6286-e04d25744344@b8b3c196-2caa-4562-6995-51df3b4bc494      0      -    19K  -

		   BOB
		   testpool-1508755395558066569-2-node-0                                                                                  142K  9.63G    19K  /dotmesh-test-pools/testpool-1508755395558066569-2-node-0/mnt
		   testpool-1508755395558066569-2-node-0/dmfs                                                                              38K  9.63G    19K  legacy
		   testpool-1508755395558066569-2-node-0/dmfs/05652ba4-acb8-4349-4711-956bd0c88c8c                                         19K  9.63G    19K  legacy
		   testpool-1508755395558066569-2-node-0/dmfs/05652ba4-acb8-4349-4711-956bd0c88c8c@b8b3c196-2caa-4562-6995-51df3b4bc494      0      -    19K  -
		*/
		// Bob clones the branch
		d(t, bobNode.Container, "dm clone cluster_0 alice/cress mustard --local-name cress")
		d(t, bobNode.Container, "dm switch cress")
		d(t, bobNode.Container, "dm checkout mustard")

		// Check we got both changes
		// TODO: had to pin the branch here, seems like `dm switch V; dm
		// checkout B; docker run C ... -v V:/...` doesn't result in V@B being
		// mounted into C. this is probably surprising behaviour.
		resp := s(t, bobNode.Container, dockerRun("cress@mustard")+" ls /foo/")
		if !strings.Contains(resp, "alice") {
			t.Error("We didn't get the master branch")
		}
		if !strings.Contains(resp, "mustard") {
			t.Error("We didn't get the mustard branch")
		}
	})

	t.Run("DefaultRemoteNamespace", func(t *testing.T) {
		// Alice pushes to the common node with no explicit remote volume, should default to alice/pears
		d(t, aliceNode.Container, dockerRun("pears")+" touch /foo/alice")
		d(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_pears alice@"+commonNode.IP)
		d(t, aliceNode.Container, "dm switch pears")
		d(t, aliceNode.Container, "dm commit -m'Alice commits'")
		d(t, aliceNode.Container, "dm push common_pears") // local pears becomes alice/pears

		// Check it gets there
		resp := s(t, commonNode.Container, "dm list -H | cut -f 1 | sort")
		if !strings.Contains(resp, "alice/pears") {
			t.Error("Didn't find alice/pears on the common node")
		}
	})

	t.Run("DefaultRemoteVolume", func(t *testing.T) {
		// Alice pushes to the common node with no explicit remote volume, should default to alice/pears
		d(t, aliceNode.Container, dockerRun("bananas")+" touch /foo/alice")
		d(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_bananas alice@"+commonNode.IP)
		d(t, aliceNode.Container, "dm switch bananas")
		d(t, aliceNode.Container, "dm commit -m'Alice commits'")
		d(t, aliceNode.Container, "dm push common_bananas bananas")

		// Check the remote branch got recorded
		resp := s(t, aliceNode.Container, "dm dot show -H bananas | grep defaultUpstreamDot")
		if resp != "defaultUpstreamDot\tcommon_bananas\talice/bananas\n" {
			t.Error("alice/bananas is not the default remote for bananas on common_bananas")
		}

		// Add Bob as a collaborator
		err := doAddCollaborator(commonNode.IP, "alice", aliceKey, "alice", "bananas", "bob")
		if err != nil {
			t.Error(err)
		}

		// Clone it back as bob
		d(t, bobNode.Container, "echo '"+bobKey+"' | dm remote add common_bananas bob@"+commonNode.IP)
		// Clone should save admin/bananas@common => alice/bananas
		d(t, bobNode.Container, "dm clone common_bananas alice/bananas --local-name bananas")
		d(t, bobNode.Container, "dm switch bananas")

		// Check it did so
		resp = s(t, bobNode.Container, "dm dot show -H bananas | grep defaultUpstreamDot")
		if resp != "defaultUpstreamDot\tcommon_bananas\talice/bananas\n" {
			t.Error("alice/bananas is not the default remote for bananas on common_bananas")
		}

		// And then do a pull, not specifying the remote or local volume
		// There is no bob/bananas, so this will fail if the default remote volume is not saved.
		d(t, bobNode.Container, "dm pull common_bananas") // local = bananas as we switched, remote = alice/banas from saved default

		// Now push back
		d(t, bobNode.Container, dockerRun("bananas")+" touch /foo/bob")
		d(t, bobNode.Container, "dm commit -m'Bob commits'")
		d(t, bobNode.Container, "dm push common_bananas") // local = bananas as we switched, remote = alice/banas from saved default
	})

	t.Run("DefaultRemoteNamespaceOverride", func(t *testing.T) {
		// Alice pushes to the common node with no explicit remote volume, should default to alice/kiwis
		d(t, aliceNode.Container, dockerRun("kiwis")+" touch /foo/alice")
		d(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_kiwis alice@"+commonNode.IP)
		d(t, aliceNode.Container, "dm switch kiwis")
		d(t, aliceNode.Container, "dm commit -m'Alice commits'")
		d(t, aliceNode.Container, "dm push common_kiwis") // local kiwis becomes alice/kiwis

		// Check the remote branch got recorded
		resp := s(t, aliceNode.Container, "dm dot show -H kiwis | grep defaultUpstreamDot")
		if resp != "defaultUpstreamDot\tcommon_kiwis\talice/kiwis\n" {
			t.Error("alice/kiwis is not the default remote for kiwis on common_kiwis")
		}

		// Manually override it (the remote repo doesn't need to exist)
		d(t, aliceNode.Container, "dm dot set-upstream common_kiwis bob/kiwis")

		// Check the remote branch got changed
		resp = s(t, aliceNode.Container, "dm dot show -H kiwis | grep defaultUpstreamDot")
		if resp != "defaultUpstreamDot\tcommon_kiwis\tbob/kiwis\n" {
			t.Error("bob/kiwis is not the default remote for kiwis on common_kiwis, looks like the set-upstream failed")
		}
	})

	t.Run("DeleteNotMineFails", func(t *testing.T) {
		fsname := uniqName()

		// Alice pushes to the common node with no explicit remote volume, should default to alice/fsname
		d(t, aliceNode.Container, dockerRun(fsname)+" touch /foo/alice")
		d(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_"+fsname+" alice@"+commonNode.IP)
		d(t, aliceNode.Container, "dm switch "+fsname)
		d(t, aliceNode.Container, "dm commit -m'Alice commits'")
		d(t, aliceNode.Container, "dm push common_"+fsname) // local fsname becomes alice/fsname

		// Bob tries to delete it
		d(t, bobNode.Container, "echo '"+bobKey+"' | dm remote add common_"+fsname+" bob@"+commonNode.IP)
		d(t, bobNode.Container, "dm remote switch common_"+fsname)
		// We expect failure, so reverse the sense
		resp := s(t, bobNode.Container, "if dm dot delete -f alice/"+fsname+"; then false; else true; fi")
		if !strings.Contains(resp, "You are not the owner") {
			t.Error("bob was able to delete alices' volumes")
		}
	})

	t.Run("NamespaceAuthorisationNonexistant", func(t *testing.T) {
		d(t, aliceNode.Container, dockerRun("grapes")+" touch /foo/alice")
		d(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_grapes alice@"+commonNode.IP)
		d(t, aliceNode.Container, "dm switch grapes")
		d(t, aliceNode.Container, "dm commit -m'Alice commits'")

		// Let's try and put things in a nonexistant namespace
		// Likewise, This SHOULD fail, so we reverse the sense of the return code.
		d(t, aliceNode.Container, "if dm push common_grapes --remote-name nonexistant/grapes; then exit 1; else exit 0; fi")

		// Check it doesn't get there
		resp := s(t, commonNode.Container, "dm list -H | cut -f 1 | sort")
		if strings.Contains(resp, "nonexistant/grapes") {
			t.Error("Found nonexistant/grapes on the common node - but alice shouldn't have been able to create that!")
		}
	})

	t.Run("NamespaceAuthorisation", func(t *testing.T) {
		d(t, aliceNode.Container, dockerRun("passionfruit")+" touch /foo/alice")
		d(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_passionfruit alice@"+commonNode.IP)
		d(t, aliceNode.Container, "dm switch passionfruit")
		d(t, aliceNode.Container, "dm commit -m'Alice commits'")

		// Let's try and put things in bob's namespace.
		// This SHOULD fail, so we reverse the sense of the return code.
		d(t, aliceNode.Container, "if dm push common_passionfruit --remote-name bob/passionfruit; then exit 1; else exit 0; fi")

		// Check it doesn't get there
		resp := s(t, commonNode.Container, "dm list -H | cut -f 1 | sort")
		if strings.Contains(resp, "bob/passionfruit") {
			t.Error("Found bob/passionfruit on the common node - but alice shouldn't have been able to create that!")
		}
	})

	t.Run("NamespaceAuthorisationAdmin", func(t *testing.T) {
		d(t, aliceNode.Container, dockerRun("prune")+" touch /foo/alice")
		d(t, aliceNode.Container, "dm switch prune")
		d(t, aliceNode.Container, "dm commit -m'Alice commits'")

		// Let's try and put things in bob's namespace, but using the cluster_0 remote which is logged in as admin
		// This should work, because we're admin, even though it's bob's namespace
		d(t, aliceNode.Container, "dm push cluster_0 --remote-name bob/prune")

		// Check it got there
		resp := s(t, commonNode.Container, "dm list -H | cut -f 1 | sort")
		if !strings.Contains(resp, "bob/prune") {
			t.Error("Didn't find bob/prune on the common node - but alice should have been able to create that using her admin account!")
		}
	})

}

func TestKubernetes(t *testing.T) {
	teardownFinishedTestRuns()

	f := Federation{NewKubernetes(3)}

	startTiming()
	err := f.Start(t)
	defer testMarkForCleanup(f)
	if err != nil {
		t.Error(err)
	}
	node1 := f[0].GetNode(0)

	t.Run("FlexVolume", func(t *testing.T) {

		// dm list should succeed in connecting to the dotmesh cluster
		d(t, node1.Container, "dm list")

		// init a dotmesh volume and put some data in it
		d(t, node1.Container,
			"docker run --rm -i -v apples:/foo --volume-driver dm "+
				"busybox sh -c \"echo 'apples' > /foo/on-the-tree\"",
		)

		// create a PV referencing the data
		kubectlApply(t, node1.Container, `
kind: PersistentVolume
apiVersion: v1
metadata:
  name: admin-apples
  labels:
    apples: tree
spec:
  storageClassName: manual
  capacity:
    storage: 1Gi
  accessModes:
    - ReadWriteOnce
  flexVolume:
    driver: dotmesh.io/dm
    options:
      namespace: admin
      name: apples
`)
		// run a pod with a PVC which lists the data (web server)
		// check that the output of querying the pod is that we can see
		// that the apples are on the tree
		kubectlApply(t, node1.Container, `
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: admin-apples-pvc
spec:
  storageClassName: manual
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
  selector:
    matchLabels:
      apples: tree
`)

		kubectlApply(t, node1.Container, `
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: apple-deployment
spec:
  replicas: 1
  template:
    metadata:
      labels:
        app: apple-server
    spec:
      volumes:
      - name: apple-storage
        persistentVolumeClaim:
         claimName: admin-apples-pvc
      containers:
      - name: apple-server
        image: nginx:1.12.1
        volumeMounts:
        - mountPath: "/usr/share/nginx/html"
          name: apple-storage
`)

		kubectlApply(t, node1.Container, `
apiVersion: v1
kind: Service
metadata:
   name: apple-service
spec:
   type: NodePort
   selector:
       app: apple-server
   ports:
     - port: 80
       nodePort: 30003
`)

		err = tryUntilSucceeds(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s:30003/on-the-tree", node1.IP))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			if !strings.Contains(string(body), "apples") {
				return fmt.Errorf("No apples on the tree, got this instead: %v", string(body))
			}
			return nil
		}, "finding apples on the tree")
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("DynamicProvisioning", func(t *testing.T) {
		// dm list should succeed in connecting to the dotmesh cluster
		d(t, node1.Container, "dm list")

		// Ok, now we have the plumbing set up, try creating a PVC and see if it gets a PV dynamically provisioned
		kubectlApply(t, node1.Container, `
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: admin-grapes-pvc
  annotations:
    # Also available: dotmeshNamespace (defaults to the one from the storage class)
    dotmeshNamespace: k8s
    dotmeshName: dynamic-grapes
    dotmeshSubdot: static-html
spec:
  storageClassName: dotmesh
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
`)

		err = tryUntilSucceeds(func() error {
			result := s(t, node1.Container, "kubectl get pv")
			// We really want a line like:
			// "pvc-85b6beb0-bb1f-11e7-8633-0242ff9ba756   1Gi        RWO           Delete          Bound     default/admin-grapes-pvc   dotmesh                 15s"
			if !strings.Contains(result, "default/admin-grapes-pvc") {
				return fmt.Errorf("grapes PV didn't get created")
			}
			return nil
		}, "finding the grapes PV")
		if err != nil {
			t.Error(err)
		}

		// Now let's see if a container can see it, and put content there that a k8s container can pick up

		d(t, node1.Container,
			"docker run --rm -i -v k8s/dynamic-grapes.static-html:/foo --volume-driver dm "+
				"busybox sh -c \"echo 'grapes' > /foo/on-the-vine\"",
		)

		kubectlApply(t, node1.Container, `
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: grape-deployment
spec:
  replicas: 1
  template:
    metadata:
      labels:
        app: grape-server
    spec:
      volumes:
      - name: grape-storage
        persistentVolumeClaim:
         claimName: admin-grapes-pvc
      containers:
      - name: grape-server
        image: nginx:1.12.1
        volumeMounts:
        - mountPath: "/usr/share/nginx/html"
          name: grape-storage
`)

		kubectlApply(t, node1.Container, `
apiVersion: v1
kind: Service
metadata:
   name: grape-service
spec:
   type: NodePort
   selector:
       app: grape-server
   ports:
     - port: 80
       nodePort: 30050
`)

		err = tryUntilSucceeds(func() error {
			resp, err := http.Get(fmt.Sprintf("http://%s:30050/on-the-vine", node1.IP))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			if !strings.Contains(string(body), "grapes") {
				return fmt.Errorf("No grapes on the vine, got this instead: %v", string(body))
			}
			return nil
		}, "finding grapes on the vine")
		if err != nil {
			t.Error(err)
		}
	})

}

func TestStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress tests in short mode.")
	}

	teardownFinishedTestRuns()

	// Tests in this suite should not assume how many nodes we have in
	// the cluster, and iterate over f[0].GetNodes, so that we can
	// scale it to the test hardware we have. Might even pick up the
	// cluster size from an env variable.
	f := Federation{
		NewCluster(5),
	}
	startTiming()
	err := f.Start(t)
	defer testMarkForCleanup(f)
	if err != nil {
		t.Error(err)
	}
	commonNode := f[0].GetNode(0)

	t.Run("HandoverStressTest", func(t *testing.T) {
		fsname := uniqName()
		d(t, commonNode.Container, dockerRun(fsname)+" sh -c 'echo STUFF > /foo/whatever'")

		for iteration := 0; iteration <= 10; iteration++ {
			for nid, node := range f[0].GetNodes() {
				runId := fmt.Sprintf("%d/%d", iteration, nid)
				st := s(t, node.Container, dockerRun(fsname)+" sh -c 'echo "+runId+"; cat /foo/whatever'")
				if !strings.Contains(st, "STUFF") {
					t.Error(fmt.Sprintf("We didn't see the STUFF we expected"))
				}
			}
		}
	})
}
