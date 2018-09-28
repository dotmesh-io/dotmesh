package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dotmesh-io/citools"
)

/*

Take a look at docs/dev-commands.md to see how to run these tests.

*/

func TestMain(m *testing.M) {
	citools.InitialCleanup()
	retCode := m.Run()
	citools.FinalCleanup(retCode)
	os.Exit(retCode)
}

func TestDefaultDot(t *testing.T) {
	// Test default dot select on a totally fresh cluster
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewCluster(1)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	node1 := f[0].GetNode(0).Container

	// These test MUST BE RUN ON A CLUSTER WITH NO DOTS.
	// Ensure that any other test in this suite deletes all its dots at the end.

	t.Run("DefaultDotSwitch", func(t *testing.T) {
		fsname := citools.UniqName()

		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/HELLO")

		// This would fail if we didn't pick up a default dot when there's only one
		citools.RunOnNode(t, node1, "dm commit -m 'Commit without selecting a dot first'")

		// Clean up
		checkTestContainerExits(t, node1)
		citools.RunOnNode(t, node1, "dm dot delete -f "+fsname)
	})

	// Embarrassingly, the combination of the above and below tests
	// together test that Configuration.DeleteStateForVolume correctly
	// cleans up the dot name from DefaultDotSwitch sothat
	// NoDefaultDotError runs successfully (otherwise, volume_1) is
	// still selected so we have a default, even if it doesn't exist.

	// We *could* write an explicit test for this case, but it's
	// probably not worth it as it *is* tested here.

	// But only due to the interaction between two tests. So I'm
	// documenting this nastiness.

	t.Run("NoDefaultDotError", func(t *testing.T) {
		fsname1 := citools.UniqName()
		fsname2 := citools.UniqName()

		citools.RunOnNode(t, node1, citools.DockerRun(fsname1)+" touch /foo/HELLO")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname2)+" touch /foo/HELLO")

		// This should fail if we didn't pick up a default dot when there's only one
		st := citools.OutputFromRunOnNode(t, node1, "if dm commit -m 'Commit without selecting a dot first'; then false; else true; fi")

		if !strings.Contains(st, "No current dot is selected") {
			t.Error(fmt.Sprintf("We didn't get an error when a default dot couldn't be found: %+v", st))
		}

		// Clean up
		checkTestContainerExits(t, node1)
		citools.RunOnNode(t, node1, "dm dot delete -f "+fsname1)
		citools.RunOnNode(t, node1, "dm dot delete -f "+fsname2)
	})
}

func TestRecoverFromUnmountedDotOnMaster(t *testing.T) {
	// single node tests
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewCluster(1)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	node1 := f[0].GetNode(0).Container

	assertMountState := func(t *testing.T, fsId string, desiredMountState bool) string {
		var mountpoint string
		err := citools.TryUntilSucceeds(func() error {
			st := citools.OutputFromRunOnNode(t, node1, "docker exec -t dotmesh-server-inner mount")
			if desiredMountState == true {
				if !strings.Contains(st, fsId) {
					return fmt.Errorf("%s not mounted", fsId)
				} else {
					fmt.Printf("%s is mounted!!! yay\n", fsId)
					for _, line := range strings.Split(st, "\n") {
						if strings.Contains(line, fsId) {
							shrapnel := strings.Split(line, " ")
							mountpoint = shrapnel[2]
						}
					}
				}
			} else {
				if strings.Contains(st, fsId) {
					return fmt.Errorf("%s mounted", fsId)
				} else {
					fmt.Printf("%s is not mounted!!! yay\n", fsId)
				}
			}
			return nil
		}, fmt.Sprintf("checking for %s to be mounted", fsId))
		if err != nil {
			t.Error(err)
		}
		return mountpoint
	}

	t.Run("FilesystemRemountedOnRestart", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}

		fsId := strings.TrimSpace(
			citools.OutputFromRunOnNode(t, node1, "dm dot show -H | grep masterBranchId | cut -f 2"),
		)
		//zfsPath := strings.Replace(node1, "cluster", "testpool", -1) + "/dmfs/" + fsId

		// wait for filesystem to be mounted (assert that it becomes mounted)
		mountpoint := assertMountState(t, fsId, true)

		// unmount the filesystem and assert that it's no longer mounted
		citools.RunOnNode(t, node1,
			"docker exec -t dotmesh-server-inner umount "+mountpoint,
		)
		assertMountState(t, fsId, false)

		// restart dotmesh, and wait for it to come back
		stopContainers(t, node1)
		startContainers(t, node1)

		// assert that dotmesh has re-mounted the filesystem
		assertMountState(t, fsId, true)

	})

	t.Run("RecoverFromUnmountedDotOnMaster", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}

		fsId := strings.TrimSpace(
			citools.OutputFromRunOnNode(t, node1, "dm dot show -H | grep masterBranchId | cut -f 2"),
		)
		//zfsPath := strings.Replace(node1, "cluster", "testpool", -1) + "/dmfs/" + fsId

		// wait for filesystem to be mounted (assert that it becomes mounted)
		mountpoint := assertMountState(t, fsId, true)

		// unmount the filesystem and assert that it's no longer mounted
		citools.RunOnNode(t, node1,
			"docker exec -t dotmesh-server-inner umount "+mountpoint,
		)
		assertMountState(t, fsId, false)

		// TODO: inject a fault into dotmesh which causes it to go back into
		// discoveringState for this fsMachine and get wedged in inactiveState
		// even though mastersCache says it should be active. send it a 'move'
		// request, and observe that rather than going into an infinite loop,
		// it self-corrects, checks mastersCache and goes back into active (and
		// remounts the filesystem)
		_, err := citools.DoSetDebugFlag(
			f[0].GetNode(0).IP,
			"admin",
			f[0].GetNode(0).ApiKey,
			"ForceStateMachineToDiscovering",
			fsId,
		)
		if err != nil {
			t.Error(err)
		}

		// assert that dotmesh has re-mounted the filesystem
		assertMountState(t, fsId, true)

	})
}

func TestSingleNode(t *testing.T) {
	// single node tests
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewCluster(1)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	node1 := f[0].GetNode(0).Container

	// Sub-tests, to reuse common setup code.
	t.Run("Init", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}
	})

	t.Run("InitDuplicate", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+fsname)

		resp := citools.OutputFromRunOnNode(t, node1, "if dm init "+fsname+"; then false; else true; fi ")
		if !strings.Contains(resp, fmt.Sprintf("Error: %s exists already", fsname)) {
			t.Error("Didn't get an error when attempting to re-create the volume")
		}

		resp = citools.OutputFromRunOnNode(t, node1, "dm list")

		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}
	})

	t.Run("InitCrashSafety", func(t *testing.T) {
		fsname := citools.UniqName()

		_, err := citools.DoSetDebugFlag(f[0].GetNode(0).IP, "admin", f[0].GetNode(0).ApiKey, "PartialFailCreateFilesystem", "true")
		if err != nil {
			t.Error(err)
		}

		resp := citools.OutputFromRunOnNode(t, node1, "if dm init "+fsname+"; then false; else true; fi ")

		if !strings.Contains(resp, "Injected fault") {
			t.Error("Couldn't inject fault into CreateFilesystem")
		}

		_, err = citools.DoSetDebugFlag(f[0].GetNode(0).IP, "admin", f[0].GetNode(0).ApiKey, "PartialFailCreateFilesystem", "false")
		if err != nil {
			t.Error(err)
		}

		// Now try again, and check it recovers and creates the volume
		citools.RunOnNode(t, node1, "dm init "+fsname)

		resp = citools.OutputFromRunOnNode(t, node1, "dm list")

		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}
	})

	t.Run("Version", func(t *testing.T) {
		var parsedResponse []string
		var validVersion = regexp.MustCompile(`[A-Za-z-.0-9]+`)
		var validRemote = regexp.MustCompile(`^Current remote: `)

		serverResponse := citools.OutputFromRunOnNode(t, node1, "dm version")
		fmt.Sprintf("Server response: %s\n", serverResponse)

		lines := strings.Split(serverResponse, "\n")

		remoteInfo := lines[0]
		versionInfo := lines[1:]

		if !validRemote.MatchString(remoteInfo) {
			t.Errorf("unable to find current remote in version string: %v", remoteInfo)
		}

		for _, versionBit := range versionInfo {
			parsedResponse = append(parsedResponse, strings.Fields(strings.TrimSpace(versionBit))...)
		}

		if len(parsedResponse) != 6 {
			t.Fatalf("dm version response has the wrong number of lines in versionInfo (found %d):\n%s\n%#v\n", len(parsedResponse), serverResponse, parsedResponse)
		}

		if (parsedResponse[0] != "Client:") || parsedResponse[1] != "Version:" {
			t.Errorf("unable to find all parts of Client version in ouput: %v %v", parsedResponse[0], parsedResponse[1])
		}
		if !validVersion.MatchString(parsedResponse[2]) {
			t.Errorf("unable to find all client version params in ouput: %v", serverResponse)
		}
		if parsedResponse[3] != "Server:" || parsedResponse[4] != "Version:" {
			t.Errorf("unable to find all version params in ouput: %v %v", parsedResponse[3], parsedResponse[4])
		}

		if !validVersion.MatchString(parsedResponse[5]) {
			t.Errorf("unable to find valid server version in ouput: %v", parsedResponse[5])
		}

		if citools.Contains(versionInfo, "uninitialized") {
			t.Errorf("Version was uninitialized: %v", versionInfo)
		}
	})

	t.Run("Commit", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'hello'")
		resp := citools.OutputFromRunOnNode(t, node1, "dm log")
		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message in log output")
		}
	})

	t.Run("Branch", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'hello'")
		citools.RunOnNode(t, node1, "dm checkout -b branch1")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/Y")
		citools.RunOnNode(t, node1, "dm commit -m 'there'")
		resp := citools.OutputFromRunOnNode(t, node1, "dm log")
		if !strings.Contains(resp, "there") {
			t.Error("unable to find commit message in log output")
		}
		citools.RunOnNode(t, node1, "dm checkout master")
		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/")
		if strings.Contains(resp, "Y") {
			t.Error("failed to switch filesystem")
		}
		citools.RunOnNode(t, node1, "dm checkout branch1")
		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/")
		if !strings.Contains(resp, "Y") {
			t.Error("failed to switch filesystem")
		}
	})

	t.Run("DotShow", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'hello'")
		citools.RunOnNode(t, node1, "dm checkout -b branch1")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/Y")
		citools.RunOnNode(t, node1, "dm commit -m 'there'")
		resp := citools.OutputFromRunOnNode(t, node1, "dm dot show")
		if !strings.Contains(resp, "master") {
			t.Error("failed to show master status")
		}
		if !strings.Contains(resp, "branch1") {
			t.Error("failed to show branch status")
		}
		re, _ := regexp.Compile(`server.*up to date`)
		matches := re.FindAllStringSubmatch(resp, -1)
		if len(matches) < 2 {
			t.Errorf("Unrecognisable result from `dm dot show`: %s, regexp matches: %#v", resp, matches)
		} else {
			masterReplicationStatus := matches[0][0]
			branchReplicationStatus := matches[1][0]
			if masterReplicationStatus == branchReplicationStatus {
				t.Error("master and branch replication statusse are suspiciously similar")
			}
		}

	})

	t.Run("Reset", func(t *testing.T) {
		fsname := citools.UniqName()
		// Run a container in the background so that we can observe it get
		// restarted.
		citools.RunOnNode(t, node1,
			citools.DockerRun(fsname, "-d --name sleeper")+" sleep 100",
		)
		initialStart := citools.OutputFromRunOnNode(t, node1,
			"docker inspect sleeper |jq .[0].State.StartedAt",
		)

		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'hello'")
		resp := citools.OutputFromRunOnNode(t, node1, "dm log")
		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message in log output")
		}
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/Y")
		citools.RunOnNode(t, node1, "dm commit -m 'again'")
		resp = citools.OutputFromRunOnNode(t, node1, "dm log")
		if !strings.Contains(resp, "again") {
			t.Error("unable to find commit message in log output")
		}
		citools.RunOnNode(t, node1, "dm reset --hard HEAD^")
		resp = citools.OutputFromRunOnNode(t, node1, "dm log")
		if strings.Contains(resp, "again") {
			t.Error("found 'again' in dm log when i shouldn't have")
		}
		// check filesystem got rolled back
		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/")
		if strings.Contains(resp, "Y") {
			t.Error("failed to roll back filesystem")
		}
		newStart := citools.OutputFromRunOnNode(t, node1,
			"docker inspect sleeper |jq .[0].State.StartedAt",
		)
		if initialStart == newStart {
			t.Errorf("container was not restarted during rollback (initialStart %v == newStart %v)", strings.TrimSpace(initialStart), strings.TrimSpace(newStart))
		}

	})

	t.Run("RunningContainersListed", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname, "-d --name tester")+" sleep 100")
		err := citools.TryUntilSucceeds(func() error {
			resp := citools.OutputFromRunOnNode(t, node1, "dm list")
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
		resp := citools.OutputFromRunOnNode(t, node1, "dm debug AllDotsAndBranches")
		fmt.Printf("AllDotsAndBranches response: %v\n", resp)
	})

	// Exercise the import functionality which should already exists in Docker
	t.Run("ImportDockerImage", func(t *testing.T) {
		fsname := citools.UniqName()
		resp := citools.OutputFromRunOnNode(t, node1,
			// Mount the volume at /etc in the container. Docker should copy the
			// contents of /etc in the image over the top of the new blank volume.
			citools.DockerRun(fsname, "--name import-test", "busybox", "/etc")+" cat /etc/passwd",
		)
		// "root" normally shows up in /etc/passwd
		if !strings.Contains(resp, "root") {
			t.Error("unable to find 'root' in expected output")
		}
		// If we reuse the volume, we should find the contents of /etc
		// imprinted therein.
		resp = citools.OutputFromRunOnNode(t, node1,
			citools.DockerRun(fsname, "--name import-test-2", "busybox", "/foo")+" cat /foo/passwd",
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
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/HELLO")
		st := citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/HELLO")
		if !strings.Contains(st, "HELLO") {
			t.Errorf("Data did not persist between two instanciations of the same volume on the same host: %v", st)
		}
	})

	t.Run("BranchPinning", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/HELLO-ORIGINAL")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m original")

		citools.RunOnNode(t, node1, "dm checkout -b branch1")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/HELLO-BRANCH1")
		citools.RunOnNode(t, node1, "dm commit -m branch1commit1")

		citools.RunOnNode(t, node1, "dm checkout master")
		citools.RunOnNode(t, node1, "dm checkout -b branch2")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/HELLO-BRANCH2")
		citools.RunOnNode(t, node1, "dm commit -m branch2commit1")

		citools.RunOnNode(t, node1, "dm checkout master")
		citools.RunOnNode(t, node1, "dm checkout -b branch3")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/HELLO-BRANCH3")
		citools.RunOnNode(t, node1, "dm commit -m branch3commit1")

		st := citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname+"@branch1")+" ls /foo")
		if st != "HELLO-BRANCH1\nHELLO-ORIGINAL\n" {
			t.Errorf("Wrong content in branch 1: '%s'", st)
		}

		st = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname+"@branch2")+" ls /foo")
		if st != "HELLO-BRANCH2\nHELLO-ORIGINAL\n" {
			t.Errorf("Wrong content in branch 2: '%s'", st)
		}

		st = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname+"@branch3")+" ls /foo")
		if st != "HELLO-BRANCH3\nHELLO-ORIGINAL\n" {
			t.Errorf("Wrong content in branch 3: '%s'", st)
		}
	})

	t.Run("Subdots", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname+".frogs")+" touch /foo/HELLO-FROGS")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname+".eat")+" touch /foo/HELLO-EAT")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname+".flies")+" touch /foo/HELLO-FLIES")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname+".__root__")+" touch /foo/HELLO-ROOT")
		st := citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname+".__root__")+" find /foo -type f | sort")
		if st != "/foo/HELLO-ROOT\n/foo/eat/HELLO-EAT\n/foo/flies/HELLO-FLIES\n/foo/frogs/HELLO-FROGS\n" {
			t.Errorf("Subdots didn't work out: %s", st)
		}
	})

	t.Run("DefaultSubdot", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/HELLO-DEFAULT")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname+".__root__")+" touch /foo/HELLO-ROOT")
		st := citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname+".__root__")+" find /foo -type f | sort")
		if st != "/foo/HELLO-ROOT\n/foo/__default__/HELLO-DEFAULT\n" {
			t.Errorf("Subdots didn't work out: %s", st)
		}
	})

	t.Run("ConcurrentSubdots", func(t *testing.T) {
		fsname := citools.UniqName()

		citools.RunOnNode(t, node1, citools.DockerRunDetached(fsname+".frogs")+" sh -c 'touch /foo/HELLO-FROGS; sleep 30'")
		citools.RunOnNode(t, node1, citools.DockerRunDetached(fsname+".eat")+" sh -c 'touch /foo/HELLO-EAT; sleep 30'")
		citools.RunOnNode(t, node1, citools.DockerRunDetached(fsname+".flies")+" sh -c 'touch /foo/HELLO-FLIES; sleep 30'")
		citools.RunOnNode(t, node1, citools.DockerRunDetached(fsname+".__root__")+" sh -c 'touch /foo/HELLO-ROOT; sleep 30'")
		// Let everything get started
		time.Sleep(5)

		st := citools.OutputFromRunOnNode(t, node1, "dm list")
		matched, err := regexp.MatchString("/[a-z]+_[a-z]+,/[a-z]+_[a-z]+,/[a-z]+_[a-z]+,/[a-z]+_[a-z]+", st)
		if err != nil {
			t.Error(err)
		}
		if !matched {
			t.Errorf("Couldn't find four containers attached to the dot: %+v", st)
		}

		// Check combined state
		st = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname+".__root__")+" find /foo -type f | sort")
		if st != "/foo/HELLO-ROOT\n/foo/eat/HELLO-EAT\n/foo/flies/HELLO-FLIES\n/foo/frogs/HELLO-FROGS\n" {
			t.Errorf("Subdots didn't work out: %s", st)
		}

		// Check commits and branches work
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m pod-commit")
		citools.RunOnNode(t, node1, "dm checkout -b branch") // Restarts the containers
		citools.RunOnNode(t, node1, citools.DockerRun(fsname+"@branch.again")+" touch /foo/HELLO-AGAIN")
		citools.RunOnNode(t, node1, "dm commit -m branch-commit")

		// Check branch state
		st = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname+"@branch.__root__")+" find /foo -type f | sort")

		if st != "/foo/HELLO-ROOT\n/foo/again/HELLO-AGAIN\n/foo/eat/HELLO-EAT\n/foo/flies/HELLO-FLIES\n/foo/frogs/HELLO-FROGS\n" {
			t.Errorf("Subdots didn't work out on branch: %s", st)
		}

		// Check master state
		citools.RunOnNode(t, node1, "dm checkout master") // Restarts the containers
		st = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname+".__root__")+" find /foo -type f | sort")

		if st != "/foo/HELLO-ROOT\n/foo/eat/HELLO-EAT\n/foo/flies/HELLO-FLIES\n/foo/frogs/HELLO-FROGS\n" {
			t.Errorf("Subdots didn't work out back on master: %s", st)
		}

		// Check containers all got restarted
		st = citools.OutputFromRunOnNode(t, node1, "docker ps | grep 'touch /foo' | wc -l")
		if st != "4\n" {
			t.Errorf("Subdot containers didn't get restarted")
			citools.RunOnNode(t, node1, "docker ps")
		}
	})

	t.Run("SubdotSwitch", func(t *testing.T) {
		fsname := citools.UniqName()

		citools.RunOnNode(t, node1, "dm init "+fsname)
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'initial empty commit'")

		// Set up branch A
		citools.RunOnNode(t, node1, "dm checkout -b branch_A")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname+"@branch_A.frogs")+" sh -c 'echo A_FROGS > /foo/HELLO'")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname+"@branch_A")+" sh -c 'echo A_DEFAULT > /foo/HELLO'")
		citools.RunOnNode(t, node1, "dm commit -m 'branch A commit'")
		time.Sleep(5)

		// Set up branch B
		citools.RunOnNode(t, node1, "dm checkout master")
		citools.RunOnNode(t, node1, "dm checkout -b branch_B")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname+"@branch_B.frogs")+" sh -c 'echo B_FROGS > /foo/HELLO'")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname+"@branch_B")+" sh -c 'echo B_DEFAULT > /foo/HELLO'")
		citools.RunOnNode(t, node1, "dm commit -m 'branch B commit'")

		// Switch back to master
		citools.RunOnNode(t, node1, "dm checkout master")

		// Start up a long sleep container on each, that we can docker exec into
		frogsContainer := fsname + "_frogs"
		defaultContainer := fsname + "_default"
		citools.RunOnNode(t, node1, citools.DockerRunDetached(fsname+".frogs", "--name "+frogsContainer)+" sh -c 'sleep 30'")
		citools.RunOnNode(t, node1, citools.DockerRunDetached(fsname, "--name "+defaultContainer)+" sh -c 'sleep 30'")

		// Check returns branch B content
		citools.RunOnNode(t, node1, "dm checkout branch_B") // Restarts container
		st := citools.OutputFromRunOnNode(t, node1, "docker exec "+frogsContainer+" cat /foo/HELLO")
		if st != "B_FROGS\n" {
			t.Errorf("Expected B_FROGS, got %+v", st)
		}
		st = citools.OutputFromRunOnNode(t, node1, "docker exec "+defaultContainer+" cat /foo/HELLO")

		if st != "B_DEFAULT\n" {
			t.Errorf("Expected B_DEFAULT, got %+v", st)
		}

		// Check returns branch A content
		citools.RunOnNode(t, node1, "dm checkout branch_A") // Restarts container
		st = citools.OutputFromRunOnNode(t, node1, "docker exec "+frogsContainer+" cat /foo/HELLO")
		if st != "A_FROGS\n" {
			t.Errorf("Expected A_FROGS, got %+v", st)
		}
		st = citools.OutputFromRunOnNode(t, node1, "docker exec "+defaultContainer+" cat /foo/HELLO")
		if st != "A_DEFAULT\n" {
			t.Errorf("Expected A_DEFAULT, got %+v", st)
		}
	})

	t.Run("InstrumentationMiddleware", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/whatever")
		metrics := citools.OutputFromRunOnNode(t, node1, "docker exec -t dotmesh-server-inner curl localhost:32607/metrics")
		if !strings.Contains(metrics, "dm_req_total") {
			t.Error("unable to find data on total request counter on /metrics")
		}
		if !strings.Contains(metrics, "dm_req_duration_seconds") {
			t.Error("unable to find data on duration of requests on /metrics")
		}
		if !strings.Contains(metrics, "dm_state_transition_total") {
			t.Error("unable to find data on duration of state transitinos on /metrics")
		}
		if !strings.Contains(metrics, "dm_zpool_usage_percentage") {
			t.Error("unable to find data on zpool capacity used on /metrics")
		}
		if !strings.Contains(metrics, "dm_rpc_req_duration_seconds") {
			t.Error("unable to find data on rpc request duration on /metrics")
		}
	})

	t.Run("MountExistingDot", func(t *testing.T) {
		fsname := citools.UniqName()

		// make a nice fresh dot with a commit
		citools.RunOnNode(t, node1, "dm init "+fsname)
		citools.RunOnNode(t, node1, "dm switch "+fsname)

		citools.RunOnNode(t, node1, "dm commit -m 'initial empty commit'")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo hello > /foo/file.txt'")
		citools.RunOnNode(t, node1, "dm commit -m 'commit with some data'")

		// mount the dot on a local path
		citools.RunOnNode(t, node1, "dm mount "+fsname+" /tmp/mounted-"+fsname)

		// check we can see the dot data from the local path
		st := citools.OutputFromRunOnNode(t, node1, "cat /tmp/mounted-"+fsname+"/file.txt")
		if st != "hello\n" {
			t.Errorf("Expected hello, got %+v", st)
		}
	})

	t.Run("MountNewDot", func(t *testing.T) {
		fsname := citools.UniqName()

		// mount a created dot on a local path
		citools.RunOnNode(t, node1, "dm mount --create "+fsname+" /tmp/mounted-"+fsname)

		// write data to the local dot path
		citools.RunOnNode(t, node1, "sh -c 'echo hello > /tmp/mounted-"+fsname+"/file.txt'")

		// check we can read the data from a container dot
		st := citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'cat /foo/file.txt'")
		if st != "hello\n" {
			t.Errorf("Expected hello, got %+v", st)
		}
	})

	t.Run("MountNewDotFailure", func(t *testing.T) {
		fsname := citools.UniqName()

		// fail to make a dot because no --create flag
		st := citools.OutputFromRunOnNode(t, node1, "if dm mount "+fsname+" /tmp/mounted-"+fsname+"; then false; else true; fi")

		if !strings.Contains(st, fmt.Sprintf("Dot %s does not exist.", fsname)) {
			t.Error(fmt.Sprintf("We didn't get an error when the --create flag was not used: %+v", st))
		}
	})

	t.Run("MountCommit", func(t *testing.T) {
		apiKey := f[0].GetNode(0).ApiKey
		fsname := citools.UniqName()

		// make a dot with a second commit that has a file we can test the mount for
		citools.RunOnNode(t, node1, "dm init "+fsname)
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'initial empty commit'")
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo APPLES > /foo/HELLO'")
		citools.RunOnNode(t, node1, "dm commit -m 'second commit with some data'")

		// get a list of commits so we have the id of the commit we will mount
		var commitIds []struct {
			Id string
		}

		err = citools.DoRPC(f[0].GetNode(0).IP, "admin", apiKey,
			"DotmeshRPC.Commits",
			struct {
				Namespace string
				Name      string
			}{
				Namespace: "admin",
				Name:      fsname,
			},
			&commitIds)

		if err != nil {
			t.Error(err)
		}

		if len(commitIds) != 2 {
			t.Errorf("Expected 2 commit ids, got %d", len(commitIds))
		}

		commitToMountId := commitIds[1].Id

		// get the id of the dot
		var filesystemId string

		err = citools.DoRPC(f[0].GetNode(0).IP, "admin", apiKey,
			"DotmeshRPC.Lookup",
			struct {
				Namespace string
				Name      string
			}{
				Namespace: "admin",
				Name:      fsname,
			},
			&filesystemId)

		if err != nil {
			t.Error(err)
		}

		// mount the commit and get the mounted path
		var mountPath string

		err = citools.DoRPC(f[0].GetNode(0).IP, "admin", apiKey,
			"DotmeshRPC.MountCommit",
			struct {
				FilesystemId string
				CommitId     string
			}{
				FilesystemId: filesystemId,
				CommitId:     commitToMountId,
			},
			&mountPath)

		// check the contents of the file on the mounted path
		st := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("cat %s/__default__/HELLO", mountPath))

		if st != "APPLES\n" {
			t.Errorf("Expected APPLES, got %+v", st)
		}
	})

	t.Run("CommitMetadata", func(t *testing.T) {
		fsname := citools.UniqName()

		citools.RunOnNode(t, node1, "dm init "+fsname)
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m \"commit message\" --metadata apples=green")
		st := citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(st, "apples: green") {
			t.Error(fmt.Sprintf("We didn't get the metadata back from dm log: %+v", st))
		}
	})

	t.Run("CommitMetadataUppercaseFailure", func(t *testing.T) {
		fsname := citools.UniqName()

		citools.RunOnNode(t, node1, "dm init "+fsname)
		citools.RunOnNode(t, node1, "dm switch "+fsname)

		// fail to commit a dot because we used an uppercase metadata fieldname
		st := citools.OutputFromRunOnNode(t, node1, "if dm commit -m \"commit message\" --metadata Apples=green; then false; else true; fi")

		if !strings.Contains(st, fmt.Sprintf("Metadata field names must start with lowercase characters: Apples")) {
			t.Error(fmt.Sprintf("We didn't get an error when we used an uppercase metadata fieldname: %+v", st))
		}
	})

	t.Run("CommitMetadataNoEqualsFailure", func(t *testing.T) {
		fsname := citools.UniqName()

		citools.RunOnNode(t, node1, "dm init "+fsname)
		citools.RunOnNode(t, node1, "dm switch "+fsname)

		// fail to commit a dot because we used an uppercase metadata fieldname
		st := citools.OutputFromRunOnNode(t, node1, "if dm commit -m \"commit message\" --metadata Applesgreen; then false; else true; fi")

		if !strings.Contains(st, fmt.Sprintf("Each metadata value must be a name=value pair: Applesgreen")) {
			t.Error(fmt.Sprintf("We didn't get an error when we didn't use an equals sign in the metadata string: %+v", st))
		}
	})

	t.Run("InvalidRequest", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}

		fsId := strings.TrimSpace(
			citools.OutputFromRunOnNode(t, node1, "dm dot show -H | grep masterBranchId | cut -f 2"),
		)

		// Inject an invalid request
		resp, err := citools.DoSetDebugFlag(
			f[0].GetNode(0).IP,
			"admin",
			f[0].GetNode(0).ApiKey,
			"SendMangledEvent",
			fsId,
		)
		if err != nil {
			t.Error(err)
		}

		if !strings.Contains(resp, "invalid-request") {
			t.Errorf("Response didn't contained 'invalid-request', should be something like '{\"Name\":\"invalid-request\",\"Args\":{\"error\":{\"Offset\":1},\"request\":null}}' but was: %s", resp)
		}

		// Check filesystem still works to some extent
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m \"Jabberwocky\"")
		st := citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(st, "Jabberwocky") {
			t.Error(fmt.Sprintf("We didn't get the commit back from dm log: %+v", st))
		}

	})

	t.Run("ApiKeys", func(t *testing.T) {
		apiKey := f[0].GetNode(0).ApiKey
		password := f[0].GetNode(0).Password

		var resp struct {
			ApiKey string
		}

		err := citools.DoRPC(f[0].GetNode(0).IP, "admin", apiKey,
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

		err = citools.DoRPC(f[0].GetNode(0).IP, "admin", apiKey,
			"DotmeshRPC.ResetApiKey",
			struct {
			}{},
			&resp)
		if err == nil {
			t.Errorf("Was able to reset API key without a password")
		}

		err = citools.DoRPC(f[0].GetNode(0).IP, "admin", password,
			"DotmeshRPC.ResetApiKey",
			struct {
			}{},
			&resp)
		if err != nil {
			t.Error(err)
		}
		if resp.ApiKey == apiKey {
			t.Errorf("Got API key %v, expected a new one (got %v)!", resp.ApiKey, apiKey)
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
		err = citools.DoRPC(f[0].GetNode(0).IP, "admin", apiKey,
			"DotmeshRPC.CurrentUser",
			struct {
			}{},
			&resp)
		if err == nil {
			t.Errorf("Successfully used old API key")
		}

		fmt.Printf("About to expect success...\n")
		// Use new API key, expect success
		err = citools.DoRPC(f[0].GetNode(0).IP, "admin", resp.ApiKey,
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
		// later tests don't fail! We can do a "citools.RunOnNode(t, node1, sed -i
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

	st := citools.OutputFromRunOnNode(t, node1, "dm list")
	if strings.Contains(st, fsname) {
		t.Error(fmt.Sprintf("The volume is still in 'dm list' on node1 (after %d seconds)", delay/time.Second))
		return
	}

	st = citools.OutputFromRunOnNode(t, node2, "dm list")
	if strings.Contains(st, fsname) {
		t.Error(fmt.Sprintf("The volume is still in 'dm list' on node2 (after %d seconds)", delay/time.Second))
		return
	}

	st = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" cat /foo/HELLO || true")
	if strings.Contains(st, "WORLD") {
		t.Error(fmt.Sprintf("The volume name wasn't reusable %d seconds after delete on node 1...", delay/time.Second))
		return
	}

	st = citools.OutputFromRunOnNode(t, node2, citools.DockerRun(fsname)+" cat /foo/HELLO || true")
	if strings.Contains(st, "WORLD") {
		t.Error(fmt.Sprintf("The volume name wasn't reusable %d seconds after delete on node 2...", delay/time.Second))
		return
	}

	st = citools.OutputFromRunOnNode(t, node1, "dm list")
	if !strings.Contains(st, fsname) {
		t.Error(fmt.Sprintf("The re-use of the deleted volume name failed in 'dm list' on node1 (after %d seconds)", delay/time.Second))
		return
	}

	st = citools.OutputFromRunOnNode(t, node2, "dm list")
	if !strings.Contains(st, fsname) {
		t.Error(fmt.Sprintf("The re-use of the deleted volume name failed in 'dm list' on node2 (after %d seconds)", delay/time.Second))
		return
	}
}

func TestDeletionSimple(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	clusterEnv := make(map[string]string)
	clusterEnv["FILESYSTEM_METADATA_TIMEOUT"] = "5"

	// Our cluster gets a metadata timeout of 5s
	f := citools.Federation{citools.NewClusterWithEnv(2, clusterEnv)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	citools.LogTiming("setup")

	node1 := f[0].GetNode(0).Container
	node2 := f[0].GetNode(1).Container

	t.Run("DeleteNonexistantFails", func(t *testing.T) {
		fsname := citools.UniqName()

		st := citools.OutputFromRunOnNode(t, node1, "if dm dot delete -f "+fsname+"; then false; else true; fi")

		if !strings.Contains(st, "No such filesystem") {
			t.Error(fmt.Sprintf("Deleting a nonexistant volume didn't fail"))
		}
	})

	t.Run("DeleteInUseFails", func(t *testing.T) {
		fsname := citools.UniqName()

		ctx, cancel := context.WithCancel(context.Background())
		defer func() {
			cancel()
			testContainer := citools.OutputFromRunOnNode(t, node1, "docker ps | grep busybox | cut -d ' ' -f 1")
			citools.RunOnNode(t, node1, fmt.Sprintf("docker kill %s", testContainer))
		}()
		go func() {
			citools.RunOnNodeContext(ctx, t, node1, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO; tail -f /dev/null'")
		}()

		for !strings.Contains(citools.OutputFromRunOnNode(t, node1, "docker ps"), "busybox") {
			time.Sleep(1)
		}

		// Delete, while the container is running! Which should fail!
		st := citools.OutputFromRunOnNode(t, node1, "if dm dot delete -f "+fsname+"; then false; else true; fi")
		if !strings.Contains(st, "cannot delete the volume") {
			t.Error(fmt.Sprintf("The presence of a running container failed to suppress volume deletion"))
		}

	})

	t.Run("DeleteInstantly", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")
		checkTestContainerExits(t, node1)
		citools.RunOnNode(t, node1, "dm dot delete -f "+fsname)

		st := citools.OutputFromRunOnNode(t, node1, "dm list")
		if strings.Contains(st, fsname) {
			t.Error(fmt.Sprintf("The volume is still in 'dm list' on node1 (immediately after deletion)"))
			return
		}

		st = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" cat /foo/HELLO || true")
		if strings.Contains(st, "WORLD") {
			t.Error(fmt.Sprintf("The volume name wasn't immediately reusable after deletion on node 1..."))
			return
		}

		/*
				         We don't try and guarantee immediate deletion on other nodes.
				         So the following may or may not fail, we can't test for it.

				   		st = citools.OutputFromRunOnNode(t, node2, citools.DockerRun(fsname)+" cat /foo/HELLO || true")

				   		if strings.Contains(st, "WORLD") {
				   			t.Error(fmt.Sprintf("The volume didn't get deleted on node 2..."))
			          		return
				   		}
		*/

	})

	t.Run("DeleteQuickly", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")
		checkTestContainerExits(t, node1)
		citools.RunOnNode(t, node1, "dm dot delete -f "+fsname)

		// Ensure the initial delete has happened, but the metadata is
		// still draining. This is less than half the metadata timeout
		// configured above; the system should be in the process of
		// cleaning up after the volume, but it should be fine to reuse
		// the name by now.

		checkDeletionWorked(t, fsname, 2*time.Second, node1, node2)
	})

	t.Run("DeleteSlowly", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")
		checkTestContainerExits(t, node1)
		citools.RunOnNode(t, node1, "dm dot delete -f "+fsname)

		// Ensure the delete has happened completely This is twice the
		// metadata timeout configured above, so all traces of the
		// volume should be gone and we get to see the result of the
		// "cleanupDeletedFilesystems" logic (has it ruined the
		// cluster?)

		checkDeletionWorked(t, fsname, 10*time.Second, node1, node2)
	})

	t.Run("DeleteQuicklyOnOtherNode", func(t *testing.T) {
		// Does deletion succeed if you attempt to initate it from another node
		// in the cluster?

		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")
		checkTestContainerExits(t, node1)
		citools.RunOnNode(t, node2, "dm dot delete -f "+fsname)

		// Ensure the initial delete has happened, but the metadata is
		// still draining. This is less than half the metadata timeout
		// configured above; the system should be in the process of
		// cleaning up after the volume, but it should be fine to reuse
		// the name by now.

		checkDeletionWorked(t, fsname, 2*time.Second, node1, node2)
	})

}

func checkTestContainerExits(t *testing.T, node string) {
	err := citools.TryUntilSucceeds(func() error {
		result := citools.OutputFromRunOnNode(t, node, "docker ps")
		if strings.Contains(result, "busybox") {
			return fmt.Errorf("container still active")
		}
		return nil
	}, "waiting for container to quit")
	if err != nil {
		t.Error(err)
	}
}

func waitForDirtyState(t *testing.T, node string, fsname string) {
	err := citools.TryUntilSucceeds(func() error {
		uncommitedBytes, err := strconv.Atoi(strings.TrimSpace(citools.OutputFromRunOnNode(t, node, "dm list -H |grep "+fsname+" |cut -f 7")))
		if err != nil {
			t.Error(err)
		}

		if uncommitedBytes == 0 {
			return fmt.Errorf("uncommited changes not detected on node, got uncommited bytes %v\n", uncommitedBytes)
		}
		return nil
	}, "waiting for dirty state to show up")

	if err != nil {
		t.Error(err)
	}
}

func setupBranchesForDeletion(t *testing.T, fsname string, node1 string, node2 string) {
	// Set up some branches:
	//
	// Master -> branch1 -> branch2
	//   |
	//   \-> branch3

	citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")
	citools.RunOnNode(t, node1, "dm switch "+fsname)
	citools.RunOnNode(t, node1, "dm commit -m 'On master'")

	citools.RunOnNode(t, node1, "dm checkout -b branch1")
	citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/GOODBYE'")
	citools.RunOnNode(t, node1, "dm commit -m 'On branch1'")

	citools.RunOnNode(t, node1, "dm checkout -b branch2")
	citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/GOODBYE_CRUEL'")
	citools.RunOnNode(t, node1, "dm commit -m 'On branch2'")

	citools.RunOnNode(t, node1, "dm checkout master")
	citools.RunOnNode(t, node1, "dm checkout -b branch3")
	citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO_CRUEL'")
	citools.RunOnNode(t, node1, "dm commit -m 'On branch3'")
}

func TestDeletionComplex(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	clusterEnv := make(map[string]string)
	clusterEnv["FILESYSTEM_METADATA_TIMEOUT"] = "5"

	// Our cluster gets a metadata timeout of 5s
	f := citools.Federation{citools.NewClusterWithEnv(2, clusterEnv)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	citools.LogTiming("setup")

	node1 := f[0].GetNode(0).Container
	node2 := f[0].GetNode(1).Container

	t.Run("DeleteBranchesQuickly", func(t *testing.T) {
		fsname := citools.UniqName()
		setupBranchesForDeletion(t, fsname, node1, node2)
		checkTestContainerExits(t, node1)
		checkTestContainerExits(t, node2)

		// Now kill the lot, right?
		citools.RunOnNode(t, node1, "dm dot delete -f "+fsname)

		// Test after two seconds, the state where the registry is cleared out but
		// the metadata remains.
		checkDeletionWorked(t, fsname, 2*time.Second, node1, node2)
	})

	t.Run("DeleteBranchesSlowly", func(t *testing.T) {

		fsname := citools.UniqName()
		setupBranchesForDeletion(t, fsname, node1, node2)
		checkTestContainerExits(t, node1)
		checkTestContainerExits(t, node2)

		// Now kill the lot, right?
		citools.RunOnNode(t, node1, "dm dot delete -f "+fsname)

		// Test after tens econds, when all the metadata should be cleared out.
		checkDeletionWorked(t, fsname, 10*time.Second, node1, node2)
	})
}

func TestTwoNodesSameCluster(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewCluster(2)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	citools.LogTiming("setup")

	node1 := f[0].GetNode(0).Container
	node2 := f[0].GetNode(1).Container

	t.Run("Move", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")
		st := citools.OutputFromRunOnNode(t, node2, citools.DockerRun(fsname)+" cat /foo/HELLO")

		if !strings.Contains(st, "WORLD") {
			t.Errorf("Unable to find world in transported data capsule, got '%s'", st)
		}
	})

	t.Run("SmashBranchMaster", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")

		// Get list of server IDs and master/replica status
		originalMaster := ""
		originalReplica := ""
		st1 := citools.OutputFromRunOnNode(t, node1, "dm dot show -H "+fsname+" | grep latency | cut -f 2,3")
		for _, line := range strings.Split(st1, "\n") {
			parts := strings.Split(line, "\t")
			if len(parts) == 2 {
				serverId := parts[0]
				role := parts[1]
				if role == "master" {
					originalMaster = serverId
				} else {
					originalReplica = serverId
				}
			}
		}

		// Hulk Smash!
		citools.RunOnNode(t, node1, "dm dot smash-branch-master "+fsname+" master "+originalReplica)

		// It's asynch, so wait
		time.Sleep(1 * time.Second)

		// Check result
		st2 := citools.OutputFromRunOnNode(t, node1, "dm dot show -H "+fsname+" | grep latency | cut -f 2,3")
		for _, line := range strings.Split(st2, "\n") {
			parts := strings.Split(line, "\t")
			if len(parts) == 2 {
				serverId := parts[0]
				role := parts[1]
				if role == "master" {
					if serverId != originalReplica {
						t.Errorf("Failed to unseat the current master: %s -> %s", st1, st2)
					}
				} else {
					if serverId != originalMaster {
						t.Errorf("Failed to promote current replica: %s -> %s", st1, st2)
					}
				}
			}
		}
	})

	t.Run("DumpInternalState", func(t *testing.T) {
		st := citools.OutputFromRunOnNode(t, node1, "dm debug DotmeshRPC.DumpInternalState")
		if len(st) < 10 {
			t.Errorf("Suspiciously short result from DumpInternalState: %s", st)
		}
	})

	if false {
		// This test is disabled until we fix https://github.com/dotmesh-io/dotmesh/issues/493 :-(
		t.Run("Divergence", func(t *testing.T) {
			fsname := citools.UniqName()
			citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO'")
			citools.RunOnNode(t, node1, "dm switch "+fsname)
			citools.RunOnNode(t, node1, "dm commit -m 'First commit'")
			ensureDotIsFullyReplicated(t, node1, fsname)
			ensureDotIsFullyReplicated(t, node2, fsname)
			time.Sleep(3 * time.Second)

			fsId := strings.TrimSpace(citools.OutputFromRunOnNode(t, node1, "dm dot show -H | grep masterBranchId | cut -f 2"))

			// Kill node1
			stopContainers(t, node1)

			// Commit on node2
			citools.RunOnNode(t, node2, "dm dot smash-branch-master "+fsname+" master")
			citools.RunOnNode(t, node2, citools.DockerRun(fsname)+" sh -c 'echo WORLD > /foo/HELLO2'")
			citools.RunOnNode(t, node2, "dm switch "+fsname)
			citools.RunOnNode(t, node2, "dm commit -m 'node2 commit'")
			citools.RunOnNode(t, node2, "dm dot show")

			// Kill node2
			stopContainers(t, node2)

			// Start node1
			startContainers(t, node1)

			// Commit on node1
			citools.RunOnNode(t, node1, "dm dot smash-branch-master "+fsname+" master")

			zfsPath := strings.Replace(node1, "cluster", "testpool", -1) + "/dmfs/" + fsId + "@Node1CommitHash"

			// Manual ZFS snapshot to circumvent etcd
			citools.RunOnNode(t, node1, "docker exec -t dotmesh-server-inner zfs snapshot "+zfsPath)

			stopContainers(t, node1)
			startContainers(t, node1)
			citools.RunOnNode(t, node1, "dm dot show")

			// Start node2 and enjoy the diverged state
			startContainers(t, node2)
			citools.RunOnNode(t, node1, "dm dot show")
			citools.RunOnNode(t, node2, "dm dot show")

			// Check status of convergence
			var try int64
			itWorkedInTheEnd := false
		retryLoop:
			for try = 1; try < 15; try++ {
				for _, node := range [...]string{node1, node2} {
					problemsFound := false

					dotStatus := citools.OutputFromRunOnNode(t, node, "dm dot show")

					// Check for fatal errors
					if strings.Contains(dotStatus, "status: failed") {
						t.Errorf("One or more state machines have failed: %s\n", dotStatus)
						break retryLoop
					}

					// Log and continue on things that might get better
					if !strings.Contains(dotStatus, "DIVERGED") || strings.Contains(dotStatus, "is missing") {
						fmt.Printf("Absence of Divergence branch or incomplete resolution on node: %s\n%s\n", node, dotStatus)
						problemsFound = true
					}
					dmLog := citools.OutputFromRunOnNode(t, node, "dm log")
					if !strings.Contains(dmLog, "First commit") || !strings.Contains(dmLog, "Node1CommitHash") {
						fmt.Printf("Absence of converged commits on branch master on node: %s\n%s", node, dmLog)
						problemsFound = true
					}

					dmBranch := citools.OutputFromRunOnNode(t, node, "dm branch | grep DIVERGED")
					citools.RunOnNode(
						t, node,
						fmt.Sprintf("dm checkout %s", strings.TrimSpace(strings.Replace(dmBranch, "*", "", -1))),
					)

					dmLog = citools.OutputFromRunOnNode(t, node, "dm log")
					if !strings.Contains(dmLog, "node2 commit") {
						fmt.Printf("Absence of non-master diverged commits on branch *DIVERGED on node: %s\n%s", node, dmLog)
						problemsFound = true
					}

					if problemsFound {
						fmt.Printf("Sleeping for %d seconds then trying again...\n", 2*try)
						time.Sleep(2 * time.Duration(try) * time.Second)
						continue retryLoop
					} else {
						// Everything's good!
						itWorkedInTheEnd = true
						break retryLoop
					}
				}
			}

			if !itWorkedInTheEnd {
				t.Errorf("After %d retries, we never got to a good diverged state :-(", try)
			}
		})
	}
}

func TestTwoDoubleNodeClusters(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{
		citools.NewCluster(2),
		citools.NewCluster(2),
	}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	// c = cluster; n = node
	c0n0 := f[0].GetNode(0).Container
	c0n1 := f[0].GetNode(1).Container
	c1n0 := f[1].GetNode(0).Container
	c1n1 := f[1].GetNode(1).Container

	t.Run("PushWrongNode", func(t *testing.T) {
		// put a branch on c0n0, with a commit.
		fsname := citools.UniqName()
		citools.RunOnNode(t, c0n0, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, c0n0, "dm switch "+fsname)
		citools.RunOnNode(t, c0n0, "dm commit -m 'hello'")

		// ask c0n1 to push it to c1n0.
		// (c1n0 becomes the master for the branch on c1; the pull should be initiated by the current master).
		citools.RunOnNode(t, c0n1, "dm push cluster_1_node_0")

		// put more commits on it, on c0n0.
		citools.RunOnNode(t, c0n0, citools.DockerRun(fsname)+" touch /foo/Y")
		citools.RunOnNode(t, c0n0, "dm switch "+fsname)
		citools.RunOnNode(t, c0n0, "dm commit -m 'world'")

		// try to push those commits to c1n1.
		citools.RunOnNode(t, c0n1, "dm push cluster_1_node_1")

		// the commits should show up on c1n0! (the push should have been proxied to the current master)
		resp := citools.OutputFromRunOnNode(t, c1n0, "dm log")
		if !strings.Contains(resp, "world") {
			t.Error("unable to find commit message remote's log output")
		}

		// and on the other node on the other cluster, for good measure.
		resp = citools.OutputFromRunOnNode(t, c1n1, "dm log")
		if !strings.Contains(resp, "world") {
			t.Error("unable to find commit message remote's log output")
		}

	})

	t.Run("PullWrongNode", func(t *testing.T) {
		// put a branch on c0n0.
		fsname := citools.UniqName()
		citools.RunOnNode(t, c0n0, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, c0n0, "dm switch "+fsname)
		citools.RunOnNode(t, c0n0, "dm commit -m 'hello'")

		_, err := citools.RunOnNodeErr(c1n0, "dm clone cluster_0_node_1 "+fsname)
		if err != nil {
			t.Errorf("Failed to clone fs %v from cluster_0_node_1, got %v", fsname, err)
		}

		// put more commits on it, on c0n0.
		citools.RunOnNode(t, c0n0, citools.DockerRun(fsname)+" touch /foo/Y")
		citools.RunOnNode(t, c0n0, "dm commit -m 'world'")

		// try to pull those commits from c1n1.
		_, err = citools.RunOnNodeErr(c1n1, "dm clone cluster_0_node_1 "+fsname)
		if err != nil {
			t.Errorf("Failed to clone fs %v from cluster_0_node_1, got %v", fsname, err)
		}

		// the commits should show up on c1n0! (the pull should have been initiated by the current master)
		resp := citools.OutputFromRunOnNode(t, c1n0, "dm log")
		if !strings.Contains(resp, "world") {
			t.Error("unable to find commit message remote's log output")
		}

		// and on the other node on the other cluster, for good measure.
		resp = citools.OutputFromRunOnNode(t, c1n1, "dm log")
		if !strings.Contains(resp, "world") {
			t.Error("unable to find commit message remote's log output")
		}
	})

	// TODO something with branches.
	// TODO arrange for push/pulls to happen while the branch was being migrated to another master

}

func TestTwoSingleNodeClusters(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{
		citools.NewCluster(1),              // cluster_0_node_0
		citools.NewClusterOnPort(32609, 1), // cluster_1_node_0
	}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	node1 := f[0].GetNode(0).Container
	node2 := f[1].GetNode(0).Container

	t.Run("SpecifyPort", func(t *testing.T) {
		citools.RunOnNode(t, node1, "echo "+f[1].GetNode(0).ApiKey+" | dm remote add funny_port_remote admin@"+f[1].GetNode(0).IP+":32609")
	})

	t.Run("PushCommitBranchExtantBase", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node2, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node2, "dm switch "+fsname)
		citools.RunOnNode(t, node2, "dm commit -m 'hello'")
		citools.RunOnNode(t, node2, "dm push cluster_0")

		citools.RunOnNode(t, node1, "dm switch "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message remote's log output")
		}
		// test incremental push
		citools.RunOnNode(t, node2, "dm commit -m 'again'")
		citools.RunOnNode(t, node2, "dm push cluster_0")

		resp = citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(resp, "again") {
			t.Error("unable to find commit message remote's log output")
		}
		// test pushing branch with extant base
		citools.RunOnNode(t, node2, "dm checkout -b newbranch")
		citools.RunOnNode(t, node2, "dm commit -m 'branchy'")
		citools.RunOnNode(t, node2, "dm push cluster_0")

		citools.RunOnNode(t, node1, "dm checkout newbranch")
		resp = citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(resp, "branchy") {
			t.Error("unable to find commit message remote's log output")
		}
	})
	t.Run("PushCommitBranchNoExtantBase", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node2, citools.DockerRun(fsname)+" touch /foo/X")
		// test pushing branch with no base on remote
		citools.RunOnNode(t, node2, "dm switch "+fsname)
		citools.RunOnNode(t, node2, "dm commit -m 'master'")
		citools.RunOnNode(t, node2, "dm checkout -b newbranch")
		citools.RunOnNode(t, node2, "dm commit -m 'branchy'")
		citools.RunOnNode(t, node2, "dm checkout -b newbranch2")
		citools.RunOnNode(t, node2, "dm commit -m 'branchy2'")
		citools.RunOnNode(t, node2, "dm checkout -b newbranch3")
		citools.RunOnNode(t, node2, "dm commit -m 'branchy3'")
		citools.RunOnNode(t, node2, "dm push cluster_0")

		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm checkout newbranch3")
		resp := citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(resp, "branchy3") {
			t.Error("unable to find commit message remote's log output")
		}
	})
	t.Run("DirtyDetected", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node2, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node2, "dm switch "+fsname)
		citools.RunOnNode(t, node2, "dm commit -m 'hello'")
		citools.RunOnNode(t, node2, "dm push cluster_0")

		citools.RunOnNode(t, node1, "dm switch "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message remote's log output")
		}
		// now dirty the filesystem on node1 w/1MB before it can be received into
		citools.RunOnNode(t, node1, citools.DockerRun(""+fsname+"")+" dd if=/dev/urandom of=/foo/Y bs=1024 count=1024")
		checkTestContainerExits(t, node1)
		waitForDirtyState(t, node1, fsname)

		// test incremental push
		citools.RunOnNode(t, node2, "dm commit -m 'again'")
		result := citools.OutputFromRunOnNode(t, node2, "dm push cluster_0 || true") // an error code is ok

		if !strings.Contains(result, "uncommitted") {
			t.Error("pushing didn't fail when there were known uncommited changes on the peer")
		}
	})
	t.Run("DirtyImmediate", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node2, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node2, "dm switch "+fsname)
		citools.RunOnNode(t, node2, "dm commit -m 'hello'")
		citools.RunOnNode(t, node2, "dm push cluster_0")

		citools.RunOnNode(t, node1, "dm switch "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(resp, "hello") {
			t.Errorf("unable to find commit message remote's log output, instead got %v\n", resp)
		}
		// now dirty the filesystem on node1 w/1MB before it can be received into
		citools.RunOnNode(t, node1, citools.DockerRun(""+fsname+"")+" dd if=/dev/urandom of=/foo/Y bs=1024 count=1024")
		checkTestContainerExits(t, node1)
		waitForDirtyState(t, node1, fsname)
		// test incremental push
		citools.RunOnNode(t, node2, "dm commit -m 'again'")
		result := citools.OutputFromRunOnNode(t, node2, "dm push cluster_0 || true") // an error code is ok
		// Pushing on dirty state can error out either at the zfs level or checks that dotmesh-server does in the RPC.
		// Asynchronous behaviour means we would have to check for either or wait until the dirty state poller to catch up.
		if !strings.Contains(result, "uncommitted changes on volume where data would be written") &&
			!strings.Contains(result, "has been modified\nsince most recent snapshot") {
			t.Errorf(
				"pushing didn't fail when there were known uncommited changes on the peer. Got unexpected response %v on push\n", result)
		}
	})
	t.Run("Diverged", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node2, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node2, "dm switch "+fsname)
		citools.RunOnNode(t, node2, "dm commit -m 'hello'")
		citools.RunOnNode(t, node2, "dm push cluster_0")

		citools.RunOnNode(t, node1, "dm switch "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message remote's log output")
		}
		// now make a commit that will diverge the filesystems
		citools.RunOnNode(t, node1, "dm commit -m 'node1 commit'")

		// test incremental push
		citools.RunOnNode(t, node2, "dm commit -m 'node2 commit'")
		result := citools.OutputFromRunOnNode(t, node2, "dm push cluster_0 || true") // an error code is ok

		if !strings.Contains(result, "diverged") && !strings.Contains(result, "hello") {
			t.Error(
				"pushing didn't fail when there was a divergence",
			)
		}
	})
	t.Run("PushStashDiverged", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node2, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node2, "dm switch "+fsname)
		citools.RunOnNode(t, node2, "dm commit -m 'hello'")
		citools.RunOnNode(t, node2, "dm push cluster_0")

		citools.RunOnNode(t, node1, "dm switch "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message remote's log output")
		}
		// now make a commit that will diverge the filesystems
		citools.RunOnNode(t, node1, "dm commit -m 'node1 commit'")

		// test incremental push
		citools.RunOnNode(t, node2, "dm commit -m 'node2 commit'")
		citools.RunOnNode(t, node2, "dm push --stash-on-divergence cluster_0") // an error code is ok

		output := citools.OutputFromRunOnNode(t, node1, "dm branch")
		if !strings.Contains(output, "master-DIVERGED-") {
			t.Error("Stashed push did not create divergent branch")
		}
	})
	t.Run("ResetAfterPushThenPushMySQL", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node2, citools.DockerRun(
			fsname, "-d -e MYSQL_ROOT_PASSWORD=secret", "mysql:5.7.17", "/var/lib/mysql",
		))
		time.Sleep(10 * time.Second)
		citools.RunOnNode(t, node2, "dm switch "+fsname)
		citools.RunOnNode(t, node2, "dm commit -m 'hello'")
		citools.RunOnNode(t, node2, "dm push cluster_0")

		citools.RunOnNode(t, node1, "dm switch "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(resp, "hello") {
			t.Error("unable to find commit message remote's log output")
		}
		// now make a commit that will diverge the filesystems
		citools.RunOnNode(t, node1, "dm commit -m 'node1 commit'")

		// test resetting a commit made on a pushed volume
		citools.RunOnNode(t, node2, "dm commit -m 'node2 commit'")
		citools.RunOnNode(t, node1, "dm reset --hard HEAD^")
		resp = citools.OutputFromRunOnNode(t, node1, "dm log")
		if strings.Contains(resp, "node1 commit") {
			t.Error("found 'node1 commit' in dm log when i shouldn't have")
		}

		citools.RunOnNode(t, node2, "dm push cluster_0")
		resp = citools.OutputFromRunOnNode(t, node1, "dm log")

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
		fsname := citools.UniqName()
		citools.RunOnNode(t, node2, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node2, "dm switch "+fsname)
		citools.RunOnNode(t, node2, "dm commit -m 'hello'")

		// XXX 'dm clone' currently tries to pull the named filesystem into the
		// _current active filesystem name_. instead, it should pull it into a
		// new filesystem with the same name. if the same named filesystem
		// already exists, it should error (and instruct the user to 'dm switch
		// foo; dm pull foo' instead).
		citools.RunOnNode(t, node1, "dm clone cluster_1 "+fsname)
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(resp, "hello") {
			// TODO fix this failure by sending prelude in intercluster case also
			t.Error("unable to find commit message remote's log output")
		}
		// test incremental pull
		citools.RunOnNode(t, node2, "dm commit -m 'again'")
		citools.RunOnNode(t, node1, "dm pull cluster_1 "+fsname)

		resp = citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(resp, "again") {
			t.Error("unable to find commit message remote's log output")
		}
		// test pulling branch with extant base
		citools.RunOnNode(t, node2, "dm checkout -b newbranch")
		citools.RunOnNode(t, node2, "dm commit -m 'branchy'")
		citools.RunOnNode(t, node1, "dm pull cluster_1 "+fsname+" newbranch")

		citools.RunOnNode(t, node1, "dm checkout newbranch")
		resp = citools.OutputFromRunOnNode(t, node1, "dm log")

		if !strings.Contains(resp, "branchy") {
			t.Error("unable to find commit message remote's log output")
		}
	})

	t.Run("CloneStashDiverged", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+fsname)
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'hello'")
		citools.RunOnNode(t, node2, "dm clone cluster_0 "+fsname)
		citools.RunOnNode(t, node2, "dm commit -m 'hello'")
		citools.RunOnNode(t, node2, "dm commit -m 'hello2'")
		citools.RunOnNode(t, node2, "dm clone --stash-on-divergence cluster_0 "+fsname)
		output := citools.OutputFromRunOnNode(t, node2, "dm branch")
		if !strings.Contains(output, "master-DIVERGED-") {
			t.Error("Stashed clone did not create divergent branch")
		}
	})
	t.Run("CloneStashToSnaps", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+fsname)
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'hello'")
		citools.RunOnNode(t, node2, "dm clone cluster_0 "+fsname)
		citools.RunOnNode(t, node2, "dm commit -m 'hello'")
		citools.RunOnNode(t, node2, "dm clone --stash-on-divergence cluster_0 "+fsname)
		output := citools.OutputFromRunOnNode(t, node2, "dm branch")
		if !strings.Contains(output, "master-DIVERGED-") {
			t.Error("Stashed clone did not create divergent branch")
		}
	})
	t.Run("Bug74MissingMetadata", func(t *testing.T) {
		fsname := citools.UniqName()

		// Commit 1
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'hello'")

		// Commit 2
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/Y")
		citools.RunOnNode(t, node1, "dm commit -m 'again'")

		// Ahhh, push it! https://www.youtube.com/watch?v=vCadcBR95oU
		citools.RunOnNode(t, node1, "dm push cluster_1 "+fsname)

		// What do we get on node2?
		citools.RunOnNode(t, node2, "dm switch "+fsname)
		resp := citools.OutputFromRunOnNode(t, node2, "dm log")

		if !strings.Contains(resp, "hello") ||
			!strings.Contains(resp, "again") {
			t.Error("Some history went missing (if it's bug#74 again, probably the 'hello')")
		}
	})

	t.Run("VolumeNameValidityChecking", func(t *testing.T) {
		// 1) dm init
		resp := citools.OutputFromRunOnNode(t, node1, "if dm init @; then false; else true; fi ")
		if !strings.Contains(resp, "invalid dot name") {
			t.Errorf("Didn't get an error when attempting to dm init an invalid volume name: %s", resp)
		}

		// 2) pull/clone it
		fsname := citools.UniqName()

		citools.RunOnNode(t, node2, citools.DockerRun(fsname)+" touch /foo/X")
		citools.RunOnNode(t, node2, "dm switch "+fsname)
		citools.RunOnNode(t, node2, "dm commit -m 'hello'")

		resp = citools.OutputFromRunOnNode(t, node1, "if dm clone cluster_0 "+fsname+" --local-name @; then false; else true; fi ")

		if !strings.Contains(resp, "invalid dot name") {
			t.Errorf("Didn't get an error when attempting to dm clone to an invalid volume name: %s", resp)
		}

		resp = citools.OutputFromRunOnNode(t, node1, "if dm pull cluster_0 @ --remote-name "+fsname+"; then false; else true; fi ")
		if !strings.Contains(resp, "invalid dot name") {
			t.Errorf("Didn't get an error when attempting to dm pull to an invalid volume name: %s", resp)
		}

		// 3) push it
		resp = citools.OutputFromRunOnNode(t, node2, "if dm push cluster_0 "+fsname+" --remote-name @; then false; else true; fi ")
		if !strings.Contains(resp, "invalid dot name") {
			t.Errorf("Didn't get an error when attempting to dm push to an invalid volume name: %s", resp)
		}
	})
}

func TestBackupAndRestoreTwoSingleNodeClusters(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{
		citools.NewCluster(1), // cluster_0_node_0
		citools.NewCluster(1), // cluster_1_node_0
	}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	cluster_0 := f[0].GetNode(0).Container

	t.Run("BackupAndRestore", func(t *testing.T) {

		// FOR OUR OWN OPERATIONAL USE
		//
		// Test that pushing dots and restoring etcd metadata results in intact
		// users & their dots (TODO: and their collaborators).

		bobKey := "bob is great"

		// Create user bob on the first node
		err := citools.RegisterUser(f[0].GetNode(0), "bob", "bob@bob.com", bobKey)
		if err != nil {
			t.Error(err)
		}

		var createResp bool

		err = citools.DoRPC(f[0].GetNode(0).IP, "bob", bobKey,
			"DotmeshRPC.Create",
			VolumeName{Namespace: "bob", Name: "restoreme"},
			&createResp)
		if err != nil {
			t.Error(err)
		}

		fsname := "bob/restoreme"

		citools.RunOnNode(t, cluster_0, "dm switch "+fsname)
		citools.RunOnNode(t, cluster_0, "dm commit -m 'hello'")

		// This is how you backup & restore dotmesh!
		citools.RunOnNode(t, cluster_0, "dm push cluster_1")
		citools.RunOnNode(t, cluster_0, "dm cluster backup-etcd > backup.json")
		citools.RunOnNode(t, cluster_0,
			"dm remote switch cluster_1 && "+
				"dm cluster restore-etcd < backup.json && "+
				"dm remote switch cluster_0",
		)

		// Wait a moment for etcd to reload itself.
		time.Sleep(5 * time.Second)

		listResp := map[string]map[string]DotmeshVolume{}
		// on the target cluster, can bob see his filesystem? can he auth? can
		// he see that it's his?
		err = citools.DoRPC(f[1].GetNode(0).IP, "bob", bobKey,
			"DotmeshRPC.List",
			struct {
			}{},
			&listResp)
		if err != nil {
			t.Error(err)
		}

		d, ok := listResp["bob"]["restoreme"]
		if !ok {
			t.Errorf("IT WAS NOT OK: %+v", listResp)
		}
		fmt.Printf("dot: %+v\n", d)

	})
}

func TestThreeSingleNodeClusters(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{
		citools.NewCluster(1), // cluster_0_node_0 - common
		citools.NewCluster(1), // cluster_1_node_0 - alice
		citools.NewCluster(1), // cluster_2_node_0 - bob
	}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	commonNode := f[0].GetNode(0)
	aliceNode := f[1].GetNode(0)
	bobNode := f[2].GetNode(0)

	bobKey := "bob is great"
	aliceKey := "alice is great"

	// Create users bob and alice on the common node
	err = citools.RegisterUser(commonNode, "bob", "bob@bob.com", bobKey)
	if err != nil {
		t.Error(err)
	}

	err = citools.RegisterUser(commonNode, "alice", "alice@bob.com", aliceKey)
	if err != nil {
		t.Error(err)
	}

	t.Run("TwoUsersSameNamedVolume", func(t *testing.T) {

		// bob and alice both push to the common node
		citools.RunOnNode(t, aliceNode.Container, citools.DockerRun("apples")+" touch /foo/alice")
		citools.RunOnNode(t, aliceNode.Container, "dm switch apples")
		citools.RunOnNode(t, aliceNode.Container, "dm commit -m'Alice commits'")
		citools.RunOnNode(t, aliceNode.Container, "dm push cluster_0 apples --remote-name alice/apples")

		citools.RunOnNode(t, bobNode.Container, citools.DockerRun("apples")+" touch /foo/bob")
		citools.RunOnNode(t, bobNode.Container, "dm switch apples")
		citools.RunOnNode(t, bobNode.Container, "dm commit -m'Bob commits'")
		citools.RunOnNode(t, bobNode.Container, "dm push cluster_0 apples --remote-name bob/apples")

		// bob and alice both clone from the common node
		citools.RunOnNode(t, aliceNode.Container, "dm clone cluster_0 bob/apples --local-name bob-apples")
		citools.RunOnNode(t, bobNode.Container, "dm clone cluster_0 alice/apples --local-name alice-apples")

		// Check they get the right volumes
		resp := citools.OutputFromRunOnNode(t, commonNode.Container, "dm list -H | cut -f 1 | grep apples")
		if resp != "alice/apples\nbob/apples\n" {
			t.Error("Didn't find alice/apples and bob/apples on common node")
		}

		resp = citools.OutputFromRunOnNode(t, aliceNode.Container, "dm list -H | cut -f 1 | grep apples")
		if resp != "apples\nbob-apples\n" {
			t.Error("Didn't find apples and bob-apples on alice's node")
		}

		resp = citools.OutputFromRunOnNode(t, bobNode.Container, "dm list -H | cut -f 1 | grep apples")
		if resp != "alice-apples\napples\n" {
			t.Error("Didn't find apples and alice-apples on bob's node")
		}

		// Check the volumes actually have the contents they should
		resp = citools.OutputFromRunOnNode(t, aliceNode.Container, citools.DockerRun("bob-apples")+" ls /foo/")
		if !strings.Contains(resp, "bob") {
			t.Error("Filesystem bob-apples had the wrong content")
		}

		resp = citools.OutputFromRunOnNode(t, bobNode.Container, citools.DockerRun("alice-apples")+" ls /foo/")
		if !strings.Contains(resp, "alice") {
			t.Error("Filesystem alice-apples had the wrong content")
		}

		// bob commits again
		citools.RunOnNode(t, bobNode.Container, citools.DockerRun("apples")+" touch /foo/bob2")
		citools.RunOnNode(t, bobNode.Container, "dm switch apples")
		citools.RunOnNode(t, bobNode.Container, "dm commit -m'Bob commits again'")
		citools.RunOnNode(t, bobNode.Container, "dm push cluster_0 apples --remote-name bob/apples")

		// alice pulls it
		citools.RunOnNode(t, aliceNode.Container, "dm pull cluster_0 bob-apples --remote-name bob/apples")

		// Check we got the change
		resp = citools.OutputFromRunOnNode(t, aliceNode.Container, citools.DockerRun("bob-apples")+" ls /foo/")
		if !strings.Contains(resp, "bob2") {
			t.Error("Filesystem bob-apples had the wrong content")
		}
	})

	t.Run("ShareBranches", func(t *testing.T) {
		// Alice pushes
		citools.RunOnNode(t, aliceNode.Container, citools.DockerRun("cress")+" touch /foo/alice")
		citools.RunOnNode(t, aliceNode.Container, "dm switch cress")
		citools.RunOnNode(t, aliceNode.Container, "dm commit -m'Alice commits'")
		citools.RunOnNode(t, aliceNode.Container, "dm push cluster_0 cress --remote-name alice/cress")

		// Generate branch and push
		citools.RunOnNode(t, aliceNode.Container, "dm checkout -b mustard")
		citools.RunOnNode(t, aliceNode.Container, citools.DockerRun("cress")+" touch /foo/mustard")
		citools.RunOnNode(t, aliceNode.Container, "dm commit -m'Alice commits mustard'")
		citools.RunOnNode(t, aliceNode.Container, "dm push cluster_0 cress mustard --remote-name alice/cress")

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
		citools.RunOnNode(t, bobNode.Container, "dm clone cluster_0 alice/cress mustard --local-name cress")
		citools.RunOnNode(t, bobNode.Container, "dm switch cress")
		citools.RunOnNode(t, bobNode.Container, "dm checkout mustard")

		// Check we got both changes
		// TODO: had to pin the branch here, seems like `dm switch V; dm
		// checkout B; docker run C ... -v V:/...` doesn't result in V@B being
		// mounted into C. this is probably surprising behaviour.
		resp := citools.OutputFromRunOnNode(t, bobNode.Container, citools.DockerRun("cress@mustard")+" ls /foo/")
		if !strings.Contains(resp, "alice") {
			t.Error("We didn't get the master branch")
		}
		if !strings.Contains(resp, "mustard") {
			t.Error("We didn't get the mustard branch")
		}
	})

	t.Run("DefaultRemoteNamespace", func(t *testing.T) {
		// Alice pushes to the common node with no explicit remote volume, should default to alice/pears
		citools.RunOnNode(t, aliceNode.Container, citools.DockerRun("pears")+" touch /foo/alice")
		citools.RunOnNode(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_pears alice@"+commonNode.IP)
		citools.RunOnNode(t, aliceNode.Container, "dm switch pears")
		citools.RunOnNode(t, aliceNode.Container, "dm commit -m'Alice commits'")
		citools.RunOnNode(t, aliceNode.Container, "dm push common_pears") // local pears becomes alice/pears

		// Check it gets there
		resp := citools.OutputFromRunOnNode(t, commonNode.Container, "dm list -H | cut -f 1 | sort")
		if !strings.Contains(resp, "alice/pears") {
			t.Error("Didn't find alice/pears on the common node")
		}
	})

	t.Run("DefaultRemoteVolume", func(t *testing.T) {
		// Alice pushes to the common node with no explicit remote volume, should default to alice/pears
		citools.RunOnNode(t, aliceNode.Container, citools.DockerRun("bananas")+" touch /foo/alice")
		citools.RunOnNode(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_bananas alice@"+commonNode.IP)
		citools.RunOnNode(t, aliceNode.Container, "dm switch bananas")
		citools.RunOnNode(t, aliceNode.Container, "dm commit -m'Alice commits'")
		citools.RunOnNode(t, aliceNode.Container, "dm push common_bananas bananas")

		// Check the remote branch got recorded
		resp := citools.OutputFromRunOnNode(t, aliceNode.Container, "dm dot show -H bananas | grep defaultUpstreamDot")
		if resp != "defaultUpstreamDot\tcommon_bananas\talice/bananas\n" {
			t.Error("alice/bananas is not the default remote for bananas on common_bananas")
		}

		// Add Bob as a collaborator
		err := citools.DoAddCollaborator(commonNode.IP, "alice", aliceKey, "alice", "bananas", "bob")
		if err != nil {
			t.Error(err)
		}

		// Clone it back as bob
		citools.RunOnNode(t, bobNode.Container, "echo '"+bobKey+"' | dm remote add common_bananas bob@"+commonNode.IP)
		// Clone should save admin/bananas@common => alice/bananas
		citools.RunOnNode(t, bobNode.Container, "dm clone common_bananas alice/bananas --local-name bananas")
		citools.RunOnNode(t, bobNode.Container, "dm switch bananas")

		// Check it did so
		resp = citools.OutputFromRunOnNode(t, bobNode.Container, "dm dot show -H bananas | grep defaultUpstreamDot")
		if resp != "defaultUpstreamDot\tcommon_bananas\talice/bananas\n" {
			t.Error("alice/bananas is not the default remote for bananas on common_bananas")
		}

		// And then do a pull, not specifying the remote or local volume
		// There is no bob/bananas, so this will fail if the default remote volume is not saved.
		citools.RunOnNode(t, bobNode.Container, "dm pull common_bananas") // local = bananas as we switched, remote = alice/banas from saved default

		// Now push back
		citools.RunOnNode(t, bobNode.Container, citools.DockerRun("bananas")+" touch /foo/bob")
		citools.RunOnNode(t, bobNode.Container, "dm commit -m'Bob commits'")
		citools.RunOnNode(t, bobNode.Container, "dm push common_bananas") // local = bananas as we switched, remote = alice/banas from saved default
	})

	t.Run("DefaultRemoteNamespaceOverride", func(t *testing.T) {
		// Alice pushes to the common node with no explicit remote volume, should default to alice/kiwis
		citools.RunOnNode(t, aliceNode.Container, citools.DockerRun("kiwis")+" touch /foo/alice")
		citools.RunOnNode(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_kiwis alice@"+commonNode.IP)
		citools.RunOnNode(t, aliceNode.Container, "dm switch kiwis")
		citools.RunOnNode(t, aliceNode.Container, "dm commit -m'Alice commits'")
		citools.RunOnNode(t, aliceNode.Container, "dm push common_kiwis") // local kiwis becomes alice/kiwis

		// Check the remote branch got recorded
		resp := citools.OutputFromRunOnNode(t, aliceNode.Container, "dm dot show -H kiwis | grep defaultUpstreamDot")
		if resp != "defaultUpstreamDot\tcommon_kiwis\talice/kiwis\n" {
			t.Error("alice/kiwis is not the default remote for kiwis on common_kiwis")
		}

		// Manually override it (the remote repo doesn't need to exist)
		citools.RunOnNode(t, aliceNode.Container, "dm dot set-upstream common_kiwis bob/kiwis")

		// Check the remote branch got changed
		resp = citools.OutputFromRunOnNode(t, aliceNode.Container, "dm dot show -H kiwis | grep defaultUpstreamDot")
		if resp != "defaultUpstreamDot\tcommon_kiwis\tbob/kiwis\n" {
			t.Error("bob/kiwis is not the default remote for kiwis on common_kiwis, looks like the set-upstream failed")
		}
	})

	t.Run("DeleteNotMineFails", func(t *testing.T) {
		fsname := citools.UniqName()

		// Alice pushes to the common node with no explicit remote volume, should default to alice/fsname
		citools.RunOnNode(t, aliceNode.Container, citools.DockerRun(fsname)+" touch /foo/alice")
		citools.RunOnNode(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_"+fsname+" alice@"+commonNode.IP)
		citools.RunOnNode(t, aliceNode.Container, "dm switch "+fsname)
		citools.RunOnNode(t, aliceNode.Container, "dm commit -m'Alice commits'")
		citools.RunOnNode(t, aliceNode.Container, "dm push common_"+fsname) // local fsname becomes alice/fsname

		// Bob tries to delete it
		citools.RunOnNode(t, bobNode.Container, "echo '"+bobKey+"' | dm remote add common_"+fsname+" bob@"+commonNode.IP)
		citools.RunOnNode(t, bobNode.Container, "dm remote switch common_"+fsname)
		// We expect failure, so reverse the sense
		resp := citools.OutputFromRunOnNode(t, bobNode.Container, "if dm dot delete -f alice/"+fsname+"; then false; else true; fi")
		if !strings.Contains(resp, "You are not the owner") {
			t.Error("bob was able to delete alices' volumes")
		}
	})

	t.Run("NamespaceAuthorisationNonexistant", func(t *testing.T) {
		citools.RunOnNode(t, aliceNode.Container, citools.DockerRun("grapes")+" touch /foo/alice")
		citools.RunOnNode(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_grapes alice@"+commonNode.IP)
		citools.RunOnNode(t, aliceNode.Container, "dm switch grapes")
		citools.RunOnNode(t, aliceNode.Container, "dm commit -m'Alice commits'")

		// Let's try and put things in a nonexistant namespace
		// Likewise, This SHOULD fail, so we reverse the sense of the return code.
		citools.RunOnNode(t, aliceNode.Container, "bash -c 'if dm push common_grapes --remote-name nonexistant/grapes; then exit 1; else exit 0; fi'")

		// Check it doesn't get there
		resp := citools.OutputFromRunOnNode(t, commonNode.Container, "dm list -H | cut -f 1 | sort")
		if strings.Contains(resp, "nonexistant/grapes") {
			t.Error("Found nonexistant/grapes on the common node - but alice shouldn't have been able to create that!")
		}
	})

	t.Run("NamespaceAuthorisation", func(t *testing.T) {
		citools.RunOnNode(t, aliceNode.Container, citools.DockerRun("passionfruit")+" touch /foo/alice")
		citools.RunOnNode(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_passionfruit alice@"+commonNode.IP)
		citools.RunOnNode(t, aliceNode.Container, "dm switch passionfruit")
		citools.RunOnNode(t, aliceNode.Container, "dm commit -m'Alice commits'")

		// Let's try and put things in bob's namespace.
		// This SHOULD fail, so we reverse the sense of the return code.
		citools.RunOnNode(t, aliceNode.Container, "bash -c 'if dm push common_passionfruit --remote-name bob/passionfruit; then exit 1; else exit 0; fi'")

		// Check it doesn't get there
		resp := citools.OutputFromRunOnNode(t, commonNode.Container, "dm list -H | cut -f 1 | sort")
		if strings.Contains(resp, "bob/passionfruit") {
			t.Error("Found bob/passionfruit on the common node - but alice shouldn't have been able to create that!")
		}
	})

	t.Run("NamespaceAuthorisationAdmin", func(t *testing.T) {
		citools.RunOnNode(t, aliceNode.Container, citools.DockerRun("prune")+" touch /foo/alice")
		citools.RunOnNode(t, aliceNode.Container, "dm switch prune")
		citools.RunOnNode(t, aliceNode.Container, "dm commit -m'Alice commits'")

		// Let's try and put things in bob's namespace, but using the cluster_0 remote which is logged in as admin
		// This should work, because we're admin, even though it's bob's namespace
		citools.RunOnNode(t, aliceNode.Container, "dm push cluster_0 --remote-name bob/prune")

		// Check it got there
		resp := citools.OutputFromRunOnNode(t, commonNode.Container, "dm list -H | cut -f 1 | sort")
		if !strings.Contains(resp, "bob/prune") {
			t.Error("Didn't find bob/prune on the common node - but alice should have been able to create that using her admin account!")
		}
	})

	// on alice's machine
	// ------------------
	// dm init foo
	// dm commit -m "initial"
	// dm checkout -b branch_a
	// dm commit -m "A commit"
	// dm push hub foo branch_a
	// <at this point, hub correctly shows master, its commit, branch_a, its commit>

	// over on bob's machine:
	// ----------------------
	// dm clone hub alice/foo branch_a
	// dm switch foo
	// dm checkout master
	// dm log <-- MYSTERIOUSLY EMPTY
	// dm checkout branch_a <-- works
	// dm commit -m "New commit from bob"
	// dm push hub [foo branch_a] <-- fails saying it can't find the commit with id of "initial" commit (I think)
	// ---- BUT! -----
	// if bob started by:
	// dm clone hub alice/foo [master]
	// dm switch foo
	// dm pull hub foo branch_a
	// ... then everything works!

	/*
		t.Run("Issue226", func(t *testing.T) {
			fsname := citools.UniqName()
			citools.RunOnNode(t, aliceNode.Container, "echo '"+aliceKey+"' | dm remote add common_"+fsname+" alice@"+commonNode.IP)

			citools.RunOnNode(t, aliceNode.Container, "dm init "+fsname)
			citools.RunOnNode(t, aliceNode.Container, "dm switch "+fsname)
			citools.RunOnNode(t, aliceNode.Container, "dm commit -m initial")
			citools.RunOnNode(t, aliceNode.Container, "dm checkout -b branch_a")
			citools.RunOnNode(t, aliceNode.Container, "dm commit -m 'commit on a'")
			citools.RunOnNode(t, aliceNode.Container, "dm push common_"+fsname+" "+fsname+" branch_a")

			citools.RunOnNode(t, bobNode.Container, "echo '"+bobKey+"' | dm remote add common_"+fsname+" bob@"+commonNode.IP)
			citools.RunOnNode(t, bobNode.Container, "dm clone common_"+fsname+" alice/"+fsname+" branch_a")
			citools.RunOnNode(t, bobNode.Container, "dm switch "+fsname)
			citools.RunOnNode(t, bobNode.Container, "dm checkout master")
			st := citools.OutputFromRunOnNode(t, bobNode.Container, "dm log")
			fmt.Printf("Master log, should include 'initial': %+v\n", st)
		})
	*/
}

func HttpGetWithTimeout(url string) (*http.Response, error) {
	timeout := time.Duration(5 * time.Second)
	client := http.Client{
		Timeout: timeout,
	}
	return client.Get(url)
}

/***********************************************************************

    KUBERNETES TEST HACKERS - PLEASE READ THIS

  The networking setup when we install k8s clusters means that k8s is
  always transparently routing dotmesh API requests to a live server
  pod via a NodePort service - so:

  1) The IPs in f[X].GetNode(Y).IP are all interchangeable from the
  perspective of the Dotmesh API on port 32607, and don't ACTUALLY
  address that node.

  2) The `dm remote`s set up for different nodes in a cluster all
  actually talk to any node in that cluster, not the ones named by
  that remote.

  3) If you run a `dm` command on a node, with the default `local`
  remote, it will talk to an arbitrary node in that cluster.

 ***********************************************************************/

func TestKubernetesOperator(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewKubernetes(3, "pvcPerNode", true)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err) // there's no point carrying on
	}
	node1 := f[0].GetNode(0)

	citools.LogTiming("setup")

	// A quick smoke test of basic operation in this mode
	t.Run("DynamicProvisioning", func(t *testing.T) {
		fmt.Printf("Starting dynamic provisioning test... applying PVC\n")
		// Ok, now we have the plumbing set up, try creating a PVC and see if it gets a PV dynamically provisioned
		citools.KubectlApply(t, node1.Container, `
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

		fmt.Printf("PVC created.\n")
		citools.LogTiming("DynamicProvisioning: PV Claim")
		err = citools.TryUntilSucceeds(func() error {
			fmt.Printf("Waiting for PV to appear...")
			result := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get pv")
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
		fmt.Printf("Got it!\n")

		citools.LogTiming("DynamicProvisioning: finding grapes PV")

		// Now let's see if a container can see it, and put content there that a k8s container can pick up
		citools.RunOnNode(t, node1.Container,
			"docker run --rm -i -v k8s/dynamic-grapes.static-html:/foo --volume-driver dm "+
				"busybox sh -c \"echo 'grapes' > /foo/on-the-vine\"",
		)

		citools.KubectlApply(t, node1.Container, `
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

		citools.LogTiming("DynamicProvisioning: grape Deployment")
		citools.KubectlApply(t, node1.Container, `
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

		citools.LogTiming("DynamicProvisioning: grape Service")
		err = citools.TryUntilSucceeds(func() error {
			fmt.Printf("About to make an http request to: http://%s:30050/on-the-vine\n", node1.IP)

			resp, err := HttpGetWithTimeout(fmt.Sprintf("http://%s:30050/on-the-vine", node1.IP))
			if err != nil {
				fmt.Printf(citools.OutputFromRunOnNode(t, node1.Container, citools.KUBE_DEBUG_CMD) + "\n")
				return err
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Printf(citools.OutputFromRunOnNode(t, node1.Container, citools.KUBE_DEBUG_CMD) + "\n")
				return err
			}
			if !strings.Contains(string(body), "grapes") {
				fmt.Printf(citools.OutputFromRunOnNode(t, node1.Container, citools.KUBE_DEBUG_CMD) + "\n")
				return fmt.Errorf("No grapes on the vine, got this instead: %v", string(body))
			}
			return nil
		}, "finding grapes on the vine")
		if err != nil {
			t.Error(err)
		}

		citools.LogTiming("DynamicProvisioning: Grapes on the vine")
	})

	t.Run("PVCReuse", func(t *testing.T) {
		// Record the names of the PVCs we use
		initialPvcs := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get pvc -n dotmesh | cut -f 1 -d ' ' | sort")

		// Modify nodeSelector to clusterSize-2=yes in the configmap and restart operator.
		err := citools.ChangeOperatorNodeSelector(node1.Container, "clusterSize-2=yes")
		if err != nil {
			t.Error(err)
		}
		citools.RestartOperator(t, node1.Container)

		err = citools.TryUntilSucceeds(func() error {
			serverPods := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get pods -n dotmesh | grep server | wc -l")
			if serverPods == "2\n" {
				return nil
			}
			return fmt.Errorf("Didn't go down to two pods, got pods: %v", serverPods)
		}, "Trying to go down to two pods")
		if err != nil {
			t.Error(err)
		}

		// Modify nodeSelector to clusterSize-3=yes in the configmap and restart operator.
		err = citools.ChangeOperatorNodeSelector(node1.Container, "clusterSize-3=yes")
		if err != nil {
			t.Error(err)
		}
		citools.RestartOperator(t, node1.Container)

		// Check we go up to three pods and don't create a new PVC.
		err = citools.TryUntilSucceeds(func() error {
			serverPods := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get pods -n dotmesh | grep server- | wc -l")
			serverPvcs := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get pvc -n dotmesh | grep pvc- | wc -l")
			fmt.Printf("DM server pods: %s, DM server PVCs: %s\n",
				strings.TrimSpace(serverPods),
				strings.TrimSpace(serverPvcs))
			if serverPods == "3\n" && serverPvcs == "3\n" {
				return nil
			}
			return fmt.Errorf("Didn't go up to three pods, got pods: %v pvcs: %v", serverPods, serverPvcs)
		}, "Trying to go up to three pods/pvcs")

		if err != nil {
			t.Error(err)
		}

		finalPvcs := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get pvc -n dotmesh | cut -f 1 -d ' ' | sort")
		if initialPvcs != finalPvcs {
			t.Errorf("Didn't end up with the same PVCs as we started with. Before: %+v After: %+v", initialPvcs, finalPvcs)
		}
	})

	t.Run("PVCMakeNew", func(t *testing.T) {
		// Modify nodeSelector to clusterSize-2=yes in the configmap and restart operator.
		err := citools.ChangeOperatorNodeSelector(node1.Container, "clusterSize-2=yes")
		if err != nil {
			t.Error(err)
		}
		citools.RestartOperator(t, node1.Container)

		// Check we go down to two pods.
		err = citools.TryUntilSucceeds(func() error {
			serverPods := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get pods -n dotmesh | grep server | wc -l")
			if serverPods == "2\n" {
				return nil
			}
			return fmt.Errorf("Didn't go down to two pods, got pods: %v", serverPods)
		}, "Trying to go down to two pods")
		if err != nil {
			t.Error(err)
		}

		// Delete the abandoned PVC
		pvcs := map[string]struct{}{}
		pvcNames := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get pvc -n dotmesh | grep pvc- | cut -f 1 -d ' '")
		for _, pvcName := range strings.Split(pvcNames, "\n") {
			if pvcName != "" {
				pvcs[pvcName] = struct{}{}
			}
		}
		serverPvcNames := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get pods -n dotmesh | grep server- | sed s/^server-pvc-// | sed s/-cluster-.*//")
		for _, pvcNameSuffix := range strings.Split(serverPvcNames, "\n") {
			if pvcNameSuffix != "" {
				for pvc, _ := range pvcs {
					if strings.Contains(pvc, pvcNameSuffix) {
						delete(pvcs, pvc)
					}
				}
			}
		}

		if len(pvcs) != 1 {
			t.Fatalf("Couldn't find the abandoned PVC name! Ended up with %#v", pvcs)
		}

		for pvcName, _ := range pvcs {
			citools.RunOnNode(t, node1.Container, "kubectl delete pvc -n dotmesh "+pvcName)
		}

		// Modify nodeSelector to clusterSize-3=yes in the configmap and restart operator.
		err = citools.ChangeOperatorNodeSelector(node1.Container, "clusterSize-3=yes")
		if err != nil {
			t.Error(err)
		}
		citools.RestartOperator(t, node1.Container)

		// Check we go up to three pods and create a new PVC.
		err = citools.TryUntilSucceeds(func() error {
			serverPods := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get pods -n dotmesh | grep server- | wc -l")
			serverPvcs := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get pvc -n dotmesh | grep pvc- | wc -l")
			fmt.Printf("DM server pods: %s, DM server PVCs: %s\n",
				strings.TrimSpace(serverPods),
				strings.TrimSpace(serverPvcs))
			if serverPods == "3\n" && serverPvcs == "3\n" {
				return nil
			}
			return fmt.Errorf("Didn't go up to three pods, got pods: %v pvcs: %v", serverPods, serverPvcs)
		}, "Trying to go up to three pods/pvcs")

		if err != nil {
			t.Error(err)
		}

	})

	citools.DumpTiming()
}

func TestKubernetesVolumes(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewKubernetes(3, "local", false)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err) // there's no point carrying on
	}
	node1 := f[0].GetNode(0)

	citools.LogTiming("setup")
	t.Run("FlexVolume", func(t *testing.T) {
		// init a dotmesh volume and put some data in it
		citools.RunOnNode(t, node1.Container,
			"docker run --rm -i -v apples:/foo --volume-driver dm "+
				"busybox sh -c \"echo 'apples' > /foo/on-the-tree\"",
		)

		citools.LogTiming("FlexVolume: init")
		// create a PV referencing the data
		citools.KubectlApply(t, node1.Container, `
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

		citools.LogTiming("FlexVolume: create PV")
		// run a pod with a PVC which lists the data (web server)
		// check that the output of querying the pod is that we can see
		// that the apples are on the tree
		citools.KubectlApply(t, node1.Container, `
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
		citools.LogTiming("FlexVolume: PV Claim")

		citools.KubectlApply(t, node1.Container, `
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

		citools.LogTiming("FlexVolume: apple Deployment")
		citools.KubectlApply(t, node1.Container, `
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

		citools.LogTiming("FlexVolume: apple Service")

		err = citools.TryUntilSucceeds(func() error {
			fmt.Printf("About to make an http request to: http://%s:30003/on-the-tree\n", node1.IP)

			resp, err := HttpGetWithTimeout(fmt.Sprintf("http://%s:30003/on-the-tree", node1.IP))
			if err != nil {
				fmt.Printf(citools.OutputFromRunOnNode(t, node1.Container, citools.KUBE_DEBUG_CMD))
				return err
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				fmt.Printf(citools.OutputFromRunOnNode(t, node1.Container, citools.KUBE_DEBUG_CMD))
				return err
			}
			if !strings.Contains(string(body), "apples") {
				fmt.Printf(citools.OutputFromRunOnNode(t, node1.Container, citools.KUBE_DEBUG_CMD))
				return fmt.Errorf("No apples on the tree, got this instead: %v", string(body))
			}
			return nil
		}, "finding apples on the tree")
		if err != nil {
			t.Error(err)
		}

		citools.LogTiming("FlexVolume: Apples on the Tree")
	})

	t.Run("DynamicProvisioning", func(t *testing.T) {
		// Ok, now we have the plumbing set up, try creating a PVC and see if it gets a PV dynamically provisioned
		citools.KubectlApply(t, node1.Container, `
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

		citools.LogTiming("DynamicProvisioning: PV Claim")
		err = citools.TryUntilSucceeds(func() error {
			result := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get pv")
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

		citools.LogTiming("DynamicProvisioning: finding grapes PV")

		// Now let's see if a container can see it, and put content there that a k8s container can pick up
		citools.RunOnNode(t, node1.Container,
			"docker run --rm -i -v k8s/dynamic-grapes.static-html:/foo --volume-driver dm "+
				"busybox sh -c \"echo 'grapes' > /foo/on-the-vine\"",
		)

		citools.KubectlApply(t, node1.Container, `
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

		citools.LogTiming("DynamicProvisioning: grape Deployment")
		citools.KubectlApply(t, node1.Container, `
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

		citools.LogTiming("DynamicProvisioning: grape Service")
		err = citools.TryUntilSucceeds(func() error {
			fmt.Printf("About to make an http request to: http://%s:30050/on-the-vine\n", node1.IP)

			resp, err := HttpGetWithTimeout(fmt.Sprintf("http://%s:30050/on-the-vine", node1.IP))
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

		citools.LogTiming("DynamicProvisioning: Grapes on the vine")
	})
	citools.DumpTiming()
}

// Test the dind-flexvolume and dind-dynamic-provisioner. In dind land, it
// should be able to do exactly the same tricks as dotmesh, but just by
// mkdir'ing a directory that happens to be bind-mount shared between nodes.
//
// Later, we can build tests on top which make the dotmesh operator _consume_
// dind PVs.

// THIS TEST IS DISABLED IN CI because the "drain" doesn't work when
// the node has pods on it managed by operators, it gives an error
// about unknown controller types :-(

func TestKubernetesTestTooling(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewKubernetes(3, "local", true)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err) // there's no point carrying on
	}
	node1 := f[0].GetNode(0)

	citools.LogTiming("setup")

	t.Run("DynamicProvisioning", func(t *testing.T) {
		citools.KubectlApply(t, node1.Container, `
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: dind-pvc-test
spec:
  storageClassName: dind-pv
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
`)

		citools.LogTiming("DynamicProvisioning: PV Claim")
		err := citools.TryUntilSucceeds(func() error {
			result := citools.OutputFromRunOnNode(t, node1.Container, "(kubectl get pv |grep default/dind-pvc-test) || true")
			// We really want a line like:
			// "pvc-85b6beb0-bb1f-11e7-8633-0242ff9ba756   1Gi        RWO           Delete          Bound     default/admin-grapes-pvc   dotmesh                 15s"
			if !strings.Contains(result, "default/dind-pvc-test") {
				return fmt.Errorf("dind PV didn't get created")
			}
			return nil
		}, "finding the dind-pv-test PV")
		if err != nil {
			t.Error(err)
		}

		citools.KubectlApply(t, node1.Container, `
apiVersion: batch/v1
kind: Job
metadata:
  name: dind-setup
spec:
  template:
    metadata:
      labels:
        app: dind-setup
    spec:
      volumes:
      - name: dind-storage
        persistentVolumeClaim:
         claimName: dind-pvc-test
      restartPolicy: Never
      containers:
      - name: dind-setup
        image: busybox
        command: ['/bin/sh', '-c', 'echo dinds > /stuff/on-the-vine; ls /stuff']
        volumeMounts:
        - mountPath: "/stuff"
          name: dind-storage
`)

		citools.LogTiming("DynamicProvisioning: finding dind PV")

		citools.KubectlApply(t, node1.Container, `
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: dind-deployment
spec:
  replicas: 1
  template:
    metadata:
      labels:
        app: dind-server
    spec:
      volumes:
      - name: dind-storage
        persistentVolumeClaim:
         claimName: dind-pvc-test
      containers:
      - name: dind-server
        image: nginx:1.12.1
        volumeMounts:
        - mountPath: "/usr/share/nginx/html"
          name: dind-storage
`)

		citools.LogTiming("DynamicProvisioning: dind Deployment")
		citools.KubectlApply(t, node1.Container, `
apiVersion: v1
kind: Service
metadata:
   name: dind-service
spec:
   type: NodePort
   selector:
       app: dind-server
   ports:
     - port: 80
       nodePort: 30050
`)

		testServiceAvailability(t, node1.IP)

		citools.LogTiming("DynamicProvisioning: Dinds on the vine")

		// cordon the nginx node
		nodeToBeCordoned := citools.OutputFromRunOnNode(t, node1.Container, "kubectl get po -o=jsonpath='{.items[0].spec.nodeName}'")
		citools.RunOnNode(t, node1.Container, fmt.Sprintf("kubectl drain %s --delete-local-data --ignore-daemonsets", nodeToBeCordoned))

		// wait until new node for nginx appears
		err = citools.TryUntilSucceeds(func() error {
			result := citools.OutputFromRunOnNode(t, node1.Container, "(kubectl get po -o wide |grep dind-deployment) || true")
			if !strings.Contains(result, "Running") {
				return fmt.Errorf("dind-deployment didn't get re-scheduled")
			}
			if strings.Contains(result, nodeToBeCordoned) {
				return fmt.Errorf("dind-deployment still on original node")
			}
			return nil
		}, "finding the new dind-deployment")
		if err != nil {
			t.Error(err)
		}

		testServiceAvailability(t, node1.IP)
	})
	citools.DumpTiming()
}

func ensureDotIsFullyReplicated(t *testing.T, node string, fsname string) {
	for try := 1; try <= 5; try++ {
		st := citools.OutputFromRunOnNode(t, node, fmt.Sprintf("dm dot show %s", fsname))
		if !strings.Contains(st, "missing") {
			fmt.Print("Replicated")
			return
		} else {
			fmt.Print("Failed to replicate, sleeping and retrying")
			time.Sleep(1 * time.Second)
		}
	}
	t.Fatalf("Dot wouldn't stabilise")
}

func stopContainers(t *testing.T, node string) {
	citools.RunOnNode(t, node, "docker stop dotmesh-server")
	citools.RunOnNode(t, node, "docker rm -f dotmesh-server-inner || true")

	for try := 1; try <= 5; try++ {
		fmt.Printf("Dotmesh containers running on %s: ", node)
		st := citools.OutputFromRunOnNode(t, node, "docker ps | grep dotmesh-server | wc -l")
		if st == "0\n" {
			return
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	t.Fatalf("Containers wouldn't stop on %+v", node)
}

func startContainers(t *testing.T, node string) {
	citools.RunOnNode(t, node, "docker start dotmesh-server")

	for try := 1; try <= 5; try++ {
		fmt.Printf("Dotmesh containers running on %s: ", node)
		st := citools.OutputFromRunOnNode(t, node, "dm version | grep 'Unable to connect' | wc -l")
		if st == "0\n" {
			return
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	t.Fatalf(
		"Containers wouldn't start on %+v; SERVER\n%s\n INNER\n%s\n SERVER LOGS\n%s\n INNER LOGS\n%s",
		node,
		citools.OutputFromRunOnNode(t, node, "docker inspect dotmesh-server"),
		citools.OutputFromRunOnNode(t, node, "docker inspect dotmesh-server-inner"),
		citools.OutputFromRunOnNode(t, node, "docker logs dotmesh-server"),
		citools.OutputFromRunOnNode(t, node, "docker logs dotmesh-server-inner"),
	)
}

func testServiceAvailability(t *testing.T, IP string) {
	citools.LogTiming("DynamicProvisioning: dind Service")
	err := citools.TryUntilSucceeds(func() error {
		fmt.Printf("About to make an http request to: http://%s:30050/on-the-vine\n", IP)

		resp, err := HttpGetWithTimeout(fmt.Sprintf("http://%s:30050/on-the-vine", IP))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if !strings.Contains(string(body), "dinds") {
			return fmt.Errorf("No dinds on the vine, got this instead: %v", string(body))
		}
		return nil
	}, "finding dinds on the vine")
	if err != nil {
		t.Error(err)
	}
}

func TestStressLotsOfCommits(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping lots of commits test in short mode.")
	}

	citools.TeardownFinishedTestRuns()

	f := citools.Federation{
		citools.NewCluster(1),
		citools.NewCluster(1),
	}

	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	cluster0 := f[0].GetNode(0)
	cluster1 := f[1].GetNode(0)

	NUMBER_OF_COMMITS := 1000

	t.Run("PushLotsOfCommitsTest", func(t *testing.T) {
		fsname := citools.UniqName()
		for i := 0; i < NUMBER_OF_COMMITS; i++ {
			citools.RunOnNode(t, cluster0.Container, citools.DockerRun(fsname)+
				fmt.Sprintf(" sh -c 'echo STUFF-%d > /foo/whatever'", i))
			if i == 0 {
				citools.RunOnNode(t, cluster0.Container, fmt.Sprintf("dm switch %s", fsname))
			}
			citools.RunOnNode(t, cluster0.Container, fmt.Sprintf("dm commit -m'Commit %d - the rain in spain falls mainly on the plain; the fox jumped over the dog and all that'", i))
		}

		citools.RunOnNode(t, cluster0.Container, fmt.Sprintf("dm push cluster_1"))

		// Now go to cluster1 and do dm list, dm log and look for all those commits

		st := citools.OutputFromRunOnNode(t, cluster1.Container, "dm list")
		if !strings.Contains(st, fsname) {
			t.Errorf("We didn't see the fsname we expected (%s) in %s", fsname, st)
		}

		citools.RunOnNode(t, cluster1.Container, fmt.Sprintf("dm switch %s", fsname))
		st = citools.OutputFromRunOnNode(t, cluster1.Container, "dm log | grep 'the rain in spain' | wc -l")
		if st != fmt.Sprintf("%d\n", NUMBER_OF_COMMITS) {
			t.Errorf("We didn't see the right number of commits: Got '%s', wanted %d", st, NUMBER_OF_COMMITS)
		}
		checkTestContainerExits(t, cluster1.Container)
		citools.RunOnNode(t, cluster1.Container, fmt.Sprintf("dm dot delete -f %s", fsname))
	})
}

func TestStressHandover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress tests in short mode.")
	}

	citools.TeardownFinishedTestRuns()

	// Tests in this suite should not assume how many nodes we have in
	// the cluster, and iterate over f[0].GetNodes, so that we can
	// scale it to the test hardware we have. Might even pick up the
	// cluster size from an env variable.
	f := citools.Federation{
		citools.NewCluster(5),
	}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatal(err)
	}
	commonNode := f[0].GetNode(0)

	t.Run("HandoverStressTest", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, commonNode.Container, citools.DockerRun(fsname)+" sh -c 'echo STUFF > /foo/whatever'")

		for iteration := 0; iteration <= 10; iteration++ {
			for nid, node := range f[0].GetNodes() {
				runId := fmt.Sprintf("%d/%d", iteration, nid)
				st := citools.OutputFromRunOnNode(t, node.Container, citools.DockerRun(fsname)+" sh -c 'echo "+runId+"; cat /foo/whatever'")
				if !strings.Contains(st, "STUFF") {
					t.Error(fmt.Sprintf("We didn't see the STUFF we expected"))
				}
			}
		}
	})
}

type DotmeshVolume struct {
	Id             string
	Name           VolumeName
	Branch         string
	Master         string
	SizeBytes      int64
	DirtyBytes     int64
	CommitCount    int64
	ServerStatuses map[string]string // serverId => status
}

type VolumeName struct {
	Namespace string
	Name      string
}
