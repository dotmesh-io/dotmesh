package main

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/dotmesh-io/citools"
)

func s3cmd(params string) string {
	s3SecretKey := os.Getenv("S3_SECRET_KEY")
	s3AccessKey := os.Getenv("S3_ACCESS_KEY")
	return fmt.Sprintf("docker run -v ${PWD}:/app --workdir /app garland/docker-s3cmd s3cmd --access_key=%s --secret_key=%s %s", s3AccessKey, s3SecretKey, params)
}

func PutBackS3Files(node string) {
	citools.RunOnNodeErr(node, s3cmd("rm s3://dotmesh-transfer-test/newfile.txt"))
	citools.RunOnNodeErr(node, s3cmd("rm s3://dotmesh-transfer-test/somedir/hello-world.txt"))

	makeS3File(node, "hello-world.txt", "hello, world", "dotmesh-transfer-test")
}

func makeS3File(node, filename, contents, bucket string) {
	citools.RunOnNodeErr(node, "echo '"+contents+"' > "+filename)
	citools.RunOnNodeErr(node, s3cmd(fmt.Sprintf("put %s s3://%s", filename, bucket)))
}

func EmptyBucket(node string) {
	citools.RunOnNodeErr(node, s3cmd("rm s3://dotmesh-transfer-test-empty/pushed-file.txt"))
}

func TestS3Remote(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewCluster(1)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatalf("failed to start cluster, error: %s", err)
	}
	node1 := f[0].GetNode(0).Container

	s3SecretKey := os.Getenv("S3_SECRET_KEY")
	s3AccessKey := os.Getenv("S3_ACCESS_KEY")
	// TODO not sure the s3 mock used here actually cares for authentication so this may not be enough. Also probably doesn't support versioning...
	citools.RunOnNode(t, node1, "docker run --name my_s3 -p 4569:4569 -d lphoward/fake-s3")
	citools.RunOnNode(t, node1, "dm s3 remote add test-real-s3 "+s3AccessKey+":"+s3SecretKey)
	citools.RunOnNode(t, node1, "dm s3 remote add test-s3 FAKEKEY:FAKESECRET@http://127.0.0.1:4569")
	PutBackS3Files(node1)
	EmptyBucket(node1)
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
		citools.RunOnNode(t, node1, "dm clone test-real-s3 dotmesh-transfer-test --local-name="+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}
		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/")
		if !strings.Contains(resp, "hello-world.txt") {
			t.Error("failed to clone s3 bucket")
		}
	})

	t.Run("PushPullDirectories", func(t *testing.T) {
		// TODO: need to check whether keys containing a dir in s3 break stuff, and vice versa
		citools.RunOnNode(t, node1, "mkdir -p somedir && echo 'directories' > somedir/hello-world.txt")
		citools.RunOnNode(t, node1, s3cmd("put somedir/hello-world.txt s3://dotmesh-transfer-test/somedir/hello-world.txt"))
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm clone test-real-s3 dotmesh-transfer-test --local-name="+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/somedir")
		if !strings.Contains(resp, "hello-world.txt") {
			t.Error("failed to clone s3 bucket")
		}
	})

	t.Run("Pull", func(t *testing.T) {
		PutBackS3Files(node1)
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm clone test-real-s3 dotmesh-transfer-test --local-name="+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/")
		if !strings.Contains(resp, "hello-world.txt") {
			t.Error("failed to clone s3 bucket")
		}
		makeS3File(node1, "newfile.txt", "new file", "dotmesh-transfer-test")
		citools.RunOnNode(t, node1, "dm pull test-real-s3 "+fsname)

		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/")
		if !strings.Contains(resp, "hello-world.txt") {
			t.Error("Unexpectedly deleted file")
		}
		if !strings.Contains(resp, "newfile.txt") {
			t.Error("Did not pull down new file")
		}
		citools.RunOnNode(t, node1, s3cmd("rm s3://dotmesh-transfer-test/hello-world.txt"))
		citools.RunOnNode(t, node1, "dm pull test-real-s3 "+fsname)
		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/")
		if strings.Contains(resp, "hello-world.txt") {
			t.Error("Did not delete file")
		}
		citools.RunOnNode(t, node1, "dm pull test-real-s3 "+fsname)
		resp = citools.OutputFromRunOnNode(t, node1, "dm dot show "+fsname+" -H | grep commitCount")
		if !strings.Contains(resp, "\t3") {
			t.Error("Created extra commit for no change")
		}
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/hello-world.txt")
		citools.TryUntilSucceeds(func() error {
			output := citools.OutputFromRunOnNode(t, node1, "dm dot show "+fsname+" -H | grep dirty")
			if strings.Contains(output, "\t0") {
				return fmt.Errorf("not dirty yet")
			}
			return nil
		}, "waiting for dirty data...")
		_, err := citools.RunOnNodeErr(node1, "dm pull test-real-s3 "+fsname)
		if err == nil {
			t.Error("Pull command did not detect dirty data")
		}
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'non-s3 pushed data'")
		_, err = citools.RunOnNodeErr(node1, "dm pull test-real-s3 "+fsname)
		if err == nil {
			t.Error("Pull command did not detect extra commits")
		}
	})

	t.Run("Push", func(t *testing.T) {
		PutBackS3Files(node1)
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm clone test-real-s3 dotmesh-transfer-test --local-name="+fsname)
		_, err := citools.RunOnNodeErr(node1, "dm push test-real-s3 "+fsname)
		if err == nil {
			t.Error("Push command did not detect there were no changes")
		}
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/pushed-file.txt")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'push this back to s3'")
		citools.RunOnNode(t, node1, "dm push test-real-s3 "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, s3cmd("ls s3://dotmesh-transfer-test"))
		if !strings.Contains(resp, "pushed-file.txt") {
			t.Error("Did not push new file to S3")
		}
		_, err = citools.RunOnNodeErr(node1, "dm push test-real-s3 "+fsname)
		if err == nil {
			t.Error("Push command did not detect there were no changes")
		}
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" rm /foo/hello-world.txt")
		citools.TryUntilSucceeds(func() error {
			output := citools.OutputFromRunOnNode(t, node1, "dm dot show "+fsname+" -H | grep dirty")
			if strings.Contains(output, "\t0") {
				return fmt.Errorf("not dirty yet")
			}
			return nil
		}, "waiting for dirty data...")
		citools.RunOnNode(t, node1, "dm commit -m 'cut this file from s3'")
		citools.RunOnNode(t, node1, "dm push test-real-s3 "+fsname)
		resp = citools.OutputFromRunOnNode(t, node1, s3cmd("ls s3://dotmesh-transfer-test"))
		if strings.Contains(resp, "hello-world.txt") {
			t.Error("Did not delete file from S3")
		}
	})

	t.Run("ClonePushThenDeleteDot", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm clone test-real-s3 dotmesh-transfer-test --local-name="+fsname)
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/pushed-file.txt")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'push this back to s3'")
		citools.RunOnNode(t, node1, "dm push test-real-s3 "+fsname)
		_, err := citools.RunOnNodeErr(node1, "dm dot delete -f "+fsname)
		if err != nil {
			t.Error("Failed deleting dot after push")
		}
	})

	t.Run("CloneSubset", func(t *testing.T) {
		PutBackS3Files(node1)
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "mkdir -p subset && echo 'directories' > subset/subdir.txt")
		citools.RunOnNode(t, node1, s3cmd("put subset/subdir.txt s3://dotmesh-transfer-test/subset/subdir.txt"))
		citools.RunOnNode(t, node1, "dm s3 clone-subset test-real-s3 dotmesh-transfer-test subset/ --local-name="+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/")
		if !strings.Contains(resp, "subset") {
			t.Error("Did not find subset dir")
		}
		if strings.Contains(resp, "hello-world.txt") {
			t.Error("Found extra files we should have skipped")
		}
		fsname2 := citools.UniqName()
		citools.RunOnNode(t, node1, "dm s3 clone-subset test-real-s3 dotmesh-transfer-test hello- --local-name="+fsname2)
		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname2)+" ls /foo/")
		if strings.Contains(resp, "subset") {
			t.Error("Found files which shouldn't be cloned")
		}
		if !strings.Contains(resp, "hello-world.txt") {
			t.Error("Did not clone hello-world.txt")
		}
		fsname3 := citools.UniqName()
		makeS3File(node1, "newfile.txt", "new files are cool", "dotmesh-transfer-test")
		citools.RunOnNode(t, node1, "dm s3 clone-subset test-real-s3 dotmesh-transfer-test hello-,newfile.txt --local-name="+fsname3)
		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname3)+" ls /foo/")
		if strings.Contains(resp, "subset") {
			t.Error("Found files which shouldn't be cloned")
		}
		if !strings.Contains(resp, "hello-world.txt") {
			t.Error("Did not clone hello-world.txt")
		}
		if !strings.Contains(resp, "newfile.txt") {
			t.Error("Did not clone newfile.txt")
		}
	})

	t.Run("CloneSubsetPushPull", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm s3 clone-subset test-real-s3 dotmesh-transfer-test subset/ --local-name="+fsname)
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/file.txt")
		citools.TryUntilSucceeds(func() error {
			output := citools.OutputFromRunOnNode(t, node1, "dm dot show "+fsname+" -H | grep dirty")
			if strings.Contains(output, "\t0") {
				return fmt.Errorf("not dirty yet")
			}
			return nil
		}, "waiting for dirty data...")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'add file to s3'")
		citools.RunOnNode(t, node1, "dm push test-real-s3 "+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, s3cmd("ls s3://dotmesh-transfer-test"))
		if !strings.Contains(resp, "hello-world.txt") {
			citools.RunOnNode(t, node1, s3cmd("put hello-world.txt s3://dotmesh-transfer-test"))
			t.Error("Deleted a file we aren't tracking")
		}
		PutBackS3Files(node1)
		citools.RunOnNode(t, node1, "mkdir -p subset && echo 'directories' > subset/subdir.txt")
		citools.RunOnNode(t, node1, s3cmd("put subset/subdir.txt s3://dotmesh-transfer-test/subset/subdir.txt"))
		fsname2 := citools.UniqName()
		citools.RunOnNode(t, node1, "dm s3 clone-subset test-real-s3 dotmesh-transfer-test subset/ --local-name="+fsname2)
		citools.RunOnNode(t, node1, "dm pull test-real-s3 "+fsname2)
		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname2)+" ls /foo/")
		if strings.Contains(resp, "hello-world.txt") {
			t.Error("Pulled down a file we aren't tracking")
		}
	})

	t.Run("CloneNoLocalName", func(t *testing.T) {
		// todo
		// clone a bucket without specifying a local name
		// check that the volume locally doesn't end up being some weird dotmesh subdot config/ dots are removed from the local name
	})

	t.Run("InitThenPush", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+fsname)
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/pushed-file.txt")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'initial commit'")
		// try pushing to s3
		citools.RunOnNode(t, node1, "dm push test-real-s3 --remote-name=dotmesh-transfer-test-empty "+fsname)
		// check the file got created
		resp := citools.OutputFromRunOnNode(t, node1, s3cmd("ls s3://dotmesh-transfer-test-empty"))
		if !strings.Contains(resp, "pushed-file.txt") {
			t.Error("Did not push new file to S3")
		}
	})

	t.Run("InitThenPull", func(t *testing.T) {
		// todo
		// create a dot
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+fsname)
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'initial commit'")
		// try pulling from s3
		_, err = citools.RunOnNodeErr(node1, "dm pull test-real-s3 --remote-name=dotmesh-transfer-test "+fsname)
		if err == nil {
			t.Error("Pull command did not detect extra commits")
		}
	})

	t.Run("InitThenPushPull", func(t *testing.T) {
		// todo
		// create a dot
		// set up an s3 remote
		// try pushing to it
		// change something on s3
		// pull it
	})
}

func TestS3Stress(t *testing.T) {
	citools.TeardownFinishedTestRuns()

	f := citools.Federation{citools.NewCluster(1)}
	defer citools.TestMarkForCleanup(f)
	citools.AddFuncToCleanups(func() { citools.TestMarkForCleanup(f) })

	citools.StartTiming()
	err := f.Start(t)
	if err != nil {
		t.Fatalf("failed to start cluster, error: %s", err)
	}
	node1 := f[0].GetNode(0).Container

	s3SecretKey := os.Getenv("S3_SECRET_KEY")
	s3AccessKey := os.Getenv("S3_ACCESS_KEY")
	citools.RunOnNode(t, node1, "dm s3 remote add test-real-s3 "+s3AccessKey+":"+s3SecretKey)

	t.Run("Clone", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm clone test-real-s3 dotmesh-transfer-stress-test --local-name="+fsname)

		resp := citools.OutputFromRunOnNode(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}

		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/")
		if !strings.Contains(resp, "large-file") {
			t.Error("unable to find the large file")
		}
		if !strings.Contains(resp, "historical-file") {
			t.Error("unable to find the historical file")
		}

		files := strings.Split(resp, "\n")
		smallFiles := 0
		for _, file := range files {
			if strings.HasPrefix(file, "small-file") {
				smallFiles = smallFiles + 1
			}
		}
		if smallFiles != 100000 {
			t.Errorf("Expected 100000 small files, got %d", smallFiles)
		}

		t.Errorf("ABS TEST: files=%#v", files)

		citools.RunOnNode(t, node1, "dm switch "+fsname)
		resp = citools.OutputFromRunOnNode(t, node1, "dm log")

		t.Errorf("ABS TEST: log=%s", resp)

		/*
			if !strings.Contains(resp, fsname) {
				t.Error("unable to find volume name in ouput")
			}
		*/
	})
}
