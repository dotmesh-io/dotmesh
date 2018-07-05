package main

import (
	"fmt"
	"github.com/dotmesh-io/citools"
	"os"
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
	s3SecretKey := os.Getenv("S3_SECRET_KEY")
	s3AccessKey := os.Getenv("S3_ACCESS_KEY")
	// TODO not sure the s3 mock used here actually cares for authentication so this may not be enough. Also probably doesn't support versioning...
	citools.RunOnNode(t, node1, "docker run --name my_s3 -p 4569:4569 -d lphoward/fake-s3")
	citools.RunOnNode(t, node1, "dm s3 remote add test-real-s3 "+s3AccessKey+":"+s3SecretKey)
	citools.RunOnNode(t, node1, "dm s3 remote add test-s3 FAKEKEY:FAKESECRET@http://127.0.0.1:4569")

	s3cmd := "docker run -v ${PWD}:/app --workdir /app garland/docker-s3cmd s3cmd --access_key=" + s3AccessKey + " --secret_key=" + s3SecretKey
	citools.RunOnNode(t, node1, s3cmd+" rm s3://test.dotmesh/newfile.txt")
	citools.RunOnNode(t, node1, "echo 'hello, s3' > hello-world.txt")
	citools.RunOnNode(t, node1, s3cmd+" put hello-world.txt s3://test.dotmesh")
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
		citools.RunOnNode(t, node1, "dm clone test-real-s3 test.dotmesh --local-name="+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, "dm list")
		if !strings.Contains(resp, fsname) {
			t.Error("unable to find volume name in ouput")
		}
		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/")
		if !strings.Contains(resp, "hello-world.txt") {
			t.Error("failed to clone s3 bucket")
		}
	})

	t.Run("Pull", func(t *testing.T) {
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm clone test-real-s3 test.dotmesh --local-name="+fsname)
		resp := citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/")
		if !strings.Contains(resp, "hello-world.txt") {
			t.Error("failed to clone s3 bucket")
		}
		citools.RunOnNode(t, node1, "echo 'new file' > newfile.txt")
		citools.RunOnNode(t, node1, s3cmd+" put newfile.txt s3://test.dotmesh")
		citools.RunOnNode(t, node1, "dm pull test-real-s3 "+fsname)

		resp = citools.OutputFromRunOnNode(t, node1, citools.DockerRun(fsname)+" ls /foo/")
		if !strings.Contains(resp, "hello-world.txt") {
			t.Error("Unexpectedly deleted file")
		}
		if !strings.Contains(resp, "newfile.txt") {
			t.Error("Did not pull down new file")
		}
		citools.RunOnNode(t, node1, s3cmd+" rm s3://test.dotmesh/hello-world.txt")
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
		fsname := citools.UniqName()
		citools.RunOnNode(t, node1, "dm clone test-real-s3 test.dotmesh --local-name="+fsname)
		citools.RunOnNode(t, node1, citools.DockerRun(fsname)+" touch /foo/committedfile.txt")
		citools.TryUntilSucceeds(func() error {
			output := citools.OutputFromRunOnNode(t, node1, "dm dot show "+fsname+" -H | grep dirty")
			if strings.Contains(output, "\t0") {
				return fmt.Errorf("not dirty yet")
			}
			return nil
		}, "waiting for dirty data...")
		citools.RunOnNode(t, node1, "dm switch "+fsname)
		citools.RunOnNode(t, node1, "dm commit -m 'non-s3 pushed data'")
		citools.RunOnNode(t, node1, "dm push test-real-s3 "+fsname)
		output := citools.OutputFromRunOnNode(t, node1, s3cmd+" ls s3://test.dotmesh")
		if !strings.Contains(output, "committedfile.txt") {
			t.Error("Did not push to s3")
		}
	})
}
