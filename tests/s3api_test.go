package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dotmesh-io/citools"
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
	err = citools.RegisterUser(host, "bob", "bob@bob.com", "password")

	t.Run("Put", func(t *testing.T) {
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)
		cmd := fmt.Sprintf("curl -T newfile.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/newfile", host.Password, dotName)
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
		resp = citools.OutputFromRunOnNode(t, node1, "dm log")
		if !strings.Contains(resp, "author: admin") {
			t.Error("Did not set author correctly")
		}
	})

	t.Run("PutDotDoesntExist", func(t *testing.T) {
		dotName := citools.UniqName()
		cmd := fmt.Sprintf("curl -T newfile.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/newfile", host.Password, dotName)
		citools.RunOnNode(t, node1, "echo helloworld > newfile.txt")
		resp := citools.OutputFromRunOnNode(t, node1, cmd)
		if !strings.Contains(resp, fmt.Sprintf("Bucket admin-%s does not exist", dotName)) {
			t.Errorf("Expected '%s', got '%s'", fmt.Sprintf("Bucket admin-%s does not exist", dotName), resp)
		}
	})

	t.Run("PutUsernameMismatch", func(t *testing.T) {
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)
		cmd := fmt.Sprintf("curl -T newfile.txt -u bob:password 127.0.0.1:32607/s3/admin:%s/newfile", dotName)
		citools.RunOnNode(t, node1, "echo helloworld > newfile.txt")
		resp := citools.OutputFromRunOnNode(t, node1, cmd)
		if !strings.Contains(resp, "User is not the administrator of namespace admin") {
			t.Errorf("Expected 'User is not the administrator of namespace admin', got: '%s'", resp)
		}
	})
	t.Run("List", func(t *testing.T) {
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)
		cmd := fmt.Sprintf("curl -T newfile.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/newfile", host.Password, dotName)
		citools.RunOnNode(t, node1, "echo helloworld > newfile.txt")
		citools.RunOnNode(t, node1, cmd)
		resp := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s", host.Password, dotName))
		if !strings.Contains(resp, "newfile") {
			fmt.Printf(resp)
			t.Error("failed to include file")
		}
	})

	t.Run("ListSnapshot", func(t *testing.T) {
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)
		cmdFile1 := fmt.Sprintf("curl -T file1.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/file1.txt", host.Password, dotName)
		citools.RunOnNode(t, node1, "echo helloworld1 > file1.txt")
		citools.RunOnNode(t, node1, cmdFile1)
		cmdFile2 := fmt.Sprintf("curl -T file2.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/file2.txt", host.Password, dotName)
		citools.RunOnNode(t, node1, "echo helloworld2 > file2.txt")
		citools.RunOnNode(t, node1, cmdFile2)

		commitIdsString := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("dm log | grep commit | awk '{print $2}'"))
		commitIdsList := strings.Split(commitIdsString, "\n")

		firstCommitId := commitIdsList[0]
		secondCommitId := commitIdsList[1]

		respFirstCommit := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s/snapshot/%s", host.Password, dotName, firstCommitId))
		if !strings.Contains(respFirstCommit, "file1.txt") {
			fmt.Printf(respFirstCommit)
			t.Error("The first commit did not contain the first file")
		}
		if strings.Contains(respFirstCommit, "file2.txt") {
			fmt.Printf(respFirstCommit)
			t.Error("The first commit contained the second file")
		}

		respSecondCommit := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s/snapshot/%s", host.Password, dotName, secondCommitId))
		if !strings.Contains(respSecondCommit, "file2.txt") {
			fmt.Printf(respSecondCommit)
			t.Error("The second commit did not contain the second file")
		}
	})

	t.Run("ReadFileAtSnapshot", func(t *testing.T) {
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)
		cmdFile1 := fmt.Sprintf("curl -T file.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/file.txt", host.Password, dotName)
		citools.RunOnNode(t, node1, "echo helloworld1 > file.txt")
		citools.RunOnNode(t, node1, cmdFile1)
		cmdFile2 := fmt.Sprintf("curl -T file.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/file.txt", host.Password, dotName)
		citools.RunOnNode(t, node1, "echo helloworld2 > file.txt")
		citools.RunOnNode(t, node1, cmdFile2)

		commitIdsString := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("dm log | grep commit | awk '{print $2}'"))
		commitIdsList := strings.Split(commitIdsString, "\n")

		firstCommitId := commitIdsList[0]
		secondCommitId := commitIdsList[1]

		respFirstCommit := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s/snapshot/%s/file.txt", host.Password, dotName, firstCommitId))
		if !strings.Contains(respFirstCommit, "helloworld1") {
			fmt.Printf(respFirstCommit)
			t.Error("The first commit did not contain the correct file data")
		}
		respSecondCommit := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s/snapshot/%s/file.txt", host.Password, dotName, secondCommitId))
		if !strings.Contains(respSecondCommit, "helloworld2") {
			fmt.Printf(respSecondCommit)
			t.Error("The second commit did not contain the correct file data")
		}
	})
}
