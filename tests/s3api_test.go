package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/archiver"
	"github.com/dotmesh-io/dotmesh/pkg/client"

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
		t.Fatalf("failed to start cluster, error: %s", err)
	}
	host := f[0].GetNode(0)
	node1 := host.Container
	dmClient := client.NewJsonRpcClient("admin", host.IP, host.Password, 0)
	dm := client.NewDotmeshAPIFromClient(dmClient, true)
	err = citools.RegisterUser(host, "bob", "bob@bob.com", "password")
	if err != nil {
		t.Errorf("failed to register user: %s", err)
	}

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
		if !strings.Contains(resp, "User bob is not the administrator of namespace admin") {
			t.Errorf("Expected 'User is not the administrator of namespace admin', got: '%s'", resp)
		}
	})
	t.Run("List", func(t *testing.T) {
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)
		cmd := fmt.Sprintf("curl -T newfile.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/newfile", host.Password, dotName)
		citools.RunOnNode(t, node1, "echo helloworld > newfile.txt")
		citools.RunOnNode(t, node1, cmd)

		req, err := http.NewRequest("GET", "http://"+host.IP+":32607/s3/admin:"+dotName, nil)
		if err != nil {
			t.Fatalf("failed to create request: %s", err)
			return
		}
		req.SetBasicAuth("admin", host.Password)

		resp, err := http.DefaultClient.Do(req)
		if resp.Body != nil {
			defer resp.Body.Close()
		}
		if err != nil {
			t.Errorf("failed to make S3 API list request: %s", err)

			bts, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("failed to read req body: %s", err)
				return
			}
			t.Logf("response body: %s", string(bts))
			return
		}

		bts, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Errorf("failed to read req body: %s", err)
			return
		}

		t.Logf("status code: %d", resp.StatusCode)
		t.Logf("response body: %s", string(bts))

		if resp.StatusCode != 200 {
			t.Errorf("unexpected status code: %d (wanted 200)", resp.StatusCode)
		}

		if !strings.Contains(string(bts), "newfile") {
			t.Errorf("wanted to fine 'newfile' in response but didn't. Response: \n %s \n", string(bts))
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
		commits, err := dm.ListCommits(fmt.Sprintf("admin/%s", dotName), "")
		if err != nil {
			t.Fatal(err)
		}
		if len(commits) < 2 {
			t.Errorf("expected to find at least 2 commits, got: %d", len(commits))
			return
		}

		firstCommitId := commits[1].Id
		secondCommitId := commits[2].Id

		respFirstCommit := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s/snapshot/%s", host.Password, dotName, firstCommitId))
		if !strings.Contains(respFirstCommit, "file1.txt") {
			t.Errorf("The first commit did not contain the first file, resp: %s", respFirstCommit)
		}
		if strings.Contains(respFirstCommit, "file2.txt") {
			t.Errorf("The first commit contained the second file, resp: %s", respFirstCommit)
		}

		respSecondCommit := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s/snapshot/%s", host.Password, dotName, secondCommitId))
		if !strings.Contains(respSecondCommit, "file2.txt") {
			t.Errorf("The second commit did not contain the second file, resp: %s", respSecondCommit)
		}
	})

	t.Run("ReadFileAtSnapshotThenDelete", func(t *testing.T) { // FIX
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)
		cmdFile1 := fmt.Sprintf("curl -T file.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/file.txt", host.Password, dotName)
		citools.RunOnNode(t, node1, "echo helloworld1 > file.txt")
		citools.RunOnNode(t, node1, cmdFile1)
		cmdFile2 := fmt.Sprintf("curl -T file.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/file.txt", host.Password, dotName)
		citools.RunOnNode(t, node1, "echo helloworld2 > file.txt")
		citools.RunOnNode(t, node1, cmdFile2)

		commits, err := dm.ListCommits(fmt.Sprintf("admin/%s", dotName), "")

		if err != nil {
			t.Error(err.Error())
		}
		// first commit (index 0) is always an "init" commit now
		firstCommitId := commits[1].Id
		secondCommitId := commits[2].Id

		t.Logf("running (first commit): '%s'", fmt.Sprintf("http://127.0.0.1:32607/s3/admin:%s/snapshot/%s/file.txt", dotName, firstCommitId))

		respBody, status, err := callWithRetries("GET", fmt.Sprintf("/s3/admin:%s/snapshot/%s/file.txt", dotName, firstCommitId), host, nil)
		if err != nil {
			t.Errorf("S3 request failed, error: %s", err)
		}
		if status != 200 {
			t.Errorf("unexpected status code: %d", status)
		}
		// respFirstCommit := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s/snapshot/%s/file.txt", host.Password, dotName, firstCommitId))
		expected1 := "helloworld1"
		if !strings.Contains(respBody, expected1) {
			t.Errorf("The first commit did not contain the correct file data (expected '%s', got: '%s')", expected1, respBody)
		}

		t.Logf("running (second commit): '%s'", fmt.Sprintf("http://127.0.0.1:32607/s3/admin:%s/snapshot/%s/file.txt", dotName, secondCommitId))
		respBodySecond, statusSecond, err := callWithRetries("GET", fmt.Sprintf("/s3/admin:%s/snapshot/%s/file.txt", dotName, secondCommitId), host, nil)
		if err != nil {
			t.Errorf("S3 request failed, error: %s", err)
		}
		if statusSecond != 200 {
			t.Errorf("unexpected status code: %d", status)
		}

		expected2 := "helloworld2"
		if !strings.Contains(respBodySecond, expected2) {
			t.Errorf("The second commit did not contain the correct file data (expected '%s', got: '%s')", expected2, respBodySecond)
		}

		_, statusSecondHead, err := callWithRetries("HEAD", fmt.Sprintf("/s3/admin:%s/snapshot/%s/file.txt", dotName, secondCommitId), host, nil)
		if err != nil {
			t.Errorf("S3 HEAD request failed, error: %s", err)
		}
		if statusSecondHead != 200 {
			t.Errorf("unexpected HEAD status code: %d", statusSecondHead)
		}

		t.Logf("running (nonexistant file): '%s'", fmt.Sprintf("http://127.0.0.1:32607/s3/admin:%s/snapshot/%s/nonexistant.txt", dotName, secondCommitId))
		_, statusThird, err := callWithRetries("GET", fmt.Sprintf("/s3/admin:%s/snapshot/%s/nonexistant.txt", dotName, secondCommitId), host, nil)
		if err != nil {
			t.Errorf("S3 request failed, error: %s", err)
		}
		if statusThird != 404 {
			t.Errorf("unexpected status code: %d", status)
		}

		_, statusThirdHead, err := callWithRetries("HEAD", fmt.Sprintf("/s3/admin:%s/snapshot/%s/nonexistant.txt", dotName, secondCommitId), host, nil)
		if err != nil {
			t.Errorf("S3 HEAD request failed, error: %s", err)
		}
		if statusThirdHead != 404 {
			t.Errorf("unexpected HEAD status code: %d", statusThirdHead)
		}

		// Next, we delete one of the files, it should now 404:
		respBody, status, err = callWithRetries("DELETE", fmt.Sprintf("/s3/admin:%s/file.txt", dotName), host, nil)
		if err != nil {
			t.Errorf("S3 request failed, error: %s", err)
		}
		if status != 200 {
			t.Errorf("unexpected status code: %d", status)
		}
		// Figure out new snapshot commit:
		commits, err = dm.ListCommits(fmt.Sprintf("admin/%s", dotName), "")
		if err != nil {
			t.Error(err.Error())
		}
		deleteCommitId := commits[3].Id

		respBody, status, err = callWithRetries("GET", fmt.Sprintf("/s3/admin:%s/snapshot/%s/file.txt", dotName, deleteCommitId), host, nil)
		if err != nil {
			t.Errorf("S3 request failed, error: %s", err)
		}
		if status != 404 {
			t.Errorf("unexpected status code: %d", status)
		}

		// Unknown file deletion:
		_, statusUnknownDelete, err := callWithRetries("DELETE", fmt.Sprintf("/s3/admin:%s/nonexistant.txt", dotName), host, nil)
		if err != nil {
			t.Errorf("S3 DELETE request failed, error: %s", err)
		}
		if statusUnknownDelete != 404 {
			t.Errorf("unexpected DELETE status code: %d", statusUnknownDelete)
		}

	})

	t.Run("CheckSnapshot", func(t *testing.T) {
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)

		port := 32607
		if host.Port != 0 {
			port = host.Port
		}

		req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/s3/admin:%s/uploaded.txt", host.IP, port, dotName), bytes.NewBufferString("contentz"))
		if err != nil {
			t.Errorf("failed to create req: %s", err)
			return
		}
		req.SetBasicAuth("admin", host.Password)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Errorf("S3 Upload request failed: %s", err)
			return
		}
		if resp.StatusCode != 200 {
			t.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		defer resp.Body.Close()
		snapshotIDHeader := resp.Header.Get("Snapshot")
		t.Logf("snapshotID from header: %s", snapshotIDHeader)

		t.Log("headers:")
		for k, vv := range resp.Header {
			t.Logf("%s: [%s]", k, strings.Join(vv, ", "))
		}

		// wait for the snapshot to propage
		time.Sleep(3 * time.Second)

		commits, err := dm.ListCommits(fmt.Sprintf("admin/%s", dotName), "")

		if err != nil {
			t.Error(err.Error())
		}

		if len(commits) != 2 {
			t.Errorf("expected to find 2 commit, got: %d commits", len(commits))
			return
		}
		// first commit (index 0) is always an "init" commit now
		firstCommitId := commits[1].Id

		if firstCommitId != snapshotIDHeader {
			t.Errorf("snapshot IDs don't match, header: %s, from dm list: %s", snapshotIDHeader, firstCommitId)
		}
	})

	t.Run("TestUploadTarAndExtract", func(t *testing.T) {
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)

		port := 32607
		if host.Port != 0 {
			port = host.Port
		}

		f, err := os.Open("./testdata/archived-test-file_tar")
		if err != nil {
			t.Fatalf("failed to open test file: %s", err)
		}

		req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/s3/admin:%s/mydata", host.IP, port, dotName), f)
		if err != nil {
			t.Errorf("failed to create req: %s", err)
			return
		}
		// setting Extract header to inform dotmesh that it should extract
		// tar file contents into the specified directory
		req.Header.Set("Extract", "true")
		// auth
		req.SetBasicAuth("admin", host.Password)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Errorf("S3 Upload request failed: %s", err)
			return
		}
		if resp.StatusCode != 200 {
			t.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		defer resp.Body.Close()
		snapshotIDHeader := resp.Header.Get("Snapshot")
		t.Logf("snapshotID from header: %s", snapshotIDHeader)

		respBody, status, err := callWithRetries("GET", fmt.Sprintf("/s3/admin:%s/snapshot/%s/mydata/test-file.txt", dotName, snapshotIDHeader), host, nil)
		if err != nil {
			t.Errorf("S3 request failed, error: %s", err)
		}
		if status != 200 {
			t.Errorf("unexpected status code: %d", status)
		}

		expected2 := "some-things-here"
		if !strings.Contains(respBody, expected2) {
			t.Errorf("The commit did not contain the correct file data (expected '%s', got: '%s')", expected2, respBody)
		}

	})

	t.Run("TestUploadAndOverwriteTarAndExtract", func(t *testing.T) {
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)

		port := 32607
		if host.Port != 0 {
			port = host.Port
		}

		uploadFile := func(file string) (string, error) {

			f, err := os.Open(file)
			if err != nil {
				t.Fatalf("failed to open test file: %s", err)
				return "", err
			}
			defer f.Close()

			req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/s3/admin:%s/mydata", host.IP, port, dotName), f)
			if err != nil {
				t.Errorf("failed to create req: %s", err)
				return "", err
			}
			// setting Extract header to inform dotmesh that it should extract
			// tar file contents into the specified directory
			req.Header.Set("Extract", "true")
			// auth
			req.SetBasicAuth("admin", host.Password)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("S3 Upload request failed: %s", err)
				return "", err
			}
			if resp.StatusCode != 200 {
				t.Errorf("unexpected status code: %d", resp.StatusCode)
				return "", err
			}

			defer resp.Body.Close()
			snapshotIDHeader := resp.Header.Get("Snapshot")
			t.Logf("snapshotID from header: %s", snapshotIDHeader)
			return snapshotIDHeader, nil
		}

		_, err = uploadFile("./testdata/archived-test-file_tar")
		if err != nil {
			t.Errorf("upload failed for the first file: %s", err)
			return
		}

		snapshotIDHeader, err := uploadFile("./testdata/archived-second-file_tar")
		if err != nil {
			t.Errorf("upload failed for second file: %s", err)
			return
		}

		respBody, status, err := callWithRetries("GET", fmt.Sprintf("/s3/admin:%s/snapshot/%s/mydata/second-file.txt", dotName, snapshotIDHeader), host, nil)
		if err != nil {
			t.Errorf("S3 request failed, error: %s", err)
		}
		if status != 200 {
			t.Errorf("unexpected status code: %d", status)
		}

		expected2 := "second-thing-here"
		if !strings.Contains(respBody, expected2) {
			t.Errorf("The commit did not contain the correct file data (expected '%s', got: '%s')", expected2, respBody)
		}

	})

	t.Run("ReadFileAtSnapshotEmpty", func(t *testing.T) { // FIX
		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)
		cmdFile1 := fmt.Sprintf("curl -T file.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/file.txt", host.Password, dotName)
		citools.RunOnNode(t, node1, "touch file.txt")
		citools.RunOnNode(t, node1, cmdFile1)
		cmdFile2 := fmt.Sprintf("curl -T file.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/file.txt", host.Password, dotName)
		citools.RunOnNode(t, node1, "touch file.txt")
		citools.RunOnNode(t, node1, cmdFile2)

		commits, err := dm.ListCommits(fmt.Sprintf("admin/%s", dotName), "")
		if err != nil {
			t.Errorf(err.Error())
		}
		firstCommitId := commits[1].Id
		secondCommitId := commits[2].Id

		t.Logf("running (first commit): '%s'", fmt.Sprintf("http://127.0.0.1:32607/s3/admin:%s/snapshot/%s/file.txt", dotName, firstCommitId))

		respBody, status, err := callWithRetries("GET", fmt.Sprintf("/s3/admin:%s/snapshot/%s/file.txt", dotName, firstCommitId), host, nil)
		if err != nil {
			t.Errorf("S3 request failed, error: %s", err)
		}
		if status != 200 {
			t.Errorf("unexpected status code: %d", status)
		}
		// respFirstCommit := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s/snapshot/%s/file.txt", host.Password, dotName, firstCommitId))
		expected1 := ""
		if !strings.Contains(respBody, expected1) {
			t.Errorf("The first commit did not contain the correct file data (expected '%s', got: '%s')", expected1, respBody)
		}

		t.Logf("running (second commit): '%s'", fmt.Sprintf("http://127.0.0.1:32607/s3/admin:%s/snapshot/%s/file.txt", dotName, secondCommitId))
		respBodySecond, statusSecond, err := callWithRetries("GET", fmt.Sprintf("/s3/admin:%s/snapshot/%s/file.txt", dotName, secondCommitId), host, nil)
		if err != nil {
			t.Errorf("S3 request failed, error: %s", err)
		}
		if statusSecond != 200 {
			t.Errorf("unexpected status code: %d", status)
		}

		expected2 := ""
		if !strings.Contains(respBodySecond, expected2) {
			t.Errorf("The second commit did not contain the correct file data (expected '%s', got: '%s')", expected2, respBodySecond)
		}
	})

	t.Run("ReadDirectoryAtSnapshot", func(t *testing.T) {

		tempDir, err := ioutil.TempDir("", "test-ReadDirectoryAtSnapshot")
		if err != nil {
			t.Fatalf("failed to create temp dir for file write: %s", err)
		}
		defer os.RemoveAll(tempDir)

		dotName := citools.UniqName()
		citools.RunOnNode(t, node1, "dm init "+dotName)

		citools.RunOnNode(t, node1, "echo helloworld1 > file.txt")

		cmdFile1 := fmt.Sprintf("curl -T file.txt -u admin:%s 127.0.0.1:32607/s3/admin:%s/subpath/file.txt", host.Password, dotName)
		citools.RunOnNode(t, node1, cmdFile1)

		commits, err := dm.ListCommits(fmt.Sprintf("admin/%s", dotName), "")
		if err != nil {
			t.Errorf(err.Error())
		}
		firstCommitId := commits[1].Id

		// t.Logf("running (first commit): '%s'", fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s/snapshot/%s/subpath", host.Password, dotName, firstCommitId))
		// respFirstCommit := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s/snapshot/%s/subpath", host.Password, dotName, firstCommitId))
		path := fmt.Sprintf("/s3/admin:%s/snapshot/%s/subpath", dotName, firstCommitId)
		s3Endpoint := fmt.Sprintf("http://%s:32607/%s", host.IP, path)

		// HEAD it

		_, status, err := callWithRetries("HEAD", path, host, nil)
		if err != nil {
			t.Errorf("S3 HEAD request failed, error: %s", err)
		}
		if status != 200 {
			t.Errorf("unexpected status code from HEAD: %d", status)
		}

		// Now GET

		req, _ := http.NewRequest("GET", s3Endpoint, nil)

		req.SetBasicAuth("admin", host.Password)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("failed to query S3 endpoint '%s', error: %s", s3Endpoint, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("expected status code: %d, got: %d", 200, resp.StatusCode)
		}

		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("failed to read response body: %s", err)
		}
		tarPath := filepath.Join(tempDir, "out.tar")
		err = ioutil.WriteFile(tarPath, []byte(respBody), os.ModeDir)
		if err != nil {
			t.Fatalf("failed to write file: %s", err)
		}

		outDir, err := ioutil.TempDir("", "test-ReadDirectoryAtSnapshot-outdir")
		if err != nil {
			t.Fatalf("Making temporary out directory: %v", err)
		}
		defer os.RemoveAll(outDir)
		os.MkdirAll(outDir, os.ModePerm)

		err = archiver.NewTar().Unarchive(tarPath, outDir)
		if err != nil {
			t.Fatalf("failed to untar: %s", err)
		}

		bts, err := ioutil.ReadFile(filepath.Join(outDir, "subpath", "file.txt"))
		if err != nil {

			files, rErr := OSReadDir(outDir)
			if err != nil {
				t.Errorf("failed to read out dir '%s', error: %s", outDir, rErr)
			} else {
				t.Fatalf("failed to read untarred file: %s, available file: %s", err, strings.Join(files, ", "))
			}

		}
		if !strings.Contains(string(bts), "helloworld1") {
			t.Errorf("expected file contents 'helloworld1', got: '%s'", string(bts))
		}

		s3Endpoint2 := fmt.Sprintf("/s3/admin:%s/snapshot/%s/nonexistant", dotName, firstCommitId)

		t.Logf("running (nonexistant directory): '%s'", s3Endpoint2)
		_, status, err = callWithRetries("GET", s3Endpoint2, host, nil)
		if err != nil {
			t.Errorf("S3 request failed, error: %s", err)
		}
		if status != 404 {
			t.Errorf("unexpected status code from GET: %d", status)
		}

		_, status, err = callWithRetries("HEAD", s3Endpoint2, host, nil)
		if err != nil {
			t.Errorf("S3 request failed, error: %s", err)
		}
		if status != 404 {
			t.Errorf("unexpected status code from HEAD: %d", status)
		}
	})
}

func OSReadDir(root string) ([]string, error) {
	var files []string
	f, err := os.Open(root)
	if err != nil {
		return files, err
	}
	fileInfo, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		return files, err
	}

	for _, file := range fileInfo {
		files = append(files, file.Name())
	}
	return files, nil
}

func callWithRetries(method string, path string, node citools.Node, body io.Reader) (respBody string, statusCode int, err error) {
	for retries := 10; retries > 0; retries-- {
		if node.Port == 0 {
			node.Port = 32607
		}

		url := fmt.Sprintf("http://%s:%d%s", node.IP, node.Port, path)

		req, err := http.NewRequest(method, url, body)
		if err != nil {
			return err.Error(), 0, err
		}
		req.SetBasicAuth("admin", node.Password)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err.Error(), 0, err
		}

		defer resp.Body.Close()

		bts, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err.Error(), 0, err
		}

		if resp.StatusCode == http.StatusServiceUnavailable {
			fmt.Printf("Got a %d return, retrying...\n", resp.StatusCode)
			time.Sleep(time.Second)
			continue
		}

		return string(bts), resp.StatusCode, nil
	}

	return "Gave up retrying", 0, fmt.Errorf("Gave up retrying")
}
