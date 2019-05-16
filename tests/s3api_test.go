package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/archiver"

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

		commitIdsString := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("dm log | grep commit | awk '{print $2}'"))
		commitIdsList := strings.Split(commitIdsString, "\n")

		if len(commitIdsList) < 2 {
			t.Errorf("expected to find at least 2 commits, got: %d", len(commitIdsList))
			return
		}

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

	t.Run("ReadFileAtSnapshot", func(t *testing.T) { // FIX
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

		t.Logf("running (first commit): '%s'", fmt.Sprintf("http://127.0.0.1:32607/s3/admin:%s/snapshot/%s/file.txt", dotName, firstCommitId))

		respBody, status, err := call("GET", fmt.Sprintf("/s3/admin:%s/snapshot/%s/file.txt", dotName, firstCommitId), host, nil)
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
		respBodySecond, statusSecond, err := call("GET", fmt.Sprintf("/s3/admin:%s/snapshot/%s/file.txt", dotName, secondCommitId), host, nil)
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

		commitIdsString := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("dm log | grep commit | awk '{print $2}'"))
		commitIdsList := strings.Split(commitIdsString, "\n")

		firstCommitId := commitIdsList[0]

		// t.Logf("running (first commit): '%s'", fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s/snapshot/%s/subpath", host.Password, dotName, firstCommitId))
		// respFirstCommit := citools.OutputFromRunOnNode(t, node1, fmt.Sprintf("curl -u admin:%s 127.0.0.1:32607/s3/admin:%s/snapshot/%s/subpath", host.Password, dotName, firstCommitId))
		s3Endpoint := fmt.Sprintf("http://%s:32607/s3/admin:%s/snapshot/%s/subpath", host.IP, dotName, firstCommitId)
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

func call(method string, path string, node citools.Node, body io.Reader) (respBody string, statusCode int, err error) {
	req, err := http.NewRequest(method, "http://"+node.IP+":32607/"+path, body)
	if err != nil {
		return
	}
	req.SetBasicAuth("admin", node.Password)

	resp, err := http.DefaultClient.Do(req)
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		bts, newErr := ioutil.ReadAll(resp.Body)
		if newErr != nil {
			return
		}
		return string(bts), resp.StatusCode, err
	}

	bts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	return string(bts), resp.StatusCode, nil
}
