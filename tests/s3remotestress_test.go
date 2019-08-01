package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dotmesh-io/citools"
)

// 100000 1KiB files plus one 16 GiB file
const TotalFileSize = (100000 * 1024 * 1024) + (16 * 1024 * 1024 * 1024 * 1024)

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
		start := time.Now()
		fmt.Printf("Timing the clone command...\n")
		command := "dm clone test-real-s3 dotmesh-transfer-stress-test --local-name=" + fsname
		t.Fatalf("Failing here, run: %s", command)
		citools.RunOnNode(t, node1, command)
		elapsed := time.Since(start)
		fmt.Printf("Clone took: %s", elapsed)
		if elapsed.Minutes() > 20 {
			t.Errorf("It took longer than 20 minutes to download the bucket. Total elapsed: %s\n", elapsed)
		}
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

		citools.RunOnNode(t, node1, "dm switch "+fsname)
		output := citools.OutputFromRunOnNode(t, node1, "dm dot show")
		fmt.Printf(output)
	})
}
