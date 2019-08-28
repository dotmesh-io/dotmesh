package fsm

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

func setupTestFiles() (string, error) {
	tDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		return "", err
	}
	for x := 1; x <= 10; x++ {
		dir := fmt.Sprintf("%s/%d", tDir, x)
		os.Mkdir(dir, os.ModePerm)
		for y := 1; y <= 10; y++ {
			file := fmt.Sprintf("%s/%d.txt", dir, y)
			ioutil.WriteFile(file, []byte("X"), os.ModePerm)
		}
	}
	return tDir, nil
}

func TestGetKeysForDirLimitRecursiveLimit3(t *testing.T) {
	tDir, err := setupTestFiles()
	if err != nil {
		t.Fatalf("failed to create dir: %s", err)
	}
	defer os.RemoveAll(tDir)

	listKeysRequest := types.ListFileRequest{
		Base:               tDir,
		Path:               "",
		Limit:              3,
		Page:               0,
		Recursive:          true,
		IncludeDirectories: false,
	}
	listKeysResponse, err := GetKeysForDirLimit(listKeysRequest)
	if err != nil {
		t.Fatalf("failed to get dir limit: %s", err)
	}
	if len(listKeysResponse.Items) != 3 {
		t.Errorf("expected to get 3 files returned, got: %d", len(listKeysResponse.Items))
	}
	if listKeysResponse.TotalCount != 100 {
		t.Errorf("expected total items to be 100: %d", listKeysResponse.TotalCount)
	}
	t.Logf("files: %d, total files count: %d", len(listKeysResponse.Items), listKeysResponse.TotalCount)
}

func TestGetKeysForDirLimitRecursiveLimitNone(t *testing.T) {
	tDir, err := setupTestFiles()
	if err != nil {
		t.Fatalf("failed to create dir: %s", err)
	}
	defer os.RemoveAll(tDir)
	listKeysRequest := types.ListFileRequest{
		Base:               tDir,
		Path:               "",
		Limit:              0,
		Page:               0,
		Recursive:          true,
		IncludeDirectories: false,
	}
	listKeysResponse, err := GetKeysForDirLimit(listKeysRequest)
	if err != nil {
		t.Fatalf("failed to get dir limit: %s", err)
	}
	if len(listKeysResponse.Items) != 100 {
		t.Errorf("expected to get 100 files returned, got: %d", len(listKeysResponse.Items))
	}
	if listKeysResponse.TotalCount != 100 {
		t.Errorf("expected total items to be 100: %d", listKeysResponse.TotalCount)
	}
	t.Logf("files: %d, total files count: %d", len(listKeysResponse.Items), listKeysResponse.TotalCount)
}

func TestGetKeysForDirLimitNotRecursiveLimitNone(t *testing.T) {
	tDir, err := setupTestFiles()
	if err != nil {
		t.Fatalf("failed to create dir: %s", err)
	}
	defer os.RemoveAll(tDir)
	listKeysRequest := types.ListFileRequest{
		Base:               tDir,
		Path:               "",
		Limit:              0,
		Page:               0,
		Recursive:          false,
		IncludeDirectories: true,
	}
	listKeysResponse, err := GetKeysForDirLimit(listKeysRequest)
	if err != nil {
		t.Fatalf("failed to get dir limit: %s", err)
	}
	if len(listKeysResponse.Items) != 10 {
		t.Errorf("expected to get 10 folders returned, got: %d", len(listKeysResponse.Items))
	}
	if listKeysResponse.Items[0].Directory != true {
		t.Errorf("expected first item to be a directory")
	}
	if listKeysResponse.TotalCount != 10 {
		t.Errorf("expected total items to be 10: %d", listKeysResponse.TotalCount)
	}
	t.Logf("files: %d, total files count: %d", len(listKeysResponse.Items), listKeysResponse.TotalCount)
}

func TestGetKeysForDirLimitSubPath(t *testing.T) {
	tDir, err := setupTestFiles()
	if err != nil {
		t.Fatalf("failed to create dir: %s", err)
	}
	//defer os.RemoveAll(tDir)
	listKeysRequest := types.ListFileRequest{
		Base:               tDir,
		Path:               "2",
		Limit:              0,
		Page:               0,
		Recursive:          false,
		IncludeDirectories: false,
	}
	listKeysResponse, err := GetKeysForDirLimit(listKeysRequest)
	if err != nil {
		t.Fatalf("failed to get dir limit: %s", err)
	}
	if len(listKeysResponse.Items) != 10 {
		t.Errorf("expected to get 10 files returned, got: %d", len(listKeysResponse.Items))
	}
	if listKeysResponse.Items[0].Key != "2/1.txt" {
		t.Errorf("expected first item to be 2/1.txt: %s", listKeysResponse.Items[0].Key)
	}
	if listKeysResponse.TotalCount != 10 {
		t.Errorf("expected total items to be 10: %d", listKeysResponse.TotalCount)
	}
	t.Logf("files: %d, total files count: %d", len(listKeysResponse.Items), listKeysResponse.TotalCount)
}

func TestGetKeysForDirLimitSubPathLimit2Page2(t *testing.T) {
	tDir, err := setupTestFiles()
	if err != nil {
		t.Fatalf("failed to create dir: %s", err)
	}
	//defer os.RemoveAll(tDir)
	listKeysRequest := types.ListFileRequest{
		Base:               tDir,
		Path:               "2",
		Limit:              2,
		Page:               2,
		Recursive:          false,
		IncludeDirectories: false,
	}
	listKeysResponse, err := GetKeysForDirLimit(listKeysRequest)
	if err != nil {
		t.Fatalf("failed to get dir limit: %s", err)
	}
	if len(listKeysResponse.Items) != 2 {
		t.Errorf("expected to get 2 files returned, got: %d", len(listKeysResponse.Items))
	}
	if listKeysResponse.Items[0].Key != "2/5.txt" {
		t.Errorf("expected first item to be 2/5.txt: %s", listKeysResponse.Items[0].Key)
	}
	if listKeysResponse.TotalCount != 10 {
		t.Errorf("expected total items to be 10: %d", listKeysResponse.TotalCount)
	}
	t.Logf("files: %d, total files count: %d", len(listKeysResponse.Items), listKeysResponse.TotalCount)
}
