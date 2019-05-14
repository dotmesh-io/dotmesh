package fsm

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestgetKeysForDirLimit3(t *testing.T) {
	tDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		t.Fatalf("failed to create dir: %s", err)
	}
	defer os.RemoveAll(tDir)

	ioutil.WriteFile(filepath.Join(tDir, "root-1.txt"), []byte("X"), os.ModePerm)
	ioutil.WriteFile(filepath.Join(tDir, "root-2.txt"), []byte("X"), os.ModePerm)
	ioutil.WriteFile(filepath.Join(tDir, "root-3.txt"), []byte("X"), os.ModePerm)
	ioutil.WriteFile(filepath.Join(tDir, "root-4.txt"), []byte("X"), os.ModePerm)
	ioutil.WriteFile(filepath.Join(tDir, "/dir/level-1-1.txt"), []byte("X"), os.ModePerm)
	ioutil.WriteFile(filepath.Join(tDir, "/dir/level-1-2.txt"), []byte("X"), os.ModePerm)
	ioutil.WriteFile(filepath.Join(tDir, "/dir/level-1-3.txt"), []byte("X"), os.ModePerm)

	files, dirFilesCount, size, err := GetKeysForDirLimit(tDir, "/", 3)
	if err != nil {
		t.Fatalf("failed to get dir limit: %s", err)
	}
	if len(files) != 3 {
		t.Errorf("expected to get 3 files returned, got: %d", len(files))
	}
	t.Logf("files: %d, files count: %d, size: %d", len(files), dirFilesCount, size)
}

func TestgetKeysForDirLimitNoLimit(t *testing.T) {
	tDir, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		t.Fatalf("failed to create dir: %s", err)
	}
	defer os.RemoveAll(tDir)

	ioutil.WriteFile(filepath.Join(tDir, "root-1.txt"), []byte("X"), os.ModePerm)
	ioutil.WriteFile(filepath.Join(tDir, "root-2.txt"), []byte("X"), os.ModePerm)
	ioutil.WriteFile(filepath.Join(tDir, "root-3.txt"), []byte("X"), os.ModePerm)
	ioutil.WriteFile(filepath.Join(tDir, "root-4.txt"), []byte("X"), os.ModePerm)
	ioutil.WriteFile(filepath.Join(tDir, "/dir/level-1-1.txt"), []byte("X"), os.ModePerm)
	ioutil.WriteFile(filepath.Join(tDir, "/dir/level-1-2.txt"), []byte("X"), os.ModePerm)
	ioutil.WriteFile(filepath.Join(tDir, "/dir/level-1-3.txt"), []byte("X"), os.ModePerm)

	files, dirFilesCount, size, err := GetKeysForDirLimit(tDir, "/", 0)
	if err != nil {
		t.Fatalf("failed to get dir limit: %s", err)
	}
	if len(files) != 7 {
		t.Errorf("expected to get 7 files returned, got: %d", len(files))
	}
	t.Logf("files: %d, files count: %d, size: %d", len(files), dirFilesCount, size)
}
