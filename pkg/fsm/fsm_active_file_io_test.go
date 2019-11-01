package fsm

import (
	"crypto/rand"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/archiver"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	log "github.com/sirupsen/logrus"
)

func createTestFile(testDir, filePath string, fileContent []byte) (err error) {
	if strings.Contains(filePath, "/") {
		// Need to create subfolder(s) before creating file
		if e := os.MkdirAll(path.Join(testDir, path.Dir(filePath)), 0777); e != nil {
			err = e
			return
		}
	}

	file, err := os.Create(path.Join(testDir, filePath))
	if err != nil {
		return
	}

	_, err = file.Write(fileContent)
	return
}

func createRandomBytes(t *testing.T) (bytes []byte) {
	bytes = make([]byte, 100)
	_, err := rand.Read(bytes)
	if err != nil {
		if err != nil {
			t.Fatalf("Making random byte array: %v", err)
		}
	}
	return
}

func TestFsmActiveReadDir(t *testing.T) {

	inputDir, err := ioutil.TempDir("", "inputFolder")
	if err != nil {
		t.Fatalf("making temporary directory: %v", err)
	}
	defer os.RemoveAll(inputDir)

	fileContent1 := createRandomBytes(t)
	filePath1 := "__default__/file1"

	fileContent2 := createRandomBytes(t)
	filePath2 := "__default__/subpath/file2"

	fileContent3 := createRandomBytes(t)
	filePath3 := "__default__/subpath2/subpath3/file3"

	for _, td := range []struct {
		filePath string
		fileData []byte
	}{
		{filePath1, fileContent1},
		{filePath2, fileContent2},
		{filePath3, fileContent3},
	} {
		if err := createTestFile(inputDir, td.filePath, td.fileData); err != nil {
			t.Fatalf("Making temporary file %s: %v", td.filePath, err)
		}
	}

	t.Run("Create a tar using readDirectory function directly", func(t *testing.T) {
		fsm := &FsMachine{}

		respCh := make(chan *types.Event)

		outputDir, err := ioutil.TempDir("", "outputFolder")
		if err != nil {
			t.Fatalf("Making temporary directory: %v", err)
		}
		defer os.RemoveAll(outputDir)

		tarFileName := filepath.Join(outputDir, "output.tar")
		f, err := os.Create(tarFileName)
		if err != nil {
			t.Fatalf("failed to create output.tar file: %s", err)
		}

		file := &types.OutputFile{
			SnapshotMountPath: inputDir,
			Filename:          "",
			Response:          respCh,
			Contents:          f,
		}

		go fsm.readDirectory(file)

		response := <-respCh

		if response.Name != types.EventNameReadSuccess {
			t.Fatalf("expected %s, got: %s, error: %s", types.EventNameReadSuccess, response.Name, response.Error())
		}

		testDir, err := ioutil.TempDir("", "outputFolderFromStream")
		if err != nil {
			t.Fatalf("Making temporary directory: %v", err)
		}
		defer os.RemoveAll(testDir)

		if err := archiver.NewTar().Unarchive(tarFileName, testDir); err != nil {
			t.Fatalf("Failed to untar archive: %s", err)
		}

		if output, err := exec.Command("diff", "-rq", inputDir, testDir).Output(); err != nil {
			t.Fatalf("Folders are different! \n%s", string(output))
		}
	})

	t.Run("Create a tar using readFile function, it should switch to directory mode", func(t *testing.T) {
		fsm := &FsMachine{}

		respCh := make(chan *types.Event)

		outputDir, err := ioutil.TempDir("", "outputFolder")
		if err != nil {
			t.Fatalf("Making temporary directory: %v", err)
		}
		defer os.RemoveAll(outputDir)

		tarFileName := filepath.Join(outputDir, "output.tar")
		f, err := os.Create(tarFileName)
		if err != nil {
			t.Fatalf("failed to create output.tar file: %s", err)
		}

		file := &types.OutputFile{
			SnapshotMountPath: inputDir,
			Filename:          "",
			Response:          respCh,
			Contents:          f,
		}

		go fsm.readFile(file)

		response := <-respCh

		if response.Name != types.EventNameReadSuccess {
			t.Fatalf("expected %s, got: %s, error: %s", types.EventNameReadSuccess, response.Name, response.Error())
		}

		testDir, err := ioutil.TempDir("", "outputFolderFromStream")
		if err != nil {
			t.Fatalf("Making temporary directory: %v", err)
		}
		defer os.RemoveAll(testDir)

		if err := archiver.NewTar().Unarchive(tarFileName, testDir); err != nil {
			t.Fatalf("Failed to untar archive: %s", err)
		}

		if output, err := exec.Command("diff", "-rq", inputDir, testDir).Output(); err != nil {
			t.Fatalf("Folders are different! \n%s", string(output))
		}
	})

}

func TestFsmActiveWriteContents(t *testing.T) {

	outputDir, err := ioutil.TempDir("", "outputDir")
	if err != nil {
		t.Fatalf("Making temporary directory: %v", err)
	}
	defer os.RemoveAll(outputDir)

	f, err := os.Open("./testdata/test-file.txt")
	if err != nil {
		t.Fatalf("failed to open test file: %s", err)
	}

	file := &types.InputFile{
		Contents: f,
	}

	l := log.WithFields(log.Fields{
		"filename": file.Filename,
		"destPath": outputDir,
	})

	_, err = writeContents(l, file, outputDir+"/file.txt")
	if err != nil {
		t.Fatalf("failed to write contents: %s", err)
	}

	contents, err := ioutil.ReadFile(outputDir + "/file.txt")
	if err != nil {
		t.Errorf("failed to read saved contents: %s", err)
		return
	}
	if strings.TrimSpace(string(contents)) != "some contents here" {
		t.Errorf("expected 'some contents here', got: %s", strings.TrimSpace(string(contents)))
	}
}

func TestFsmActiveWriteDirectoryContents(t *testing.T) {

	outputDir, err := ioutil.TempDir("", "outputDir")
	if err != nil {
		t.Fatalf("Making temporary directory: %v", err)
	}
	defer os.RemoveAll(outputDir)

	f, err := os.Open("./testdata/archived-test-file_tar")
	if err != nil {
		t.Fatalf("failed to open test file: %s", err)
	}

	file := &types.InputFile{
		Contents: f,
		Extract:  true,
	}

	l := log.WithFields(log.Fields{
		"destPath": outputDir,
	})

	_, err = writeContents(l, file, outputDir+"/extracted")
	if err != nil {
		t.Fatalf("failed to write contents: %s", err)
	}

	contents, err := ioutil.ReadFile(outputDir + "/extracted/test-file.txt")
	if err != nil {
		t.Errorf("failed to read saved contents: %s", err)
		return
	}
	if strings.TrimSpace(string(contents)) != "some contents here" {
		t.Errorf("expected 'some contents here', got: %s", strings.TrimSpace(string(contents)))
	}
}

func TestFsmActiveOverwriteDirectoryContents(t *testing.T) {

	outputDir, err := ioutil.TempDir("", "outputDir")
	if err != nil {
		t.Fatalf("Making temporary directory: %v", err)
	}
	defer os.RemoveAll(outputDir)

	t.Logf("output dir: '%s'", outputDir)

	f, err := os.Open("./testdata/archived-test-file_tar")
	if err != nil {
		t.Fatalf("failed to open test file: %s", err)
	}
	file := &types.InputFile{
		Contents: f,
		Extract:  true,
	}
	l := log.WithFields(log.Fields{
		"destPath": outputDir,
	})

	_, err = writeContents(l, file, outputDir+"/extracted")
	if err != nil {
		t.Fatalf("failed to write first archive contents: %s", err)
	}
	f.Close()

	// now, overwrite it
	f2, err := os.Open("./testdata/updated-file_tar")
	if err != nil {
		t.Fatalf("failed to open test file: %s", err)
	}
	secondFile := &types.InputFile{
		Contents: f2,
		Extract:  true,
	}

	_, err = writeContents(l, secondFile, outputDir+"/extracted")
	if err != nil {
		t.Fatalf("failed to write second archive contents: %s", err)
	}
	f2.Close()

	// _, err = os.Stat(outputDir + "/extracted/test-file.txt")
	// if err == nil {
	// 	t.Errorf("didn't expect to find first file there")
	// }

	contents, err := ioutil.ReadFile(outputDir + "/extracted/test-file.txt")
	if err != nil {
		t.Errorf("failed to read saved contents: %s", err)
		return
	}
	if strings.TrimSpace(string(contents)) != "updated contents" {
		t.Errorf("expected 'updated contents', got: %s", strings.TrimSpace(string(contents)))
	}
}
