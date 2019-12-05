package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetFilePathCantEscapeRootDirectory(t *testing.T) {
	check := func(snapshotPath, filename, expectedPath string) {
		file := OutputFile{
			SnapshotMountPath: snapshotPath,
			Filename:          filename,
		}
		path, err := file.GetFilePath()
		if assert.NoError(t, err) {
			assert.Equal(t, expectedPath, path)
		}
	}

	// Single element:
	check("/mnt", "a", "/mnt/__default__/a")
	// Multiple elements:
	check("/mnt", "foo/bar", "/mnt/__default__/foo/bar")
	// Insecure paths don't allow escaping the root directory:
	check("/mnt", "..", "/mnt/__default__")
	check("/mnt/mount", "../..", "/mnt/mount/__default__")
}
