package archiver

import (
	"github.com/mholt/archiver/v3"
)

// Unarchive - unarchive tar
func Unarchive(source, destination string) error {
	return archiver.Unarchive(source, destination)
}
