package archiver

import (
	"github.com/mholt/archiver"
)

func Unarchive(source, destination string) error {
	return archiver.Unarchive(source, destination)
}
