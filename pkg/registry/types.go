package registry

import "fmt"

type NoSuchClone struct {
	filesystemId string
}

func (n NoSuchClone) Error() string {
	return fmt.Sprintf("No clone with filesystem id '%s'", n.filesystemId)
}
