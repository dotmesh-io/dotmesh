package uuid

import (
	"github.com/google/uuid"
)

type UUID = uuid.UUID

// New creates a new random UUID or panics.  New is equivalent to
// the expression
//
//    uuid.Must(uuid.NewRandom())
func New() uuid.UUID {
	return uuid.New()
}

func FromString(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}
