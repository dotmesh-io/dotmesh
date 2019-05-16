package fsm

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

func CalculatePrelude(snaps []types.Snapshot, toSnapshotId string) (types.Prelude, error) {
	var prelude types.Prelude
	// Intentionally empty prelude, as commit metadata is
	// transmitted in a file in the dot now, and prelude
	// performance suuuucks!
	// https://github.com/dotmesh-io/dotmesh/issues/700
	return prelude, nil
}

func ConsumePrelude(r io.Reader) (types.Prelude, error) {
	// called when we know that there's a prelude to read from r.

	// read a byte at a time, so that we leave the reader ready for someone
	// else.
	b := make([]byte, 1)
	finished := false
	buf := []byte{}

	for !finished {
		_, err := r.Read(b)
		if err == io.EOF {
			return types.Prelude{}, fmt.Errorf("Stream ended before prelude completed, got '%s' so far", string(buf))
		}
		if err != nil {
			return types.Prelude{}, err
		}
		buf = append(buf, b...)
		idx := bytes.Index(buf, types.EndDotmeshPrelude)
		if idx != -1 {
			preludeEncoded := buf[0:idx]
			data, err := base64.StdEncoding.DecodeString(string(preludeEncoded))
			if err != nil {
				return types.Prelude{}, err
			}
			var p types.Prelude
			err = json.Unmarshal(data, &p)
			if err != nil {
				return p, err
			}
			return p, nil
		}
	}
	return types.Prelude{}, nil
}

func EncodePrelude(prelude types.Prelude) ([]byte, error) {
	// encode a prelude as JSON wrapped up in base64. The reason for the base64
	// is to avoid framing issues. This works because END_DOTMESH_PRELUDE has
	// non-base64 characters in it.
	preludeBytes, err := json.Marshal(prelude)
	if err != nil {
		return []byte{}, err
	}
	encoded := []byte(base64.StdEncoding.EncodeToString(preludeBytes))
	encoded = append(encoded, types.EndDotmeshPrelude...)
	return encoded, nil
}
