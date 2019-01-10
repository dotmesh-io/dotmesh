package fsm

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

func calculatePrelude(snaps []types.Snapshot, toSnapshotId string) (types.Prelude, error) {
	var prelude types.Prelude
	// snaps, err := s.SnapshotsFor(s.myNodeId, toFilesystemId)
	// if err != nil {
	// 	return prelude, err
	// }
	pointerSnaps := []*types.Snapshot{}
	for _, s := range snaps {
		// Take a copy of s to take a pointer of, rather than getting
		// lots of pointers to so in the pointerSnaps slice...
		snapshots := s
		pointerSnaps = append(pointerSnaps, &snapshots)
	}
	var err error
	prelude.SnapshotProperties, err = restrictSnapshots(pointerSnaps, toSnapshotId)
	if err != nil {
		return prelude, err
	}
	return prelude, nil
}

func consumePrelude(r io.Reader) (types.Prelude, error) {
	// called when we know that there's a prelude to read from r.

	// read a byte at a time, so that we leave the reader ready for someone
	// else.
	b := make([]byte, 1)
	finished := false
	buf := []byte{}

	for !finished {
		_, err := r.Read(b)
		if err == io.EOF {
			return types.Prelude{}, fmt.Errorf("Stream ended before prelude completed")
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

func encodePrelude(prelude types.Prelude) ([]byte, error) {
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
