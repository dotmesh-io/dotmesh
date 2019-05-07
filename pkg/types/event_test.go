package types

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestEventWithError(t *testing.T) {
	myErr := errors.New("some error")

	e := NewErrorEvent("foo", myErr)

	if e.Error() != myErr {
		t.Errorf("expected to find err %v, got %v", myErr, e.Error())
	}
}

func TestEventWithNoError(t *testing.T) {
	e := NewErrorEvent("foo", nil)

	if e.Error() != nil {
		t.Errorf("expected to find err %v, got %v", nil, e.Error())
	}
}

func TestGetErrorFromNormalEvent(t *testing.T) {
	e := NewEvent("some-event")

	if e.Error() != nil {
		t.Errorf("unexpected error: %v", e.Error())
	}
}
func TestGetErrorFromNilArgsEvent(t *testing.T) {
	e := &Event{
		Name: "foo",
	}

	if e.Error() != nil {
		t.Errorf("unexpected error: %v", e.Error())
	}
}

func TestEncodeDecode(t *testing.T) {
	diffFiles := []ZFSFileDiff{
		{
			Change:   FileChangeAdded,
			Filename: "foo",
		},
	}

	enc, err := EncodeZFSFileDiff(diffFiles)
	if err != nil {
		t.Error(err)
	}

	files, err := DecodeZFSFileDiff(enc)
	if err != nil {
		t.Errorf("interface conversion failed to files: %s", err)
		return
	}

	if files[0].Filename != diffFiles[0].Filename {
		t.Errorf("filenames do not match: %s != %s", files[0].Filename, diffFiles[0].Filename)
	}

}

func TestEncodeDiffFiles(t *testing.T) {

	diffFiles := []ZFSFileDiff{
		{
			Change:   FileChangeAdded,
			Filename: "foo",
		},
	}

	enc, err := EncodeZFSFileDiff(diffFiles)
	if err != nil {
		t.Error(err)
	}

	event := &Event{
		Name: "diffed",
		Args: &EventArgs{
			"files": enc,
		},
	}

	bts, err := json.Marshal(event)
	if err != nil {
		t.Errorf("failed to encode: %s", err)
		return
	}

	var e Event
	err = json.Unmarshal(bts, &e)
	if err != nil {
		t.Errorf("failed to decode: %s", err)
		return
	}

	f, ok := (*e.Args)["files"]
	if !ok {
		t.Errorf("failed to get files key")
		return
	}

	files, err := DecodeZFSFileDiff(f.(string))
	if err != nil {

		t.Log(f)

		t.Errorf("interface conversion failed to files: %s", err)
		return
	}

	if files[0].Filename != "foo" {
		t.Errorf("expected 'foo', got: %s ", files[0].Filename)
	}
}
