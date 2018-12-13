package types

import (
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
