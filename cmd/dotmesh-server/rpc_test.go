package main

import (
	"testing"
)

func TestRequireValidVolumeName(t *testing.T) {
	err := requireValidVolumeName(VolumeName{Namespace: "", Name: ""})
	if err == nil {
		t.Error("Empty names shouldn't be valid")
	}

	err = requireValidVolumeName(VolumeName{Namespace: "ok", Name: ""})
	if err == nil {
		t.Error("Empty names shouldn't be valid")
	}

	err = requireValidVolumeName(VolumeName{Namespace: "!", Name: ""})
	if err == nil {
		t.Error("Funny characters shouldn't be valid")
	}

	err = requireValidVolumeName(VolumeName{Namespace: "", Name: "!"})
	if err == nil {
		t.Error("Funny characters shouldn't be valid")
	}

	err = requireValidVolumeName(VolumeName{Namespace: "111111111122222222223333333333444444444455555555556", Name: "ok"})
	if err == nil {
		t.Error("51-character names shouldn't be valid")
	}

	err = requireValidVolumeName(VolumeName{Namespace: "11111111112222222222333333333344444444445555555555", Name: "ok"})
	if err != nil {
		t.Error("50-character names should be valid")
	}

	err = requireValidVolumeName(VolumeName{Namespace: "ok", Name: "111111111122222222223333333333444444444455555555556"})
	if err == nil {
		t.Error("51-character names shouldn't be valid")
	}

	err = requireValidVolumeName(VolumeName{Namespace: "ok", Name: "11111111112222222222333333333344444444445555555555"})
	if err != nil {
		t.Error("50-character names should be valid")
	}
}

func TestRequireValidBranchName(t *testing.T) {
	err := requireValidBranchName("")
	if err != nil {
		t.Error("Empty names should be valid")
	}

	err = requireValidBranchName("!")
	if err == nil {
		t.Error("Funny characters shouldn't be valid")
	}

	err = requireValidBranchName("111111111122222222223333333333444444444455555555556")
	if err == nil {
		t.Error("51-character names shouldn't be valid")
	}

	err = requireValidBranchName("11111111112222222222333333333344444444445555555555")
	if err != nil {
		t.Error("50-character names should be valid")
	}
}
