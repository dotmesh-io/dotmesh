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

	err = requireValidVolumeName(VolumeName{Namespace: "00000000001111111111222222222233333333334444444444555555555566666", Name: "ok"})
	if err == nil {
		t.Error("65-character names shouldn't be valid")
	}

	err = requireValidVolumeName(VolumeName{Namespace: "0000000000111111111122222222223333333333444444444455555555556666", Name: "ok"})
	if err != nil {
		t.Error("64-character names should be valid")
	}

	err = requireValidVolumeName(VolumeName{Namespace: "ok", Name: "00000000001111111111222222222233333333334444444444555555555566666"})
	if err == nil {
		t.Error("65-character names shouldn't be valid")
	}

	err = requireValidVolumeName(VolumeName{Namespace: "ok", Name: "0000000000111111111122222222223333333333444444444455555555556666"})
	if err != nil {
		t.Error("64-character names should be valid")
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

	err = requireValidBranchName("00000000001111111111222222222233333333334444444444555555555566666")
	if err == nil {
		t.Error("65-character names shouldn't be valid")
	}

	err = requireValidBranchName("0000000000111111111122222222223333333333444444444455555555556666")
	if err != nil {
		t.Error("64-character names should be valid")
	}
}
