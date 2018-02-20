package main

import "testing"

type MockGit struct {
	mode string
}

func (git MockGit) getBranch() (string, error) {
	if git.mode == "release" {
		return "release-1.0", nil
	} else if git.mode == "re-release" {
		return "release-1.10.9", nil
	} else {
		return "master", nil
	}
}

func (git MockGit) getCommitHash() (string, error) {
	return "d70e9622933eec787c18827600b9eba438420b90", nil
}

func (git MockGit) getNumberOfCommitsSinceMaster() (int, error) {
	return 10, nil
}

func TestVersionerMaster(t *testing.T) {
	// git rev-parse --abbrev-ref HEAD -> master
	// git rev-parse HEAD -> d70e9622933eec787c18827600b9eba438420b90
	masterCase := MockGit{mode: "non-release"}
	release, err := calculateVersion(masterCase)
	if err != nil {
		t.Error(err)
	}
	if release != "master-d70e962" {
		t.Errorf("Version does not include the hash of the commit: %s", release)
	}
}

func TestVersionerRelease(t *testing.T) {
	// git rev-parse --abbrev-ref HEAD -> release-x.y
	// git rev-list --count HEAD ^master -> 0, 1, 2

	//releaseCase := MockGit{mode: "release"}
	releaseBranchCase := MockGit{mode: "release"}
	release, err := calculateVersion(releaseBranchCase)
	if err != nil {
		t.Error(err)
	}
	if release != "release-1.0.10" {
		t.Errorf("Version does not include the semver: %s", release)
	}
}

func TestVersionerAlreadyReleasedBranch(t *testing.T) {
	releaseBranchCase := MockGit{mode: "re-release"}
	release, err := calculateVersion(releaseBranchCase)
	if err != nil {
		t.Error(err)
	}
	if release != "release-1.10.9" {
		t.Errorf("Version does not include the semver: %s", release)
	}
}
