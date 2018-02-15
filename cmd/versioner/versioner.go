package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

type RealGit struct{}

type Git interface {
	getBranch() (string, error)
	getCommitHash() (string, error)
	getNumberOfCommitsSinceMaster() (int, error)
}

func calculateVersion(git Git) (string, error) {
	branch, err := git.getBranch()
	if err != nil {
		handleError(err)
	}

	if strings.Contains(branch, "release-") {
		commitNumber, err := git.getNumberOfCommitsSinceMaster()
		if err != nil {
			handleError(err)
		}
		return fmt.Sprintf("%s.%v", branch, commitNumber), nil
	} else {
		hash, err := git.getCommitHash()
		if err != nil {
			handleError(err)
		}
		return fmt.Sprintf("%s-%s", branch, hash[:7]), nil
	}
}

func (git RealGit) getBranch() (string, error) {
	branch := os.Getenv("CI_COMMIT_REF_NAME")
	if branch == "" {
		branch = executeCommand([]string{"rev-parse", "--abbrev-ref", "HEAD"})
	}
	return branch, nil
}

func (git RealGit) getCommitHash() (string, error) {
	commits := executeCommand([]string{"rev-parse", "HEAD"})
	return commits, nil
}

func (git RealGit) getNumberOfCommitsSinceMaster() (int, error) {
	commits := executeCommand([]string{"rev-list", "--count", "HEAD", "^master"})
	numberOfCommits, err := strconv.Atoi(commits)
	if err != nil {
		handleError(err)
	}
	return numberOfCommits, nil
}

func executeCommand(args []string) string {
	cmd := exec.Command("git", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		handleError(err)
	}
	return strings.TrimSpace(out.String())
}

func main() {
	version, err := calculateVersion(RealGit{})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(version)

}

func handleError(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
