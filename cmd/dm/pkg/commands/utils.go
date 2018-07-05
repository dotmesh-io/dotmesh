package commands

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"os"

	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
)

// pretty-print MiB or KiB or GiB
func prettyPrintSize(size int64) string {
	s := "-"
	if size > 0 {
		if size < 1024*1024 {
			s = fmt.Sprintf("%.2f kiB", float64(size)/1024)
		} else if size < 1024*1024*1024 {
			s = fmt.Sprintf("%.2f MiB", float64(size)/(1024*1024))
		} else {
			s = fmt.Sprintf("%.2f GiB", float64(size)/(1024*1024*1024))
		}
	}
	return s
}

func resolveTransferArgs(args []string) (returnPeer string, returnFilesystemName string, returnBranchName string, returnError error) {

	// Use:   "{push,pull,clone} <remote>",
	/*

		Cases:
		- dm push <remote>
		  Use current volume and current branch. (TODO, should we have the idea
		  of a "remote tracking branch"?)

	*/
	dm, err := remotes.NewDotmeshAPI(configPath, verboseOutput)
	if err != nil {
		return "", "", "", err
	}

	if len(args) < 1 {
		return "", "", "", fmt.Errorf(
			"Please specify which remote you want to push/pull/clone to/from. " +
				"Use 'dm remote -v' to list remotes.",
		)
	}

	var filesystemName, branchName string

	if len(args) == 1 {
		// dm push
		filesystemName, err = dm.StrictCurrentVolume()
		if err != nil {
			return "", "", "", err
		}

		branchName, err = dm.CurrentBranch(filesystemName)
		if err != nil {
			return "", "", "", err
		}
	} else if len(args) == 2 {
		// dm clone foo
		filesystemName = args[1]
		branchName = remotes.DEFAULT_BRANCH
	} else if len(args) == 3 {
		// dm pull foo branch
		filesystemName = args[1]
		branchName = args[2]
	}

	return args[0], filesystemName, branchName, nil
}

func RandToken(length int) (string, error) {
	if length != 32 {
		panic("Can't generate non-32 byte long random tokens.")
	}
	b := make([]byte, 20)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base32.StdEncoding.EncodeToString(b), nil
}

func runHandlingError(f func() error) {
	if err := f(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
