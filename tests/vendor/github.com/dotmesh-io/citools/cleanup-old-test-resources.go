package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

const dotmeshTestPools = "/dotmesh-test-pools"
const staleTimeout = time.Hour * 24

func cleanup(runDir string) {
	log.Printf("Cleaning up %s", runDir)
	fp, err := os.Open(runDir)
	if err != nil {
		log.Printf("Error opening %s: %s", runDir, err.Error())
		return
	}
	names, err := fp.Readdirnames(-1)
	if err != nil {
		log.Printf("Error reading %s: %s", runDir, err.Error())
		return
	}

	scripts := []string{}

	for _, name := range names {
		if strings.HasPrefix(name, "cleanup-actions.") {
			scripts = append(scripts, path.Join(runDir, name))
		}
	}

	sort.Strings(scripts)

	for _, script := range scripts {
		log.Printf("Running %s", script)
		cmd := exec.Command("/bin/sh", "-ex", script)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			log.Printf("Error running %s: %s", script, err.Error())
			// If we want to plough on when one script fails, we can remove this break.

			// However, this tends to mean that the final `99` cleanup
			// script will nuke the entire directory with all the cleanup
			// scripts, leaving no trace of what went wrong except maybe a
			// few dirs we couldn't remove because they still had mounts,
			// and some lingering zpools.

			break
		} else {
			// Ignore errors here
			_ = os.Remove(script)
			log.Printf("Completed %s and deleted it", script)
		}
	}
	log.Printf("Finished cleanup for %s", runDir)
}

func main() {
	log.Printf("Cleaning up old test resources...")

	fp, err := os.Open(dotmeshTestPools)
	if err != nil {
		log.Printf("Error opening %s: %s", dotmeshTestPools, err.Error())
		os.Exit(-1)
	}
	defer fp.Close()

	contents, err := fp.Readdir(-1)
	if err != nil {
		log.Printf("Error reading %s: %s", dotmeshTestPools, err.Error())
		os.Exit(-1)
	}

	staleThreshold := time.Now().Add(-staleTimeout)

	wg := sync.WaitGroup{}

	// Within this loop, continue on error, rather than aborting entire process
	for _, fi := range contents {
		if !fi.IsDir() {
			// Not interesting as it's not a directory
			continue
		}

		name := fi.Name()

		finishedPath := path.Join(dotmeshTestPools, name, "finished")
		_, err := os.Stat(finishedPath)

		switch {
		case err != nil && !os.IsNotExist(err):
			log.Printf("Error checking for %s: %s", finishedPath, err.Error())
			continue
		case os.IsNotExist(err):
			// Not marked as finished; but is it stale? Note that "fi"
			// refers to the directory here, NOT the nonexistant finished
			// file.
			if fi.ModTime().After(staleThreshold) {
				// It's recent, so continue
				log.Printf("Skipping %s as it's unfinished and fresh", name)
				continue
			}
			// Otherwise, fall through...
		case err == nil:
			log.Printf("%s is finished", name)
			// Marked as finished, so fall through...
		}

		initiatedPath := path.Join(dotmeshTestPools, name, "cleanup-initiated")
		fp, err := os.OpenFile(initiatedPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		switch {
		case err != nil && os.IsExist(err):
			log.Printf("Skipping %s as it's already being cleaned up", name)
			continue
		case err != nil:
			log.Printf("Error obtaining cleanup lock %s: %s", initiatedPath, err.Error())
			continue
		case err == nil:
			// Let's proceed
		}
		fp.Write([]byte(fmt.Sprintf("%d", os.Getpid())))
		fp.Close()

		// This is a directory we want to clean up.
		wg.Add(1)
		go func(runDir, initiatedPath string) {
			defer wg.Done()

			// If the cleanup failed, we'll want to remove this lockfile so
			// we can try again later. If the cleanup succeeded the lockfile
			// will be gone anyway, so we can just ignore errors here.
			defer os.Remove(initiatedPath)

			cleanup(runDir)
		}(path.Join(dotmeshTestPools, name), initiatedPath)
	}

	wg.Wait()

	log.Printf("Finished.")
}
