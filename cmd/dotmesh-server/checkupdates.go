package main

import (
	"fmt"
	"os/exec"
	"strings"

	cp "github.com/dotmesh-io/go-checkpoint"
)

func (state *InMemoryState) checkForUpdates() error {
	numberOfDots := func() string {
		state.mastersCacheLock.Lock()
		defer state.mastersCacheLock.Unlock()
		return fmt.Sprintf("%d", len(state.mastersCache))
	}()

	totalBytes := func() string {
		state.globalDirtyCacheLock.Lock()
		defer state.globalDirtyCacheLock.Unlock()
		var totalBytes int64 = 0
		for _, dirtyInfo := range state.globalDirtyCache {
			totalBytes += dirtyInfo.SizeBytes
		}
		return fmt.Sprintf("%d", totalBytes)
	}()

	otherServerIDs := func() string {
		state.serverAddressesCacheLock.Lock()
		defer state.serverAddressesCacheLock.Unlock()
		idList := ""
		for server, _ := range state.serverAddressesCache {
			if idList == "" {
				idList = server
			} else {
				idList = idList + "," + server
			}
		}
		return idList
	}()

	osCommand := exec.Command("cat", "/etc/issue")
	os, err := osCommand.CombinedOutput()
	var operatingSystem string
	if err != nil {
		operatingSystem = fmt.Sprintf("ERROR: %+v", err)
	} else {
		operatingSystem = strings.TrimSpace(string(os))
	}

	archCommand := exec.Command("uname", "-a")
	var architecture string
	arch, err := archCommand.CombinedOutput()
	if err != nil {
		architecture = fmt.Sprintf("ERROR: %+v", err)
	} else {
		architecture = strings.TrimSpace(string(arch))
	}

	checkParams := cp.CheckParams{
		Product: "dotmesh-server",
		Version: state.versionInfo.InstalledVersion,
		Flags: map[string]string{
			"ServerID":                 state.myNodeId,
			"NumberOfDots":             numberOfDots,
			"TotalCurrentDotSizeBytes": totalBytes,
			"ServersInCluster":         otherServerIDs,
		},
		OS:   operatingSystem,
		Arch: architecture,
	}

	response, err := cp.Check(&checkParams)

	if err != nil {
		return err
	}

	state.versionInfo.CurrentVersion = response.CurrentVersion
	state.versionInfo.CurrentReleaseDate = response.CurrentReleaseDate
	state.versionInfo.CurrentDownloadURL = response.CurrentDownloadURL
	state.versionInfo.CurrentChangelogURL = response.CurrentChangelogURL
	state.versionInfo.ProjectWebsite = response.ProjectWebsite
	state.versionInfo.Outdated = response.Outdated

	return nil
}
