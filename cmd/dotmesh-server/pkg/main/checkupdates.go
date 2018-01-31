package main

import (
	cp "github.com/dotmesh-io/go-checkpoint"
)

func (state *InMemoryState) checkForUpdates() error {
	checkParams := cp.CheckParams{
		Product: "dotmesh-server",
		Version: state.versionInfo.InstalledVersion,
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
