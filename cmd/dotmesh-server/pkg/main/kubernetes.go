package main

import (
	"log"
	"os"
)

var FLEXVOLUME_DIR = "/system-flexvolume/dotmesh.io~dm"
var FLEXVOLUME_BIN = "dm"
var FLEXVOLUME_SOURCE = "/usr/local/bin/flexvolume"

func installKubernetesPlugin() error {
	// Just atomically install the flexvolume binary every time we start up.
	// This way we'll always handle upgrades.

	log.Printf("[flexvolume] Installing driver into %s...", FLEXVOLUME_DIR)

	_, err := os.Stat(FLEXVOLUME_DIR)
	if os.IsNotExist(err) {
		err = os.MkdirAll(FLEXVOLUME_DIR, 0700)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	err = Copy(FLEXVOLUME_SOURCE, FLEXVOLUME_DIR+"/."+FLEXVOLUME_BIN, 0755)
	if err != nil {
		return err
	}

	err = os.Rename(
		FLEXVOLUME_DIR+"/."+FLEXVOLUME_BIN,
		FLEXVOLUME_DIR+"/"+FLEXVOLUME_BIN,
	)
	if err != nil {
		return err
	}

	log.Printf("[flexvolume] Installed into %s.", FLEXVOLUME_DIR)

	return nil
}
