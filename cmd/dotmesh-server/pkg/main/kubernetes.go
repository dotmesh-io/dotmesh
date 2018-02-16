package main

import (
	"encoding/base64"
	"io/ioutil"
	"os"
)

var FLEXVOLUME_DIR = "/system-usr/usr/libexec/kubernetes/kubelet-plugins/volume/exec/dotmesh.io~dm"
var FLEXVOLUME_BIN = "dm"
var FLEXVOLUME_KEYFILE = "admin-api-key"
var FLEXVOLUME_SOURCE = "/usr/local/bin/flexvolume"

func installKubernetesPlugin() error {
	// Just atomically install the flexvolume binary every time we start up.
	// This way we'll always handle upgrades.

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

	// Save admin API key into the file the FV driver reads it from
	key64, ok := os.LookupEnv("INITIAL_ADMIN_API_KEY")
	if ok {
		key, err := base64.StdEncoding.DecodeString(key64)
		err = ioutil.WriteFile(FLEXVOLUME_DIR+"/"+FLEXVOLUME_KEYFILE, []byte(key), 0600)
		if err != nil {
			return err
		}
	}

	return nil
}
