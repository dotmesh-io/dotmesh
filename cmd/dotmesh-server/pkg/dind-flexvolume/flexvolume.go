/*
Copyright 2017 Mirantis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Copied from https://github.com/Mirantis/virtlet/tree/master/pkg/flexvolume
// with thanks :-)

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

var logger *log.Logger

// flexVolumeDebug indicates whether flexvolume debugging should be enabled
var flexVolumeDebug = false

var dindStorageRoot = ""

func System(cmd string, args ...string) error {
	logger.Printf("[system] running %s %s", cmd, args)
	c := exec.Command(cmd, args...)
	out, err := c.CombinedOutput()
	logger.Printf("[system] result: %v, %s", err, out)
	return err
}

func init() {
	// XXX: invent a better way to decide whether debugging should
	// be used for flexvolume driver. For now we only enable it if
	// Docker-in-Docker env is used
	if fi, err := os.Stat("/dind/flexvolume_driver"); err == nil && !fi.IsDir() {
		flexVolumeDebug = true
	}

	pathBytes, err := ioutil.ReadFile("/dind-flexvolume-prefix")
	if err != nil {
		logger.Printf("INIT: Can't read /dind-flexvolume-prefix: %#v", err)
		panic("Can't read configuration")
	}

	dindStorageRoot = strings.TrimSpace(string(pathBytes))
}

type FlexVolumeDriver struct {
}

func NewFlexVolumeDriver() *FlexVolumeDriver {
	return &FlexVolumeDriver{}
}

// The following functions are not currently needed, but still
// keeping them to make it easier to actually implement them

// Invocation: <driver executable> init
func (d *FlexVolumeDriver) init() (map[string]interface{}, error) {
	return map[string]interface{}{"capabilities": map[string]bool{"attach": false}}, nil
}

// Invocation: <driver executable> mount <target mount dir> <mount device> <json options>
func (d *FlexVolumeDriver) mount(targetMountDir, jsonOptions string) (map[string]interface{}, error) {
	var opts map[string]interface{}
	if err := json.Unmarshal([]byte(jsonOptions), &opts); err != nil {
		return nil, fmt.Errorf("failed to unmarshal json options: %v", err)
	}
	logger.Printf("MOUNT: targetMountDir: %v, jsonOptions: %+v", targetMountDir, jsonOptions)

	pvId := opts["id"].(string)
	sizeBytes, err := strconv.Atoi(opts["size"].(string))
	if err != nil {
		logger.Printf("MOUNT: Invalid size %s: %v", opts["size"].(string), err)
		return nil, err
	}

	sourceFile := filepath.Join(dindStorageRoot, pvId)

	// See if our target image file exists already, and create it if
	// not.  Take care to be atomic here!

	fp, err := os.OpenFile(sourceFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	switch {
	case os.IsExist(err):
		// Already exists, just use it
		logger.Printf("MOUNT: Using existing source file %s", sourceFile)
	case err == nil:
		// We created the file, so it didn't already exist; close it
		// (zero length) and let mkfs.ext4 fill it in.
		fp.Close()

		// Pull the requested size from the opts, must be "NNNk" to NNN kilobytes etc.
		err = System("mkfs.ext4", sourceFile, fmt.Sprintf("%dk", (sizeBytes+1023)/1024))
		if err != nil {
			logger.Printf("MOUNT: mkfs err for %s: %v", sourceFile, err)
			return nil, err
		}
		logger.Printf("MOUNT: Created source file %s", sourceFile)
	default:
		// Something went wrong!!!
		logger.Printf("MOUNT: error acquiring exclusive access to %s: %v", sourceFile, err)
		return nil, err
	}

	// Mount sourceFile at targetMountDir, -o loop
	err = os.MkdirAll(targetMountDir, 0777)
	if err != nil {
		logger.Printf("MOUNT: MkdirAll err for %s: %v", targetMountDir, err)
		return nil, err
	}
	err = System("mount", sourceFile, targetMountDir)
	if err != nil {
		logger.Printf("MOUNT: mount err for %s on %s: %v", sourceFile, targetMountDir, err)
		return nil, err
	}

	logger.Printf("MOUNT: Successfully mounted %s on %s", sourceFile, targetMountDir)

	return nil, nil
}

// Invocation: <driver executable> unmount <mount dir>
func (d *FlexVolumeDriver) unmount(targetMountDir string) (map[string]interface{}, error) {
	err := System("umount", targetMountDir)
	if err != nil {
		logger.Printf("UNMOUNT: unmount err for %s: %v", targetMountDir, err)
		return nil, err
	}
	err = os.Remove(targetMountDir)
	if err != nil {
		logger.Printf("UNMOUNT: remove mountpoint err for %s: %v", targetMountDir, err)
		return nil, err
	}

	return nil, nil
}

type driverOp func(*FlexVolumeDriver, []string) (map[string]interface{}, error)

type cmdInfo struct {
	numArgs int
	run     driverOp
}

var commands = map[string]cmdInfo{
	"init": cmdInfo{
		0, func(d *FlexVolumeDriver, args []string) (map[string]interface{}, error) {
			return d.init()
		},
	},
	"mount": cmdInfo{
		2, func(d *FlexVolumeDriver, args []string) (map[string]interface{}, error) {
			return d.mount(args[0], args[1])
		},
	},
	"unmount": cmdInfo{
		1, func(d *FlexVolumeDriver, args []string) (map[string]interface{}, error) {
			return d.unmount(args[0])
		},
	},
}

func (d *FlexVolumeDriver) doRun(args []string) (map[string]interface{}, error) {
	if len(args) == 0 {
		return nil, errors.New("no arguments passed to flexvolume driver")
	}
	nArgs := len(args) - 1
	op := args[0]
	if cmdInfo, found := commands[op]; found {
		if cmdInfo.numArgs == nArgs {
			return cmdInfo.run(d, args[1:])
		} else {
			return nil, fmt.Errorf("unexpected number of args %d (expected %d) for operation %q", nArgs, cmdInfo.numArgs, op)
		}
	} else {
		return map[string]interface{}{
			"status": "Not supported",
		}, nil
	}
}

func (d *FlexVolumeDriver) Run(args []string) string {
	r := formatResult(d.doRun(args))

	if flexVolumeDebug {
		// This is for debugging purposes only.
		// The problem is that kubelet grabs CombinedOutput() from the process
		// and tries to parse it as JSON (need to recheck this,
		// maybe submit a PS to fix it)
		logger.Printf("DEBUG: %s -> %s", strings.Join(args, " "), r)
	}

	return r
}

func formatResult(fields map[string]interface{}, err error) string {
	var data map[string]interface{}
	if err != nil {
		data = map[string]interface{}{
			"status":  "Failure",
			"message": err.Error(),
		}
	} else {
		data = map[string]interface{}{
			"status": "Success",
		}
		for k, v := range fields {
			data[k] = v
		}
	}
	s, err := json.Marshal(data)
	if err != nil {
		panic("error marshalling the data")
	}
	return string(s) + "\n"
}

func main() {
	f, err := os.OpenFile("/var/log/dotmesh-dind-flexvolume.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	logger = log.New(f, fmt.Sprintf("%d: ", os.Getpid()), log.Ldate+log.Ltime+log.Lmicroseconds)
	driver := NewFlexVolumeDriver()
	os.Stdout.WriteString(driver.Run(os.Args[1:]))
}
