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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gorilla/rpc/v2/json2"
)

var logger *log.Logger

// flexVolumeDebug indicates whether flexvolume debugging should be enabled
var flexVolumeDebug = false

func init() {
	// XXX: invent a better way to decide whether debugging should
	// be used for flexvolume driver. For now we only enable it if
	// Docker-in-Docker env is used
	if fi, err := os.Stat("/dind/flexvolume_driver"); err == nil && !fi.IsDir() {
		flexVolumeDebug = true
	}
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

	var mountPath string

	// Load the API key
	binaryDir := filepath.Dir(os.Args[0])
	keyBytes, err := ioutil.ReadFile(binaryDir + "/admin-api-key")
	if err != nil {
		return nil, err
	}

	apiKey := string(keyBytes)

	namespace := opts["namespace"].(string)

	name := opts["name"].(string)

	var subvolume string
	if _, ok := opts["subdot"]; ok {
		subvolume = opts["subdot"].(string)
	}

	// Match the semantics used by Docker, from parseNamespacedVolumeWithSubvolumes
	switch subvolume {
	case "":
		subvolume = "__default__"
	case "__root__":
		subvolume = ""
	}

	err = doRPC(
		"127.0.0.1",
		"admin",
		apiKey,
		"DotmeshRPC.Procure",
		struct {
			Namespace string
			Name      string
			Subdot    string
		}{
			Namespace: namespace,
			Name:      name,
			Subdot:    subvolume,
		},
		&mountPath,
	)
	if err != nil {
		logger.Printf("MOUNT: Procure of %s/%s.%s failed: %+v", namespace, name, subvolume, err)
		return opts, err
	}

	logger.Printf("MOUNT: Procured %s/%s.%s at %s", namespace, name, subvolume, mountPath)

	// hackity hack, let's see how kube feels about being given a symlink
	_, err = os.Stat(targetMountDir)
	if os.IsNotExist(err) {
		err = os.Symlink(mountPath, targetMountDir)
		if err != nil {
			logger.Printf("MOUNT: symlink err: %v", err)
			return nil, err
		}
	} else if err != nil {
		logger.Printf("MOUNT: stat err: %v", err)
		return nil, err
	} else if err == nil {
		logger.Printf("MOUNT: symlink already existed!")
		err = os.Remove(targetMountDir)
		if err != nil {
			logger.Printf("MOUNT: remove err: %v", err)
			return nil, err
		}
		err = os.Symlink(mountPath, targetMountDir)
		if err != nil {
			logger.Printf("MOUNT: symlink err: %v", err)
			return nil, err
		}
	}
	return nil, nil
}

// Invocation: <driver executable> unmount <mount dir>
func (d *FlexVolumeDriver) unmount(targetMountDir string) (map[string]interface{}, error) {
	// TODO
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
	f, err := os.OpenFile("/var/log/dotmesh-flexvolume.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	logger = log.New(f, fmt.Sprintf("%d: ", os.Getpid()), log.Ldate+log.Ltime+log.Lmicroseconds)
	driver := NewFlexVolumeDriver()
	os.Stdout.WriteString(driver.Run(os.Args[1:]))
}

// RPC client

func doRPC(hostname, user, apiKey, method string, args interface{}, result interface{}) error {
	url := fmt.Sprintf("http://%s:6969/rpc", hostname)
	message, err := json2.EncodeClientRequest(method, args)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(message))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(user, apiKey)
	client := new(http.Client)

	resp, err := client.Do(req)

	if err != nil {
		logger.Printf("RPC FAIL: %+v -> %s -> %+v", args, method, err)
		return err
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Printf("RPC FAIL: %+v -> %s -> %+v", args, method, err)
		return fmt.Errorf("Error reading body: %s", err)
	}
	err = json2.DecodeClientResponse(bytes.NewBuffer(b), &result)
	if err != nil {
		logger.Printf("RPC FAIL: %+v -> %s -> %+v", args, method, err)
		return fmt.Errorf("Couldn't decode response '%s': %s", string(b), err)
	}

	if flexVolumeDebug {
		logger.Printf("RPC: %+v -> %s -> %v", args, method, result)
	}
	return nil
}
