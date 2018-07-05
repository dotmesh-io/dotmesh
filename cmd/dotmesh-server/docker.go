package main

// docker volume plugin for providing dotmesh volumes to docker via e.g.
// docker run -v name:/path --volume-driver=dm

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/auth"
)

const PLUGINS_DIR = "/run/docker/plugins"
const DM_SOCKET = PLUGINS_DIR + "/dm.sock"

type ResponseImplements struct {
	// A response to the Plugin.Activate request
	Implements []string
}

type RequestCreate struct {
	// A request to create a volume for Docker
	Name string
	Opts map[string]string
}

type RequestMount struct {
	// A request to mount a volume for Docker
	Name string
}

type RequestGet struct {
	// A request to get a volume for Docker
	Name string
}

type RequestRemove struct {
	// A request to remove a volume for Docker
	Name string
}

type ResponseSimple struct {
	// A response which only indicates if there was an error or not
	Err string
}

type ResponseMount struct {
	// A response to the VolumeDriver.Mount request
	Mountpoint string
	Err        string
}

type ResponseListVolume struct {
	// Used in the JSON representation of ResponseList
	Name       string
	Mountpoint string
	Status     map[string]string // TODO actually start using the status to report on things in dm
}

type ResponseList struct {
	// A response which enumerates volumes for VolumeDriver.List
	Volumes []ResponseListVolume
	Err     string
}

type ResponseGet struct {
	// A response which enumerates volumes for VolumeDriver.Get
	Volume ResponseListVolume
	Err    string
}

// create a symlink from /dotmesh/:name[@:branch] into /dmfs/:filesystemId
func newContainerMountSymlink(name VolumeName, filesystemId string, subvolume string) (string, error) {
	log.Printf("[newContainerMountSymlink] name=%+v, fsId=%s.%s", name, filesystemId, subvolume)
	if _, err := os.Stat(CONTAINER_MOUNT_PREFIX); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(CONTAINER_MOUNT_PREFIX, 0700); err != nil {
				log.Printf("[newContainerMountSymlink] error creating prefix %s: %+v", CONTAINER_MOUNT_PREFIX, err)
				return "", err
			}
		} else {
			log.Printf("[newContainerMountSymlink] error statting prefix %s: %+v", CONTAINER_MOUNT_PREFIX, err)
			return "", err
		}
	}
	parent := containerMntParent(name)
	if _, err := os.Stat(parent); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(parent, 0700); err != nil {
				log.Printf("[newContainerMountSymlink] error creating parent %s: %+v", parent, err)
				return "", err
			}
		} else {
			log.Printf("[newContainerMountSymlink] error statting parent %s: %+v", parent, err)
			return "", err
		}
	}

	// Raw ZFS mountpoint
	mountpoint := containerMnt(name)

	// Only create symlink if it doesn't already exist. Otherwise just hand it back
	// (the target of it may have been updated elsewhere).
	if stat, err := os.Stat(mountpoint); err != nil {
		if os.IsNotExist(err) {
			err = os.Symlink(mnt(filesystemId), mountpoint)
			if err != nil {
				log.Printf("[newContainerMountSymlink] error symlinking mountpoint %s: %+v", mountpoint, err)
				return "", err
			}
		} else {
			log.Printf("[newContainerMountSymlink] error statting mountpoint %s: %+v", mountpoint, err)
			return "", err
		}
	} else {
		// FIXME: Check it really is a symlink. Various bugs lead to a raw directory being here, which then silently breaks things.
		log.Printf("[newContainerMountSymlink] mountpoint %s found: %+v", mountpoint, stat)
	}

	// ...and we return either that raw mountpoint, or a subvolume within
	result := containerMntSubvolume(name, subvolume)

	// Do we need to create the subvolume directory?
	if _, err := os.Stat(result); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(result, 0777); err != nil {
				log.Printf("[newContainerMountSymlink] error creating subdot %s: %+v", result, err)
				return "", err
			}
		} else {
			log.Printf("[newContainerMountSymlink] error statting subdot %s: %+v", result, err)
			return "", err
		}
	}

	log.Printf("[newContainerMountSymlink] returning %s", result)
	return result, nil
}

func (state *InMemoryState) mustCleanupSocket() {
	if _, err := os.Stat(PLUGINS_DIR); err != nil {
		if err := os.MkdirAll(PLUGINS_DIR, 0700); err != nil {
			log.Fatalf("Could not make plugin directory %s: %v", PLUGINS_DIR, err)
		}
	}
	if _, err := os.Stat(DM_SOCKET); err == nil {
		if err = os.Remove(DM_SOCKET); err != nil {
			log.Fatalf("Could not clean up existing socket at %s: %v", DM_SOCKET, err)
		}
	}
}

// Annotate a context with admin-level authorization.
func AdminContext(ctx context.Context) context.Context {
	return auth.SetUserIDCtx(ctx, ADMIN_USER_UUID)
}

func (state *InMemoryState) runPlugin() {
	log.Printf("[runPlugin] Starting dm plugin with socket: %s", DM_SOCKET)

	// docker acts like the admin user, for now.
	ctx := AdminContext(context.Background())

	state.mustCleanupSocket()

	http.HandleFunc("/Plugin.Activate", func(w http.ResponseWriter, r *http.Request) {
		log.Print("<= /Plugin.Activate")
		responseJSON, _ := json.Marshal(&ResponseImplements{
			Implements: []string{"VolumeDriver"},
		})
		log.Printf("=> %s", string(responseJSON))
		w.Write(responseJSON)
	})
	http.HandleFunc("/VolumeDriver.Create", func(w http.ResponseWriter, r *http.Request) {
		log.Print("<= /VolumeDriver.Create")
		requestJSON, err := ioutil.ReadAll(r.Body)
		if err != nil {
			writeResponseErr(err, w)
			return
		}
		request := new(RequestCreate)
		err = json.Unmarshal(requestJSON, request)
		if err != nil {
			writeResponseErr(err, w)
			return
		}
		namespace, localName, _, err := parseNamespacedVolumeWithSubvolumes(request.Name)
		if err != nil {
			writeResponseErr(err, w)
			return
		}

		name := VolumeName{namespace, localName}

		// for now, just name the volumes as requested by the user. later,
		// adding ids and per-fs metadata may be useful.

		if _, err := state.procureFilesystem(ctx, name); err != nil {
			writeResponseErr(err, w)
			return
		}
		// TODO acquire containerRuntimeLock and update our state and etcd with
		// the fact that a container will soon be running on this volume...
		writeResponseOK(w)
		// asynchronously notify dotmesh that the containers running on a
		// volume may have changed
		go func() { state.fetchRelatedContainersChan <- true }()
	})

	http.HandleFunc("/VolumeDriver.Remove", func(w http.ResponseWriter, r *http.Request) {
		/*
			We do not actually want to remove the dm volume when Docker
			references to them are removed.

			This is a no-op.
		*/
		writeResponseOK(w)
		// asynchronously notify dotmesh that the containers running on a
		// volume may have changed
		go func() { state.fetchRelatedContainersChan <- true }()
	})

	http.HandleFunc("/VolumeDriver.Path", func(w http.ResponseWriter, r *http.Request) {
		// TODO: Only return the path if it's actually active on the local host.
		log.Print("<= /VolumeDriver.Path")
		requestJSON, err := ioutil.ReadAll(r.Body)
		if err != nil {
			writeResponseErr(err, w)
			return
		}
		request := new(RequestMount)
		if err := json.Unmarshal(requestJSON, request); err != nil {
			writeResponseErr(err, w)
			return
		}
		namespace, localName, subvolume, err := parseNamespacedVolumeWithSubvolumes(request.Name)
		if err != nil {
			writeResponseErr(err, w)
			return
		}

		name := VolumeName{
			Namespace: namespace,
			Name:      localName,
		}
		mountPoint := containerMntSubvolume(name, subvolume)

		log.Printf("Mountpoint for %s: %s", name, mountPoint)
		responseJSON, _ := json.Marshal(&ResponseMount{
			Mountpoint: mountPoint,
			Err:        "",
		})
		log.Printf("=> %s", string(responseJSON))
		w.Write(responseJSON)
		// asynchronously notify dotmesh that the containers running on a
		// volume may have changed
		go func() { state.fetchRelatedContainersChan <- true }()
	})

	http.HandleFunc("/VolumeDriver.Mount", func(w http.ResponseWriter, r *http.Request) {
		// TODO acquire containerRuntimeLock and update our state and etcd with
		// the fact that a container will soon be running on this volume...
		log.Print("<= /VolumeDriver.Mount")
		requestJSON, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatalf("Unable to read response body %s", err)
		}
		request := new(RequestMount)
		if err := json.Unmarshal(requestJSON, request); err != nil {
			writeResponseErr(err, w)
			return
		}

		namespace, localName, subvolume, err := parseNamespacedVolumeWithSubvolumes(request.Name)
		if err != nil {
			writeResponseErr(err, w)
			return
		}

		name := VolumeName{Namespace: namespace, Name: localName}

		filesystemId, err := state.procureFilesystem(ctx, name)
		if err != nil {
			writeResponseErr(err, w)
			return
		}
		mountpoint, err := newContainerMountSymlink(name, filesystemId, subvolume)
		if err != nil {
			writeResponseErr(err, w)
			return
		}
		// Allow things that don't want containers to start during their
		// operations to delay the start of a container. Commented out because
		// it causes a deadlock.
		/*
			state.containersLock.Lock()
			defer state.containersLock.Unlock()
		*/

		log.Printf("Mountpoint for %s: %s", name, mountpoint)
		responseJSON, _ := json.Marshal(&ResponseMount{
			Mountpoint: mountpoint,
			Err:        "",
		})
		log.Printf("=> %s", string(responseJSON))
		w.Write(responseJSON)

		// asynchronously notify dotmesh that the containers running on a
		// volume may have changed
		go func() { state.fetchRelatedContainersChan <- true }()
		go func() {
			// Do this again a second later, to cope with Docker's lack of
			// immediate consistency
			time.Sleep(time.Second)
			state.fetchRelatedContainersChan <- true
		}()
	})

	http.HandleFunc("/VolumeDriver.Unmount", func(w http.ResponseWriter, r *http.Request) {
		// TODO acquire containerRuntimeLock and update our state and etcd with
		// the fact that one less container is now running on this volume...
		writeResponseOK(w)
		// asynchronously notify dotmesh that the containers running on a
		// volume may have changed
		go func() { state.fetchRelatedContainersChan <- true }()
		go func() {
			// Do this again a second later, to cope with Docker's lack of
			// immediate consistency
			time.Sleep(time.Second)
			state.fetchRelatedContainersChan <- true
		}()
	})

	http.HandleFunc("/VolumeDriver.List", func(w http.ResponseWriter, r *http.Request) {
		log.Print("<= /VolumeDriver.List")
		var response = ResponseList{
			Err: "",
		}

		for _, fs := range (*state).registry.Filesystems() {
			log.Printf("Mountpoint for %s: %s", fs, containerMnt(fs))
			response.Volumes = append(response.Volumes, ResponseListVolume{
				Name:       fs.StringWithoutAdmin(),
				Mountpoint: containerMnt(fs),
			})
		}

		responseJSON, _ := json.Marshal(response)
		log.Printf("=> %s", string(responseJSON))
		w.Write(responseJSON)
		// asynchronously notify dotmesh that the containers running on a
		// volume may have changed
		go func() { state.fetchRelatedContainersChan <- true }()
	})
	http.HandleFunc("/VolumeDriver.Get", func(w http.ResponseWriter, r *http.Request) {
		log.Print("<= /VolumeDriver.Get")
		requestJSON, err := ioutil.ReadAll(r.Body)
		if err != nil {
			writeResponseErr(err, w)
			return
		}
		request := new(RequestMount)
		if err := json.Unmarshal(requestJSON, request); err != nil {
			writeResponseErr(err, w)
			return
		}
		namespace, localName, subvolume, err := parseNamespacedVolumeWithSubvolumes(request.Name)
		if err != nil {
			writeResponseErr(err, w)
			return
		}

		name := VolumeName{Namespace: namespace, Name: localName}

		var response = ResponseGet{
			Err: "",
		}

		// Technically, fetching the TopLevelFilesystem object from the
		// registry isn't necessary, but maybe one day we'll get additional
		// Status information from that call that we want to use here, so
		// leaving it in for now rather than just hand-constructing the
		// response from the name.
		fs, err := (*state).registry.GetByName(name)
		if err != nil {
			response.Err = fmt.Sprintf("Error getting volume: %v", err)
		}

		mountpoint := containerMntSubvolume(fs.MasterBranch.Name, subvolume)
		log.Printf("Mountpoint for %s (%+v): %s", request.Name, fs, mountpoint)
		response.Volume = ResponseListVolume{
			Name:       request.Name,
			Mountpoint: mountpoint,
		}

		responseJSON, _ := json.Marshal(response)
		log.Printf("=> %s", string(responseJSON))
		w.Write(responseJSON)
		// asynchronously notify dotmesh that the containers running on a
		// volume may have changed
		go func() { state.fetchRelatedContainersChan <- true }()
	})

	listener, err := net.Listen("unix", DM_SOCKET)
	if err != nil {
		log.Fatalf("[runPlugin] Could not listen on %s: %v", DM_SOCKET, err)
	}

	http.Serve(listener, nil)
}

func (state *InMemoryState) runErrorPlugin() {
	// A variant of the normal plugin which just returns immediately with
	// errors. For bootstrapping.
	log.Printf("[bootstrap] Starting dm temporary bootstrap plugin on %s", DM_SOCKET)
	state.mustCleanupSocket()
	http.HandleFunc("/Plugin.Activate", func(w http.ResponseWriter, r *http.Request) {
		log.Print("[bootstrap] /Plugin.Activate")
		responseJSON, _ := json.Marshal(&ResponseImplements{
			Implements: []string{"VolumeDriver"},
		})
		w.Write(responseJSON)
	})
	http.HandleFunc("/VolumeDriver.Create", func(w http.ResponseWriter, r *http.Request) {
		log.Print("[bootstrap] /VolumeDriver.Create")
		writeResponseErr(fmt.Errorf("I'm sorry Dave, I can't do that. I'm still starting up."), w)
	})
	http.HandleFunc("/VolumeDriver.Remove", func(w http.ResponseWriter, r *http.Request) {
		log.Print("[bootstrap] /VolumeDriver.Remove")
		writeResponseOK(w)
	})
	http.HandleFunc("/VolumeDriver.Path", func(w http.ResponseWriter, r *http.Request) {
		log.Print("[bootstrap] /VolumeDriver.Path")
		requestJSON, err := ioutil.ReadAll(r.Body)
		if err != nil {
			writeResponseErr(err, w)
			return
		}
		request := new(RequestMount)
		if err := json.Unmarshal(requestJSON, request); err != nil {
			writeResponseErr(err, w)
			return
		}

		namespace, localName, subvolume, err := parseNamespacedVolumeWithSubvolumes(request.Name)
		if err != nil {
			writeResponseErr(err, w)
			return
		}

		name := VolumeName{namespace, localName}
		mountpoint := containerMntSubvolume(name, subvolume)
		log.Printf("Mountpoint for %s: %s", name, mountpoint)
		responseJSON, _ := json.Marshal(&ResponseMount{
			Mountpoint: mountpoint,
			Err:        "",
		})
		log.Printf("=> %s", string(responseJSON))
		w.Write(responseJSON)
	})
	http.HandleFunc("/VolumeDriver.Mount", func(w http.ResponseWriter, r *http.Request) {
		log.Print("[bootstrap] /VolumeDriver.Mount")
		writeResponseErr(fmt.Errorf("dotmesh still starting or dotmesh-etcd unable to achieve quorum"), w)
	})
	http.HandleFunc("/VolumeDriver.Unmount", func(w http.ResponseWriter, r *http.Request) {
		log.Print("[bootstrap] /VolumeDriver.Unmount")
		writeResponseErr(fmt.Errorf("dotmesh still starting or dotmesh-etcd unable to achieve quorum"), w)
	})
	http.HandleFunc("/VolumeDriver.List", func(w http.ResponseWriter, r *http.Request) {
		log.Print("[bootstrap] /VolumeDriver.List")
		var response = ResponseList{
			Err: "dotmesh still starting or dotmesh-etcd unable to achieve quorum",
		}
		responseJSON, _ := json.Marshal(response)
		w.Write(responseJSON)
	})
	listener, err := net.Listen("unix", DM_SOCKET)
	if err != nil {
		log.Fatalf("[bootstrap] Could not listen on %s: %v", DM_SOCKET, err)
	}
	http.Serve(listener, nil)
}

func writeResponseOK(w http.ResponseWriter) {
	// A shortcut to writing a ResponseOK to w
	responseJSON, _ := json.Marshal(&ResponseSimple{Err: ""})
	w.Write(responseJSON)
}

func writeResponseErr(err error, w http.ResponseWriter) {
	// A shortcut to responding with an error, and then log the error
	errString := fmt.Sprintln(err)
	log.Printf("Error: %v", err)
	responseJSON, _ := json.Marshal(&ResponseSimple{Err: errString})
	w.Write(responseJSON)
}

func (state *InMemoryState) cleanupDockerFilesystemState() error {
	err := filepath.Walk(CONTAINER_MOUNT_PREFIX, func(symlinkPath string, info os.FileInfo, err error) error {
		if info == nil {
			log.Printf("[cleanupDockerFilesystemState] found something with no fileinfo: %s", symlinkPath)
		} else {
			if !info.IsDir() {
				target, err := os.Readlink(symlinkPath)
				log.Printf("[cleanupDockerFilesystemState] Found %s -> %s", symlinkPath, target)
				if err != nil {
					if os.IsNotExist(err) {
						// It's already gone, nothing to clean up.
					} else {
						// Some other error happened, that's not good.
						return err
					}
				} else {
					fsid, err := unmnt(target)
					log.Printf("[cleanupDockerFilesystemState] Found %s -> %s extracted fsid %s", symlinkPath, target, fsid)
					if err != nil {
						return err
					}

					deleted, err := isFilesystemDeletedInEtcd(fsid)
					if err != nil {
						return err
					}

					if deleted {
						log.Printf("[cleanupDockerFilesystemState] %s -> %s -> %s - deleting", symlinkPath, target, fsid)
						if err := os.Remove(symlinkPath); err != nil {
							return err
						}
					} else {
						// Do nothing; the symlink has been taken over by another filesystem, and points to a new non-deleted fs.
					}
				}
			}
		}

		return nil
	})
	return err
}
