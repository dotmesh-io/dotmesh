package main

// docker client for finding containers which are using dm volumes, and
// stopping and starting containers.

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/fsouza/go-dockerclient"
)

type DockerContainer struct {
	Name string
	Id   string
}

type DockerClient struct {
	// TODO move the map to fsMachine
	client            *docker.Client
	containersStopped map[string]map[string]string
}

type NotLocked struct {
	volumeName string
}

func (n NotLocked) Error() string {
	return fmt.Sprintf("%s not locked when tried to unlock", n.volumeName)
}

type AlreadyLocked struct {
	volumeName string
}

func (a AlreadyLocked) Error() string {
	return fmt.Sprintf("%s already locked when tried to lock", a.volumeName)
}

// AllRelated returns every running container that is using any dotmesh
// filesystem, as a map from filesystem ids to lists of such containers
func (d *DockerClient) AllRelated() (map[string][]DockerContainer, error) {
	relatedContainers := map[string][]DockerContainer{}
	containers, err := d.client.ListContainers(docker.ListContainersOptions{})
	if err != nil {
		return relatedContainers, err
	}
	for _, c := range containers {
		container, err := d.client.InspectContainer(c.ID)
		if err != nil {
			return relatedContainers, err
		}
		if container.State.Running {
			filesystems, err := d.relatedFilesystems(container)
			if err != nil {
				return map[string][]DockerContainer{}, err
			}
			for _, filesystem := range filesystems {
				_, ok := relatedContainers[filesystem]
				if !ok {
					relatedContainers[filesystem] = []DockerContainer{}
				}
				relatedContainers[filesystem] = append(
					relatedContainers[filesystem],
					DockerContainer{Id: container.ID, Name: container.Name},
				)
			}
		}
	}
	return relatedContainers, nil
}

func baseDotName(dotName string) string {
	// Given a dot name that may include bells and whistles like
	// "dot@branch.subdot" and so forth (or "dot.subdot@branch", I can't
	// remember), strip off the @s and .s, yielding just the base subdot name.
	// This is particularly useful in comparisons with mount.Name from Docker.
	if strings.Contains(dotName, "@") {
		shrapnel := strings.Split(dotName, "@")
		dotName = shrapnel[0]
	}
	if strings.Contains(dotName, ".") {
		shrapnel := strings.Split(dotName, ".")
		dotName = shrapnel[0]
	}
	return dotName
}

func containerRelated(volumeName string, container *docker.Container) bool {
	for _, m := range container.Mounts {
		// split off "@" in mount name, in case of pinned branches. we want to
		// stop those containers too. (and later we may want to be more precise
		// about not stopping containers that are currently using other
		// branches)
		mountName := baseDotName(m.Name)
		if m.Driver == "dm" && mountName == volumeName {
			return true
		}
	}
	return false
}

// Given a dm container mount path, find the path of the root dot
// The path may already be the root path, or it may be some subdot bneeath it.
// Paths look like CONTAINER_MOUNT_PREFIX/namespace/volume[/subdot]
func findDotRoot(path string) string {
	if !strings.HasPrefix(path, CONTAINER_MOUNT_PREFIX+"/") {
		log.Printf("[findDotRoot] Container mount path %v doesn't start with %v/", path, CONTAINER_MOUNT_PREFIX)
		return path
	}
	subpath := strings.TrimPrefix(path, CONTAINER_MOUNT_PREFIX+"/")
	parts := strings.Split(subpath, "/")
	switch len(parts) {
	case 2: // no subdot
		return path
	case 3: // subdot
		return CONTAINER_MOUNT_PREFIX + "/" + parts[0] + "/" + parts[1]
	default:
		log.Printf("[findDotRoot] Container mount path %v didn't have the usual number of parts: %+v %+v", path, subpath, parts)
		return path
	}
}

// Given a container, return a list of filesystem ids of dotmesh volumes that
// are currently in-use by it (by resolving the symlinks of its mount sources).
func (d *DockerClient) relatedFilesystems(container *docker.Container) ([]string, error) {
	result := []string{}
	for _, mount := range container.Mounts {
		if mount.Driver != "dm" {
			continue
		}
		target, err := os.Readlink(findDotRoot(mount.Source))
		if err != nil {
			log.Printf("Error trying to read symlink '%s', skipping: %s", mount.Source, err)
			continue
		}
		// target will be like
		// /var/lib/dotmesh/mnt/dmfs/9e394010-0f2b-481d-779d-d81c2d4f51fb
		log.Printf("[relatedFilesystems] target = %s\n", target)
		shrapnel := strings.Split(target, "/")
		if len(shrapnel) > 1 {
			filesystemId := shrapnel[len(shrapnel)-1]
			result = append(result, filesystemId)
		}
	}
	return result, nil
}

func NewDockerClient() (*DockerClient, error) {
	client, err := docker.NewClientFromEnv()
	if err != nil {
		return nil, err
	}
	stopped := map[string]map[string]string{}
	return &DockerClient{client, stopped}, nil
}

func (d *DockerClient) Related(volumeName string) ([]DockerContainer, error) {
	related := []DockerContainer{}
	// default to using docker runtime, but also allow specifying.
	if os.Getenv("CONTAINER_RUNTIME") == "" || os.Getenv("CONTAINER_RUNTIME") == "docker" {
		cs, err := d.client.ListContainers(docker.ListContainersOptions{})
		if err != nil {
			return related, err
		}
		done := map[string]bool{} // Kinda like a set.
		for _, c := range cs {
			container, err := d.client.InspectContainer(c.ID)
			if err != nil {
				return related, err
			}
			if container.State.Running && containerRelated(volumeName, container) {
				// Only append if it's not already done.
				if _, ok := done[container.ID]; !ok {
					related = append(
						related, DockerContainer{
							Id: container.ID, Name: container.Name,
						},
					)
					done[container.ID] = true
				}
			}
		}
		log.Printf("[Related] Containers related to volume %+v: %+v", volumeName, related)
	} else if os.Getenv("CONTAINER_RUNTIME") == "null" {
		// do nothing
	} else {
		return related, fmt.Errorf("Unsupported container runtime %v", os.Getenv("CONTAINER_RUNTIME"))
	}
	return related, nil
}

func (d *DockerClient) SwitchSymlinks(volumeName, toFilesystemIdPath string) error {
	// iterate over all the containers, finding mounts where the name of the
	// mount is volumeName. assuming the container is stopped, unlink the
	// symlink and create a new one, with the same filename, pointing to
	// toFilesystemIdPath
	containers, err := d.client.ListContainers(
		docker.ListContainersOptions{All: true},
	)
	if err != nil {
		return err
	}
	// TODO something like the following structure (+locking) can also be used
	// for "cleaning up" stale /dotmesh mount symlinks, say every 60 seconds,
	// or when docker says unmount/remove.

	for _, c := range containers {
		container, err := d.client.InspectContainer(c.ID)
		if err != nil {
			return err
		}
		for _, mount := range container.Mounts {
			if mount.Driver == "dm" {
				// TODO the only purpose for this Readlink call is to check
				// whether it's a symlink before trying os.Remove. maybe we can
				// check whether it's a symlink with Stat instead.

				mountPoint := findDotRoot(mount.Source)

				_, err := os.Readlink(mountPoint)
				if err != nil {
					log.Printf("Error trying to read symlink '%s', skipping: %s", mountPoint, err)
					continue
				}
				if baseDotName(mount.Name) == volumeName {
					// TODO could also check whether containerMnt(volumeName) == mount.Source. should we?
					if container.State.Running {
						return fmt.Errorf(
							"Container %s was running when you asked me to switch its symlinks (%s => %s)",
							c.ID, mountPoint, toFilesystemIdPath,
						)
					}
					// ok, container is not running, and we know the symlink to
					// switcharoo.
					log.Printf("Switching %s to %s", mountPoint, toFilesystemIdPath)
					if err := os.Remove(mountPoint); err != nil {
						return err
					}
					if err := os.Symlink(toFilesystemIdPath, mountPoint); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (d *DockerClient) Start(volumeName string) error {
	stopped, ok := d.containersStopped[volumeName]
	if !ok {
		return NotLocked{volumeName: volumeName}
	}
	for containerId := range stopped {
		err := d.client.StartContainer(containerId, nil)
		if err != nil {
			return err
		}
	}
	delete(d.containersStopped, volumeName)
	return nil
}

func (d *DockerClient) Stop(volumeName string) error {
	_, ok := d.containersStopped[volumeName]
	if ok {
		return AlreadyLocked{volumeName: volumeName}
	}
	relatedContainers, err := d.Related(volumeName)
	if err != nil {
		return err
	}
	d.containersStopped[volumeName] = map[string]string{}

	for _, container := range relatedContainers {
		err = func() error {
			var err error
			for i := 0; i < 10; i++ {
				err = d.client.StopContainer(container.Id, 10)
				if err == nil {
					return nil
				}
			}
			return err
		}()
		if err != nil {
			log.Printf("[Stop] Error stopping container %s: %+v", container.Id, err)
			// Ignore error and proceed to deal with other containers;
			// the underlying container may have already stopped itself.
		}
		d.containersStopped[volumeName][container.Id] = container.Name
	}
	return nil
}
