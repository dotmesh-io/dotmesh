package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

func (s *InMemoryState) repl() {
	scanner := bufio.NewScanner(os.Stdin)
	log.Print("Starting a scanner...")
	prompt := func() { os.Stdout.Write([]byte("dotmesh> ")) }
	prompt()
	for scanner.Scan() {
		// some human interaction commands
		text := scanner.Text()
		words := strings.Fields(string(text))
		if len(words) > 0 {
			cmd := words[0]
			// TODO these case bodies should be function calls
			switch cmd {
			case "exit":
				os.Exit(0)
			case "status":
				out("My ID:", s.myNodeId, "\n")
				out("Known servers:\n")
				s.serverAddressesCacheLock.Lock()
				out("SERVER ID\tSERVER IP\n")
				for server, addresses := range *s.serverAddressesCache {
					out(server, "\t", addresses, "\n")
				}
				s.serverAddressesCacheLock.Unlock()
			case "help":
				out("Try:\n" +
					"exit - leave dotmesh\n" +
					"create - create a new fs\n" +
					"list - view filesystems\n" +
					"unmount <fs> - unmount a given filesystem\n" +
					"mount <fs> - mount a given filesystem\n" +
					"snapshot <fs> key1=value,key2=value - take snapshot with metadata\n" +
					"status - my ID and other known servers\n" +
					"promote <fs> <server_id> - make a filesystem be a master on a server, possibly incurring data loss\n" +
					"locks - show which global locks are currently held\n",
				)
			case "locks":
				out("filesystemsLock", s.filesystemsLock, "\n")
				out("mastersCacheLock", s.mastersCacheLock, "\n")
				out("serverAddressesCacheLock", s.serverAddressesCacheLock, "\n")
				out("globalSnapshotCacheLock", s.globalSnapshotCacheLock, "\n")
				out("globalStateCacheLock", s.globalSnapshotCacheLock, "\n")
				out("\n")
				filesystems := []string{}
				s.filesystemsLock.Lock()
				for _, fs := range *s.filesystems {
					filesystems = append(filesystems, fs.filesystemId)
				}
				sort.Strings(filesystems)
				for _, fsName := range filesystems {
					out("-", fsName, (*s.filesystems)[fsName].snapshotsLock, "\n")
				}
				out("\n")
				s.filesystemsLock.Unlock()
			case "create":
				if len(words) == 1 || len(words) == 2 {
					var fsMachine *fsMachine
					var ch chan *Event
					var err error
					if len(words) == 1 {
						fsMachine, ch, err = s.CreateFilesystem(AdminContext(context.TODO()), nil)
						if err != nil {
							out("Error:", err)
							break
						}
					} else if len(words) == 2 {
						namespace, localName, err := parseNamespacedVolume(words[1])
						if err != nil {
							out("Error:", err)
							return
						}

						name := VolumeName{namespace, localName}

						fsMachine, ch, err = s.CreateFilesystem(AdminContext(context.TODO()), &name)
						if err != nil {
							out("Error:", err)
							break
						}
					}
					if err == nil {
						e := <-ch
						if e.Name == "created" {
							out("Created", fsMachine.filesystem.id, "\n")
						} else {
							out("Unexpected response", e.Name, "-", e.Args, "\n")
						}
					}
				} else {
					out("Create optionally takes id as argument")
				}
			case "unmount":
				if len(words) != 2 {
					out("please specify only the filesystem id as an argument\n")
				} else {
					filesystemId := words[1]
					_, err := s.maybeFilesystem(filesystemId)
					if err != nil {
						out(err)
						break
					}
					ch, err := s.dispatchEvent(filesystemId, &Event{Name: "unmount"}, "")
					if err != nil {
						out("error!", err)
					}
					e := <-ch
					if e.Name == "unmounted" {
						out("Unmounted", filesystemId, "\n")
					} else {
						out("Unexpected response", e.Name, "-", e.Args, "\n")
					}
				}
			case "mount":
				if len(words) != 2 {
					out("please specify only the filesystem id as an argument\n")
				} else {
					filesystemId := words[1]
					_, err := s.maybeFilesystem(filesystemId)
					if err != nil {
						out(err)
						break
					}

					ch, err := s.dispatchEvent(filesystemId, &Event{Name: "mount"}, "")
					if err != nil {
						out("error!", err)
					}
					e := <-ch

					if e.Name == "mounted" {
						out("Mounted", filesystemId, "\n")
					} else {
						out("Unexpected response", e.Name, "-", e.Args, "\n")
					}
				}
			case "snapshot":
				if len(words) > 3 {
					out("please specify the filesystem id as an argument, " +
						"with optional comma/equals deliminated metadata\n")
				} else {
					meta := metadata{}
					if len(words) == 3 {
						// parse a=b,c=d
						for _, body := range strings.Split(words[2], ",") {
							items := strings.Split(body, "=")
							meta[items[0]] = items[1]
						}
					}
					filesystemId := words[1]
					_, err := s.maybeFilesystem(filesystemId)
					if err != nil {
						out(err)
						break
					}
					ch, err := s.dispatchEvent(filesystemId, &Event{Name: "snapshot",
						Args: &EventArgs{"metadata": meta}}, "")
					if err != nil {
						out("error!", err)
					}
					e := <-ch
					if e.Name == "snapshotted" {
						out("Snapshotted", filesystemId, "\n")
					} else {
						out("Unexpected response", e.Name, "-", e.Args, "\n")
					}
				}
			case "masters":
				s.mastersCacheLock.Lock()
				s.serverAddressesCacheLock.Lock()
				out("FILESYSTEM\tSERVER ID\tSERVER IP\n")
				for fs, master := range *s.mastersCache {
					serverAddress, ok := (*s.serverAddressesCache)[master]
					if !ok {
						serverAddress = "<unknown>"
					}
					out(fs, "\t", master, "\t", serverAddress, "\n")
				}
				s.mastersCacheLock.Unlock()
				s.serverAddressesCacheLock.Unlock()
			case "promote":
				// TODO maybe move setting a master into controller.go
				kapi, err := getEtcdKeysApi()
				if err != nil {
					out(err)
					break
				}
				if len(words) != 3 {
					out("Usage: promote <fs> <server_id> - make a filesystem be a master on a server, possibly incurring data loss. See also 'status' command. Only use this if you know what you're doing.\n")
					break
				}
				fs := words[1]
				serverId := words[2]
				// TODO validate that serverId is known
				_, err = kapi.Set(
					context.Background(),
					fmt.Sprintf("%s/filesystems/masters/%s", ETCD_PREFIX, fs),
					serverId,
					nil,
				)
				if err != nil {
					out(err)
					break
				}
			case "get":
				if len(words) != 2 {
					out("please specify only the filesystem id as an argument\n")
				} else {
					s.filesystemsLock.Lock()
					fs := words[1]
					filesystem, ok := (*s.filesystems)[fs]
					if !ok {
						out("no such filesystem\n")
					} else {
						out("snapshots:\n")
						filesystem.snapshotsLock.Lock()
						for _, snapshot := range filesystem.filesystem.snapshots {
							out(
								"- id:", snapshot.Id, "\n",
								" metadata:", snapshot.Metadata, "\n",
							)
						}
						filesystem.snapshotsLock.Unlock()
					}
					s.filesystemsLock.Unlock()
				}
			case "list":
				filesystems := []string{}
				s.filesystemsLock.Lock()
				for _, fs := range *s.filesystems {
					filesystems = append(filesystems, fs.filesystemId)
				}
				sort.Strings(filesystems)
				for _, fsName := range filesystems {
					fs := (*s.filesystems)[fsName]
					fs.snapshotsLock.Lock()
					numSnapshots := len(fs.filesystem.snapshots)
					out(
						fmt.Sprintf("%-32s", fs.filesystem.id),
						"\texists:", fs.filesystem.exists,
						"\tmounted:", fs.filesystem.mounted,
						"\tstate:",
						fmt.Sprintf("%-8s", fs.currentState),
						"\tstatus:",
						fs.status,
						"\tsnapshots:",
						numSnapshots,
						"\n",
					)
					fs.snapshotsLock.Unlock()
				}
				s.filesystemsLock.Unlock()
			case "refresh":
				s.fetchRelatedContainersChan <- true
			case "containers":
				s.globalContainerCacheLock.Lock()
				for k, v := range *s.globalContainerCache {
					out(fmt.Sprintf("%s => %s\n", k, v))
				}
				s.globalContainerCacheLock.Unlock()
			default:
				out("No such command, try help\n")
			}
		}
		prompt()
	}
}
