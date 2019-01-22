package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/portworx/kvdb"

	"github.com/coreos/etcd/client"
	"github.com/dotmesh-io/dotmesh/pkg/store"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
	"github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"

	log "github.com/sirupsen/logrus"
)

func getKVDBCfg() *store.KVDBConfig {
	endpoint := os.Getenv("DOTMESH_ETCD_ENDPOINT")
	if endpoint == "" {
		endpoint = "https://dotmesh-etcd:42379"
	}
	options := make(map[string]string)
	if strings.HasPrefix(endpoint, "https://") {
		pkiPath := os.Getenv("DOTMESH_PKI_PATH")
		if pkiPath == "" {
			pkiPath = "/pki"
		}

		options[kvdb.CAFileKey] = fmt.Sprintf("%s/ca.pem", pkiPath)
		options[kvdb.CertKeyFileKey] = fmt.Sprintf("%s/apiserver-key.pem", pkiPath)
		options[kvdb.CertFileKey] = fmt.Sprintf("%s/apiserver.pem", pkiPath)
	}
	cfg := &store.KVDBConfig{
		Machines: []string{endpoint},
		Type:     store.KVTypeEtcdV3,
		Options:  options,
		Prefix:   types.EtcdPrefix,
	}
	return cfg
}

func getKVDBStores() (store.FilesystemStore, store.RegistryStore, store.ServerStore, store.KVStoreWithIndex) {

	cfg := getKVDBCfg()
	kvdbStore, err := store.NewKVDBFilesystemStore(cfg)
	if err != nil {
		log.WithFields(log.Fields{
			"options":  cfg.Options,
			"endpoint": cfg.Machines,
			"error":    err,
		}).Fatalf("failed to setup KV store")
	}
	kvdbIndexStore, err := store.NewKVDBStoreWithIndex(cfg, user.UsersPrefix)
	if err != nil {
		log.WithFields(log.Fields{
			"options":  cfg.Options,
			"endpoint": cfg.Machines,
			"error":    err,
		}).Fatalf("failed to setup KV index store")
	}
	serverStore, err := store.NewKVServerStore(cfg)
	if err != nil {
		log.WithFields(log.Fields{
			"options":  cfg.Options,
			"endpoint": cfg.Machines,
			"error":    err,
		}).Fatalf("failed to setup server store")
	}
	return kvdbStore, kvdbStore, serverStore, kvdbIndexStore
}

var onceAgain Once

func transportFromTLS(certFile, keyFile, caFile string) (*http.Transport, error) {
	// Load client cert
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}
	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	return transport, nil
}

// TODO: why?
func guessIPv4Addresses() ([]string, error) {
	override := os.Getenv("YOUR_IPV4_ADDRS")
	if override != "" {
		return strings.Split(override, ","), nil
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return []string{}, err
	}
	addresses := []string{}
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			return []string{}, err
		}
		for _, a := range addrs {
			if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				// ignore docker and container interfaces, they are unlikely to
				// be routable
				if !(strings.HasPrefix(i.Name, "docker") || strings.HasPrefix(i.Name, "veth")) {
					// log.Printf("Found address %s with address %s", i.Name, ipnet.IP.String())
					if !strings.Contains(ipnet.IP.String(), ":") {
						addresses = append(addresses, ipnet.IP.String())
					}
				}
			}
		}
	}
	return addresses, nil
}

// etcd listener
func (s *InMemoryState) updateAddressesInEtcd() error {
	addresses, err := guessIPv4Addresses()
	if err != nil {
		return err
	}

	return s.serverStore.SetAddresses(&types.Server{
		Addresses: addresses,
		Id:        s.NodeID(),
	}, &store.SetOptions{
		TTL: 60,
	})
}

func (s *InMemoryState) isFilesystemDeletedInEtcd(fsId string) (bool, error) {

	_, err := s.filesystemStore.GetDeleted(fsId)
	if err != nil {
		if store.IsKeyNotFound(err) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func (s *InMemoryState) markFilesystemAsDeletedInEtcd(fsId, username string, name VolumeName, tlFsId, branch string) error {

	at := &types.FilesystemDeletionAudit{
		FilesystemID:         fsId,
		Server:               s.NodeID(),
		Username:             username,
		DeletedAt:            time.Now(),
		Name:                 name,
		TopLevelFilesystemId: tlFsId,
		Clone:                branch,
	}

	err := s.filesystemStore.SetDeleted(at, &store.SetOptions{})
	if err != nil {
		return err
	}

	return s.filesystemStore.SetCleanupPending(at, &store.SetOptions{})
}

// This struct is a subset of the struct used as the audit trail in
// markFilesystemAsDeletedInEtcd.  A subset is used here to avoid
// issues with upgrading a running cluster, if old deletion audit
// trails are in etcd with different state. These are the only things
// we need to read back.
type NameOrClone struct {
	Name                 VolumeName
	TopLevelFilesystemId string
	Clone                string
}

func (s *InMemoryState) cleanupDeletedFilesystems() error {

	pending, err := s.listFilesystemsPendingCleanup()
	if err != nil {
		return err
	}

	for fsId, deletionAudit := range pending {
		var errors []error

		err = s.filesystemStore.DeleteContainers(fsId)
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": fsId,
			}).Error("[cleanupDeletedFilesystems] failed to delete filesystem containers during cleanup")
		}
		err = s.filesystemStore.DeleteMaster(fsId)
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": fsId,
			}).Error("[cleanupDeletedFilesystems] failed to delete filesystem master info during cleanup")
		}
		err = s.filesystemStore.DeleteDirty(fsId)
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": fsId,
			}).Error("[cleanupDeletedFilesystems] failed to delete filesystem dirty info during cleanup")
		}

		if deletionAudit.Name.Namespace != "" && deletionAudit.Name.Name != "" {
			// The name might be blank in the audit trail - this is used
			// to indicate that this was a clone, NOT the toplevel filesystem, so
			// there's no need to remove the registry entry for the whole
			// volume. We only do that when deleting the toplevel filesystem.

			// Normally, the registry entry is deleted as soon as the volume
			// is deleted, but in the event of a failure it might not have
			// been. So we try again.

			registryFilesystem, err := s.registryStore.GetFilesystem(
				deletionAudit.Name.Namespace,
				deletionAudit.Name.Name,
			)

			if err != nil {
				if store.IsKeyNotFound(err) {
					// we are good, it doesn't exist, nothing to delete
				} else {
					errors = append(errors, err)
				}
			} else {
				// We have an existing registry entry, but is it the one
				// we're supposed to delete, or a newly-created volume
				// with the name of the deleted one?

				if registryFilesystem.Id == fsId {
					err = s.registryStore.DeleteFilesystem(
						deletionAudit.Name.Namespace,
						deletionAudit.Name.Name,
					)
					if err != nil && !store.IsKeyNotFound(err) {
						errors = append(errors, err)
					}
				}
			}
		}

		if deletionAudit.Clone != "" {
			// The clone name might be blank in the audit trail - this is
			// used to indicate that this was the toplevel filesystem
			// rather than a clone. But when a clone name is specified,
			// we need to delete a clone record from etc.
			err = s.registryStore.DeleteClone(
				deletionAudit.TopLevelFilesystemId,
				deletionAudit.Clone,
			)
			if err != nil && !store.IsKeyNotFound(err) {
				errors = append(errors, err)
			}
		}

		if len(errors) == 0 {
			err = s.filesystemStore.DeleteCleanupPending(fsId)
			if err != nil {
				log.WithFields(log.Fields{
					"error":         err,
					"filesystem_id": fsId,
				}).Error("[cleanupDeletedFilesystems] failed to remove 'cleanupPending' filesystem after the cleanup")
			}
		} else {
			return fmt.Errorf("Errors found cleaning up after a deleted filesystem: %+v", errors)
		}
	}

	return nil
}

// The result is a map from filesystem ID to the VolumeName or branch name it once had.
func (s *InMemoryState) listFilesystemsPendingCleanup() (map[string]*types.FilesystemDeletionAudit, error) {
	result := make(map[string]*types.FilesystemDeletionAudit)
	pending, err := s.filesystemStore.ListCleanupPending()
	if err != nil {
		if store.IsKeyNotFound(err) {
			return result, nil
		} else {
			return result, err
		}
	}

	for _, at := range pending {
		_, err := s.filesystemStore.GetLive(at.FilesystemID)
		if err == nil {
			// key found, nothing to do
			continue
		}
		if store.IsKeyNotFound(err) {
			result[at.FilesystemID] = at
		} else {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": at.FilesystemID,
			}).Error("[listFilesystemsPendingCleanup] unexpected error while checking if filesystem is live")
		}
	}

	return result, nil
}

func (s *InMemoryState) MarkFilesystemAsLiveInEtcd(topLevelFilesystemId string) error {
	return s.filesystemStore.SetLive(&types.FilesystemLive{
		FilesystemID: topLevelFilesystemId,
		NodeID:       s.NodeID(),
	}, &store.SetOptions{
		TTL: uint64(s.config.FilesystemMetadataTimeout),
	})
}

// shortcut for dispatching an event to a filesystem's fsMachine's event
// stream, returning the event stream for convenience so the caller can listen
// for a response
func (s *InMemoryState) dispatchEvent(filesystem string, e *types.Event, requestId string) (chan *types.Event, error) {
	fs, err := s.InitFilesystemMachine(filesystem)
	if err != nil {
		return nil, err
	}

	return fs.Submit(e, requestId)
}

func (s *InMemoryState) handleOneFilesystemMaster(node *client.Node) error {
	if node.Value == "" {
		// The filesystem is being deleted, and we need do nothing about it
	} else {
		pieces := strings.Split(node.Key, "/")
		fs := pieces[len(pieces)-1]

		var err error
		var deleted bool

		deleted, err = s.isFilesystemDeletedInEtcd(fs)
		if err != nil {
			log.Errorf("[handleOneFilesystemMaster] error determining if file system is deleted: fs: %s, etcd nodeValue: %s, error: %+v", fs, node.Value, err)
			return err
		}
		if deleted {
			log.Printf("[handleOneFilesystemMaster] filesystem is deleted so no need to mount/unmount fs: %s, etcd nodeValue: %s", fs, node.Value)
			// Filesystem is being deleted, so ignore it.
			return nil
		}

		_, err = s.InitFilesystemMachine(fs)
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": fs,
			}).Error("[handleOneFilesystemMaster] failed to initialize filesystem")
		}
		var responseChan chan *types.Event
		requestId := pieces[len(pieces)-1]
		if node.Value == s.myNodeId {
			log.Debugf("MOUNTING: %s=%s", fs, node.Value)
			responseChan, err = s.dispatchEvent(fs, &types.Event{Name: "mount"}, requestId)
			if err != nil {
				return err
			}
		} else {
			log.Debugf("UNMOUNTING: %s=%s", fs, node.Value)
			responseChan, err = s.dispatchEvent(fs, &types.Event{Name: "unmount"}, requestId)
			if err != nil {
				return err
			}
		}
		go func() {
			e := <-responseChan
			log.Debugf("[handleOneFilesystemMaster] filesystem %s response %#v", fs, e)
		}()
	}
	return nil
}

func (s *InMemoryState) handleOneFilesystemDeletion(node *client.Node) error {
	// This is where each node is notified of a filesystem being
	// deleted.  We must inform the fsmachine.
	log.Infof("DELETING: %s=%s", node.Key, node.Value)

	pieces := strings.Split(node.Key, "/")
	fs := pieces[len(pieces)-1]

	f, err := s.InitFilesystemMachine(fs)
	if err != nil {
		log.Infof("[handleOneFilesystemDeletion:%s] after initFs.. no fsMachine, error: %s", fs, err)
	} else {
		log.Infof("[handleOneFilesystemDeletion:%s] after initFs.. state: %s, status: %s", fs, f.GetCurrentState(), f.GetStatus())
	}

	var responseChan chan *Event
	// var err error
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}
	requestId := id.String()
	responseChan, err = s.dispatchEvent(fs, &types.Event{Name: "delete"}, requestId)
	if err != nil {
		return err
	}
	go func() {
		<-responseChan
	}()
	return nil
}

// make a global request, returning its id
func (s *InMemoryState) globalFsRequestId(fs string, event *types.Event) (chan *types.Event, string, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, "", err
	}
	requestID := id.String()

	event.ID = requestID
	event.FilesystemID = fs

	responseChan := make(chan *types.Event)

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		defer close(responseChan)

		rc, err := s.messenger.Subscribe(ctx, &types.SubscribeQuery{
			Type:         types.EventTypeResponse,
			FilesystemID: fs,
			RequestID:    requestID,
		})
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": fs,
				"request_id":    requestID,
			}).Error("[globalFsRequestId] failed to subscribe for responses")
			return
		}
		// waiting for the first event
		select {
		case event := <-rc:
			responseChan <- event
			return
		}
	}()

	err = s.messenger.Publish(event)
	if err != nil {
		log.WithFields(log.Fields{
			"error":         err,
			"filesystem_id": fs,
			"request_id":    requestID,
		}).Errorf("[globalFsRequest] error dispatching event %s: %s", event, err)
		return nil, "", err
	}

	return responseChan, requestID, nil
}

// attempt to register an event in etcd upon which the current master for that
// filesystem will act on it and then respond
func (s *InMemoryState) globalFsRequest(fs string, e *Event) (chan *Event, error) {
	c, _, err := s.globalFsRequestId(fs, e)
	// throw away id
	return c, err
}

func (s *InMemoryState) serializeEvent(e *Event) (string, error) {
	response, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	return string(response), nil
}

func (s *InMemoryState) deserializeEvent(node *client.Node) (*Event, error) {
	e := new(Event)
	err := json.Unmarshal([]byte(node.Value), e)
	if err != nil {
		log.Printf("deserializeEvent: error trying to unmarshal '%s' for %s", node.Value, node.Key)
		return nil, err
	}
	return e, nil
}

func (s *InMemoryState) respondToEvent(fs, requestId string, response *Event) error {

	response.Type = types.EventTypeResponse
	response.ID = requestId
	response.FilesystemID = fs

	return s.messenger.Publish(response)
}

// Update our local record of who has which snapshots, either based on learning
// from etcd or learning about our own snapshots (the latter is necessary
// because "Set" in etcd doesn't result in the value coming back over a Watch
// when using the same client). This can result in out-of-order updates (not
// serialized through Raft) across the cluster, which is a worry, but maybe the
// fact that the data in question is partitioned per server makes that OK.
//
// Possible alternative: use different etcd clients for the Watch versus the
// Set.
func (s *InMemoryState) UpdateSnapshotsFromKnownState(server, filesystem string, snapshots []*Snapshot) error {
	// deleted, err := isFilesystemDeletedInEtcd(filesystem)
	// if err != nil {
	// 	return err
	// }
	// if deleted {
	// 	// Filesystem is being deleted, so ignore it.
	// 	return nil
	// }

	fsm, err := s.InitFilesystemMachine(filesystem)
	if err != nil {
		if err == ErrFilesystemDeleted {
			// Filesystem is being deleted, ignoring it
			return nil
		}

		return err
	}

	oldSnapshots := fsm.GetSnapshots(server)

	fsm.SetSnapshots(server, snapshots)

	// s.globalSnapshotCacheLock.Lock()
	// if _, ok := s.globalSnapshotCache[server]; !ok {
	// 	s.globalSnapshotCache[server] = map[string][]snapshot{}
	// }
	// oldSnapshots := s.globalSnapshotCache[server][filesystem]
	// s.globalSnapshotCache[server][filesystem] = *snapshots
	// s.globalSnapshotCacheLock.Unlock()

	masterNode, err := s.registry.CurrentMasterNode(filesystem)
	if err != nil {
		return err
	}
	log.Printf(
		"[updateSnapshots] checking %s master: %s == %s?",
		filesystem, masterNode, server,
	)
	if masterNode == server {
		if len(snapshots) > 0 {
			// notify any interested parties that there are some new snapshots on
			// the master

			latest := (snapshots)[len(snapshots)-1]
			log.Printf(
				"[updateSnapshots] publishing latest snapshot %v on %s",
				latest, filesystem,
			)

			// External pubsub
			if len(snapshots) > len(oldSnapshots) {
				tlf, branch, err := s.registry.LookupFilesystemById(filesystem)
				if err != nil {
					return err
				}

				namespace := tlf.MasterBranch.Name.Namespace
				name := tlf.MasterBranch.Name.Name
				go func() {
					for _, ss := range snapshots[len(oldSnapshots):] {
						collaborators := make([]string, len(tlf.Collaborators))
						for idx, u := range tlf.Collaborators {
							collaborators[idx] = u.Id
						}
						err := s.publisher.PublishCommit(&types.CommitNotification{
							FilesystemId:    filesystem,
							Namespace:       namespace,
							Name:            name,
							Branch:          branch,
							CommitId:        ss.Id,
							Metadata:        ss.Metadata,
							OwnerID:         tlf.Owner.Id,
							CollaboratorIDs: collaborators,
						})
						if err != nil {
							log.WithFields(log.Fields{
								"error":         err,
								"filesystem_id": filesystem,
								"commit_id":     ss.Id,
							}).Error("[updateSnapshots] failed to publish ")
						}
					}
				}()
			}

			// Internal pubsub
			go func() {
				err := s.newSnapsOnMaster.Publish(filesystem, latest)
				if err != nil {
					log.Errorf(
						"[updateSnapshotsFromKnownState] "+
							"error publishing to newSnapsOnMaster: %s",
						err,
					)
				}
			}()
		}
	}
	// also slice it filesystem-wise, and publish to any observers
	// listening on a per-filesystem observer parameterized on server
	s.filesystemsLock.Lock()
	fs, ok := s.filesystems[filesystem]
	s.filesystemsLock.Unlock()
	if !ok {
		log.Printf(
			"state machine for %s not set up yet, can't notify newSnapsOnServers",
			filesystem,
		)
	} else {
		go func() {
			err := fs.PublishNewSnaps(server, true) // TODO publish latest, as above
			if err != nil {
				log.Printf(
					"[updateSnapshotsFromKnownState] "+
						"error publishing to newSnapsOnServers: %s",
					err,
				)
			}
		}()
	}
	return nil
}

// Recursively fetch and then watch the entire tree in etcd (so we can get
// serialized master updates as well as events), and filter out events that are
// irrelevent to us for whatever reason.  Support deserializing events from
// JSON into event objects with appropriate arguments.
//
// When an event shows up which we need to deal with, dispatch it to the
// appropriate filesystem state machine with dispatchEvent.
//
// TODO pass lastIndex around, and factor Get out apart from Watcher, in order
// to more succinctly get updated when we reconnect.
func (s *InMemoryState) fetchAndWatchEtcd() error {
	// thread-local map to remember whether to act on events for a master or
	// not (currently we never act on events for non-masters, one day we'll
	// want to for e.g. deletions, rollbacks and the like)
	filesystemBelongsToMe := map[string]bool{}

	// handy inline funcs to avoid duplication

	// returns whether mastersCache was modified
	updateMine := func(node *client.Node) bool {
		// (0)/(1)dotmesh.io/(2)servers/(3)masters/(4):filesystem = master
		pieces := strings.Split(node.Key, "/")
		filesystemID := pieces[4]

		// s.mastersCacheLock.Lock()
		// defer s.mastersCacheLock.Unlock()

		var modified bool
		if node.Value == "" {
			// delete(s.mastersCache, fs)
			s.registry.DeleteMasterNode(filesystemID)
			delete(filesystemBelongsToMe, filesystemID)
		} else {
			masterNode, ok := s.registry.GetMasterNode(filesystemID)
			if !ok || masterNode != node.Value {
				modified = true
				s.registry.SetMasterNode(filesystemID, node.Value)
			}
			// if ok && masterNode != node.Value {
			// 	modified = true
			// }
			// if !ok {
			// 	// new value
			// 	modified = true
			// }
			// s.mastersCache[fs] = node.Value

			if node.Value == s.myNodeId {
				filesystemBelongsToMe[filesystemID] = true
			} else {
				filesystemBelongsToMe[filesystemID] = false
			}
		}
		return modified
	}
	updateAddresses := func(node *client.Node) error {
		// (0)/(1)dotmesh.io/(2)servers/(3)addresses/(4):server = addresses
		pieces := strings.Split(node.Key, "/")
		server := pieces[4]

		s.serverAddressesCacheLock.Lock()
		defer s.serverAddressesCacheLock.Unlock()
		s.serverAddressesCache[server] = node.Value
		return nil
	}
	updateStates := func(node *client.Node) error {
		// (0)/(1)dotmesh.io/(2)servers/
		//     (3)snapshots/(4):server/(5):filesystem = snapshots
		pieces := strings.Split(node.Key, "/")
		server := pieces[4]
		filesystem := pieces[5]
		// s.globalStateCacheLock.Lock()
		// defer s.globalStateCacheLock.Unlock()

		fsm, err := s.InitFilesystemMachine(filesystem)
		if err != nil {
			// failed to initialize, nothing to do
			return nil
		}

		stateMetadata := &map[string]string{}
		if node.Value == "" {
			// TODO(karolis): not sure what happened here, it tries to delete if it already
			// couldn't find it?
			// if _, ok := s.globalStateCache[server]; !ok {
			// delete(s.globalStateCache[server], filesystem)
			// } else {
			// We don't know about the server anyway, so there's nothing to do
			// }
		} else {
			err := json.Unmarshal([]byte(node.Value), stateMetadata)
			if err != nil {
				log.Printf("Unable to marshal for updateStates - %s: %s", node.Value, err)
				return err
			}
			// if _, ok := s.globalStateCache[server]; !ok {
			// s.globalStateCache[server] = map[string]map[string]string{}
			// } else {

			// if _, ok := s.globalStateCache[server][filesystem]; !ok {
			// ok, we'll be setting it below...
			// } else {
			// state already exists. check that we're updating with a
			// revision that is the same or newer...
			// currentVersion := s.globalStateCache[server][filesystem]["version"]
			// currentVersion := fsm.GetMetadata(server)["version"]
			currentMeta := fsm.GetMetadata(server)
			currentVersion, ok := currentMeta["version"]
			if ok {
				i, err := strconv.ParseUint(currentVersion, 10, 64)
				if err != nil {
					// return err
					// unparsable version?
				} else {
					if i > node.ModifiedIndex {
						log.Printf(
							"Out of order updates! %s is older than %s",
							currentMeta,
							node,
						)
						return nil
					}
				}

			}

			// }
			// }
			newMeta := *stateMetadata
			newMeta["version"] = fmt.Sprintf("%d", node.ModifiedIndex)
			fsm.SetMetadata(server, newMeta)
			// s.globalStateCache[server][filesystem] = *stateMetadata
			// s.globalStateCache[server][filesystem]["version"] = fmt.Sprintf(
			// "%d", node.ModifiedIndex,
			// )
		}
		return nil
	}

	updateSnapshots := func(node *client.Node) error {
		// (0)/(1)dotmesh.io/(2)servers/
		//     (3)snapshots/(4):server/(5):filesystem = snapshots
		pieces := strings.Split(node.Key, "/")
		server := pieces[4]
		filesystem := pieces[5]

		if server == s.myNodeId {
			// Don't listen to updates from etcd about ourselves -
			// because we update that by calling
			// updateSnapshotsFromKnownState from the discovery code, and
			// that's better information.
			return nil
		}

		snapshots := []*Snapshot{}
		if node.Value == "" {
			// Key was deleted, so there's no snapshots
			return s.UpdateSnapshotsFromKnownState(server, filesystem, snapshots)
		} else {
			err := json.Unmarshal([]byte(node.Value), &snapshots)
			if err != nil {
				log.Printf(
					"updateSnapshots: error trying to unmarshal '%s' for %s on %s, %s",
					node.Value, filesystem, server, node.Key,
				)
				return err
			}
			return s.UpdateSnapshotsFromKnownState(server, filesystem, snapshots)
		}
	}
	/*
		(0)/(1)dotmesh.io/(2)registry/(3)filesystems/(4)<namespace>/(5)name =>
		{"Uuid": "<fs-uuid>"}
			fs-uuid can be a branch or filesystem uuid
	*/
	updateFilesystemRegistry := func(node *client.Node) error {
		pieces := strings.Split(node.Key, "/")
		name := VolumeName{Namespace: pieces[4], Name: pieces[5]}
		rf := RegistryFilesystem{}
		if node.Value == "" {
			// Deletion: the empty registryFilesystem will indicate that.
			return s.registry.UpdateFilesystemFromEtcd(name, rf)
		} else {
			err := json.Unmarshal([]byte(node.Value), &rf)
			if err != nil {
				return err
			}
			return s.registry.UpdateFilesystemFromEtcd(name, rf)
		}
	}
	/*
		   (0)/(1)dotmesh.io/(2)registry/(3)clones/(4)<fs-uuid-of-filesystem>/(5)<name> =>
			   uniqueness: we want branch names under a top-level filesystem to be unique, that is, assuming we're wedging the git UI into this
			   the fs-uuid has to be one of the filesystems, here that the clone gets attributed to in the UI

		   {"Origin": {"FilesystemId": "<fs-uuid-of-actual-origin-snapshot>", "SnapshotId": "<snap-id>"}, "Uuid": "<fs-uuid>"}
			   fs-uuid-of-filesystem can differ from fs-uuid-of-actual-origin-snapshot, uuid-of-filesystem is what gets attributed in the UI, fs-uuid-of-actual-origin-snapshot is the physical zfs filesystem it depends on
			   fs-uuid-of-actual-origin-snapshot is allowed to be the uuid of another clone
			   fs-uuid-of-filesystem, however has to be in /registry/filesystems because the filesystem always gets attributed to one top-level "repository"
	*/
	updateClonesRegistry := func(node *client.Node) error {
		pieces := strings.Split(node.Key, "/")
		topLevelFilesystemId := pieces[4]
		name := pieces[5]
		clone := &Clone{}
		if node.Value == "" {
			// It's a deletion, so pass the empty value in clone
			s.registry.DeleteCloneFromEtcd(name, topLevelFilesystemId)
		} else {
			err := json.Unmarshal([]byte(node.Value), clone)
			if err != nil {
				return err
			}
			s.registry.UpdateCloneFromEtcd(name, topLevelFilesystemId, *clone)
		}
		return nil
	}
	/*
	   (0)/(1)dotmesh.io/(2)filesystems/(3)containers/(4):filesystem_id =>
	   {"server": X, "containers": [<docker inspect info>, ...]}
	*/
	updateFilesystemsDirty := func(node *client.Node) error {
		pieces := strings.Split(node.Key, "/")
		filesystemId := pieces[4]
		dirtyInfo := &dirtyInfo{}
		if node.Value == "" {
			s.globalDirtyCacheLock.Lock()
			defer s.globalDirtyCacheLock.Unlock()
			delete(s.globalDirtyCache, filesystemId)
		} else {
			err := json.Unmarshal([]byte(node.Value), dirtyInfo)
			if err != nil {
				return err
			}
			s.globalDirtyCacheLock.Lock()
			defer s.globalDirtyCacheLock.Unlock()
			s.globalDirtyCache[filesystemId] = *dirtyInfo
		}
		return nil
	}
	updateFilesystemsContainers := func(node *client.Node) error {
		pieces := strings.Split(node.Key, "/")
		filesystemId := pieces[4]
		containerInfo := &containerInfo{}
		if node.Value == "" {
			s.globalContainerCacheLock.Lock()
			defer s.globalContainerCacheLock.Unlock()
			delete(s.globalContainerCache, filesystemId)
		} else {
			err := json.Unmarshal([]byte(node.Value), containerInfo)
			if err != nil {
				return err
			}
			s.globalContainerCacheLock.Lock()
			defer s.globalContainerCacheLock.Unlock()
			(s.globalContainerCache)[filesystemId] = *containerInfo
		}
		return nil
	}
	updateTransfers := func(node *client.Node) error {
		// (0)/(1)dotmesh.io/(2)filesystems/
		//     (3)transfers/(4):transferId = transferRequest
		pieces := strings.Split(node.Key, "/")
		transferId := pieces[4]
		transferInfo := &TransferPollResult{}
		if node.Value == "" {
			s.interclusterTransfersLock.Lock()
			defer s.interclusterTransfersLock.Unlock()
			delete(s.interclusterTransfers, transferId)
		} else {
			err := json.Unmarshal([]byte(node.Value), transferInfo)
			if err != nil {
				return err
			}
			s.interclusterTransfersLock.Lock()
			defer s.interclusterTransfersLock.Unlock()
			s.interclusterTransfers[transferId] = *transferInfo
		}
		return nil
	}

	// // TODO: REMOVE
	// maybeDispatchEvent := func(node *client.Node) error {
	// 	// (0)/(1)dotmesh.io/(2)filesystems/
	// 	//     (3)requests/(4):filesystem/(5):request_id = request
	// 	pieces := strings.Split(node.Key, "/")
	// 	fs := pieces[4]
	// 	mine, ok := filesystemBelongsToMe[fs]
	// 	if ok && mine {
	// 		// only act on events for filesystems that etcd reports as
	// 		// belonging to me
	// 		if err := s.deserializeDispatchAndRespond(fs, node); err != nil {
	// 			return err
	// 		}
	// 	}
	// 	return nil
	// }
	getVariant := func(node *client.Node) string {
		// e.g. "masters" in (0)/(1)dotmesh.io/(2)filesystems/(3)masters/(4)1b25b8f5...
		pieces := strings.Split(node.Key, "/")
		if len(pieces) > 3 {
			return pieces[2] + "/" + pieces[3]
		}
		return ""
	}

	// func() {
	// 	s.etcdWaitTimestampLock.Lock()
	// 	defer s.etcdWaitTimestampLock.Unlock()
	// 	s.etcdWaitTimestamp = time.Now().UnixNano()
	// 	s.etcdWaitState = "connect"
	// }()

	s.etcdWaitTimestampLock.Lock()

	s.etcdWaitTimestamp = time.Now().UnixNano()
	s.etcdWaitState = "insert initial admin password if not exists"
	s.etcdWaitTimestampLock.Unlock()

	// Do this every time, even if it fails.  This is to handle the case where
	// etcd gets wiped underneath us.
	err := s.insertInitialAdminPassword()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("failed to create initial admin")
	}

	s.etcdWaitTimestampLock.Lock()
	s.etcdWaitTimestamp = time.Now().UnixNano()
	s.etcdWaitState = "initial get"
	s.etcdWaitTimestampLock.Unlock()

	// on first connect, fetch all of, well, everything
	current, err := s.etcdClient.Get(context.Background(),
		fmt.Sprint(ETCD_PREFIX),
		&client.GetOptions{Recursive: true, Sort: false, Quorum: true},
	)
	if err != nil {
		return err
	}

	s.etcdWaitTimestampLock.Lock()
	s.etcdWaitTimestamp = time.Now().UnixNano()
	s.etcdWaitState = "initial processing"
	s.etcdWaitTimestampLock.Unlock()

	// find the masters and requests nodes
	var masters *client.Node
	// var requests *client.Node
	var serverAddresses *client.Node
	var serverSnapshots *client.Node
	var serverStates *client.Node
	var registryFilesystems *client.Node
	var registryClones *client.Node
	var filesystemsContainers *client.Node
	var interclusterTransfers *client.Node
	var dirtyFilesystems *client.Node
	for _, parent := range current.Node.Nodes {
		// need to iterate in...

		// We delibarately skip the "filesystems/deleted",
		// "filesystems/cleanupNeeded" and "filesystems/live" regions as
		// we don't store any caches of those things. In particular,
		// filesystems/deleted might grow without bound in a long-lived
		// cluster so we avoid ever keeping the whole thing anywhere
		// other than in etcd (and even there it might require pruning
		// in future); and cleanupNeeded and live are polled for in the
		// cleanupDeletedFilesystems goroutine. We could, if needed,
		// rewrite that to make the watcher notice "live" nodes
		// disappearing and check for a corresponding "cleanupNeeded"
		// and trigger cleanup, but that makes it almost certain that
		// multiple nodes (indeed, all nodes) will attempt to do the
		// cleanup - polling makes it most likely that one random node
		// will do any given cleanup, avoiding waste without the cost of
		// an explicit distributed transaction to ensure exactly one
		// node does it.
		for _, child := range parent.Nodes {
			switch getVariant(child) {
			case "filesystems/masters":
				masters = child
			case "servers/addresses":
				serverAddresses = child
			case "servers/snapshots":
				serverSnapshots = child
			case "servers/states":
				serverStates = child
			case "registry/filesystems":
				registryFilesystems = child
			case "registry/clones":
				registryClones = child
			case "filesystems/transfers":
				interclusterTransfers = child
			case "filesystems/dirty":
				dirtyFilesystems = child
			}
		}
	}

	if serverAddresses != nil {
		for _, node := range serverAddresses.Nodes {
			if err = updateAddresses(node); err != nil {
				return err
			}
		}
	}
	if serverStates != nil {
		for _, servers := range serverStates.Nodes {
			for _, filesystem := range servers.Nodes {
				if err = updateStates(filesystem); err != nil {
					return err
				}
			}
		}
	}

	if masters != nil {
		for _, node := range masters.Nodes {
			modified := updateMine(node)
			if modified {
				if err = s.handleOneFilesystemMaster(node); err != nil {
					return err
				}
			}
		}
	}

	if registryFilesystems != nil {
		for _, namespace := range registryFilesystems.Nodes {
			for _, topLevelFilesystem := range namespace.Nodes {
				if err = updateFilesystemRegistry(topLevelFilesystem); err != nil {
					return err
				}
			}
		}
	}
	if registryClones != nil {
		for _, topLevelFilesystem := range registryClones.Nodes {
			for _, nameToCloneFilesystem := range topLevelFilesystem.Nodes {
				if err = updateClonesRegistry(nameToCloneFilesystem); err != nil {
					return err
				}
			}
		}
	}
	if serverSnapshots != nil {
		for _, servers := range serverSnapshots.Nodes {
			for _, filesystem := range servers.Nodes {
				if err = updateSnapshots(filesystem); err != nil {
					return err
				}
			}
		}
	}

	if dirtyFilesystems != nil {
		for _, filesystem := range dirtyFilesystems.Nodes {
			for _, dirty := range filesystem.Nodes {
				if err = updateFilesystemsDirty(dirty); err != nil {
					return err
				}
			}
		}
	}
	if filesystemsContainers != nil {
		for _, filesystem := range filesystemsContainers.Nodes {
			for _, containers := range filesystem.Nodes {
				if err = updateFilesystemsContainers(containers); err != nil {
					return err
				}
			}
		}
	}
	if interclusterTransfers != nil {
		for _, node := range interclusterTransfers.Nodes {
			if err = updateTransfers(node); err != nil {
				return err
			}
		}
	}
	// TODO: REMOVE
	// if requests != nil {
	// 	for _, requestsForFilesystem := range requests.Nodes {
	// 		for _, node := range requestsForFilesystem.Nodes {
	// 			if err = maybeDispatchEvent(node); err != nil {
	// 				return err
	// 			}
	// 		}
	// 	}
	// }
	// now that our state is initialized, maybe we're in a good place to
	// interrogate docker for running containers as part of initial
	// bootstrap, and also start the docker plugin
	go func() { s.fetchRelatedContainersChan <- true }()
	// it only runs after we've successfully fetched some data from etcd,
	// to avoid startup deadlock when etcd is down. run api/rpc server at same
	// time as docker plugin to avoid 'dm cluster' health-check triggering
	// before we're fully up.
	onceAgain.Do(func() {
		go s.runServer()
		go s.runUnixDomainServer()
		go s.runPlugin()
	})

	// now watch for changes, and pipe them into the state machines
	watcher := s.etcdClient.Watcher(
		fmt.Sprintf(ETCD_PREFIX),
		&client.WatcherOptions{
			AfterIndex: current.Index, Recursive: true,
		},
	)
	for {
		func() {
			s.etcdWaitTimestampLock.Lock()
			defer s.etcdWaitTimestampLock.Unlock()
			s.etcdWaitTimestamp = time.Now().UnixNano()
			s.etcdWaitState = "watch"
		}()
		node, err := watcher.Next(context.Background())
		if err != nil {
			func() {
				s.etcdWaitTimestampLock.Lock()
				defer s.etcdWaitTimestampLock.Unlock()
				s.etcdWaitTimestamp = time.Now().UnixNano()
				s.etcdWaitState = fmt.Sprintf("watcher error %+v", err)
			}()

			// TODO: add some logging in this case
			// we want to see if we have been too slow to process the etcd initial get
			if strings.Contains(fmt.Sprintf("%v", err), "the requested history has been cleared") {
				// Too much stuff changed in etcd since we processed all of it.
				// Try to recover from this case. Just make a watcher from the
				// current state, which means we'll have missed some events,
				// but at least we won't crashloop.
				watcher = s.etcdClient.Watcher(
					fmt.Sprintf(ETCD_PREFIX),
					&client.WatcherOptions{
						// NB: no AfterIndex option, throw away interim
						// history...
						Recursive: true,
					},
				)
				node, err = watcher.Next(context.Background())
				if err != nil {
					func() {
						s.etcdWaitTimestampLock.Lock()
						defer s.etcdWaitTimestampLock.Unlock()
						s.etcdWaitTimestamp = time.Now().UnixNano()
						s.etcdWaitState = fmt.Sprintf("recovered watcher error %+v", err)
					}()
					log.Printf(
						"[fetchAndWatchEtcd] failed fetching next event after creating recovered watcher: %v",
						err,
					)
					return err
				}
			} else {
				return err
			}
		}
		func() {
			s.etcdWaitTimestampLock.Lock()
			defer s.etcdWaitTimestampLock.Unlock()
			s.etcdWaitTimestamp = time.Now().UnixNano()
			if node == nil {
				s.etcdWaitState = fmt.Sprintf("processing nil node")
			} else {
				if node.Node == nil {
					s.etcdWaitState = fmt.Sprintf("processing nil node.Node")
				} else {
					s.etcdWaitState = fmt.Sprintf("processing %s", node.Node.Key)
				}
			}
		}()

		// From time to time, the entire registry will be deleted (see rpc.go
		// RestoreEtcd). Detect this case and wipe out the registry records as
		// commonly dots will be re-owned in this scenario.

		if node.Node.Key == fmt.Sprintf("%s/registry", ETCD_PREFIX) {
			s.resetRegistry()
			return fmt.Errorf(
				"intentionally reloading from etcd because " +
					"we noticed the registry disappear.",
			)
		}

		variant := getVariant(node.Node)

		if variant == "filesystems/masters" {

			/*

				TODO: PUT SOME LOGGING HERE!

			*/
			modified := updateMine(node.Node)
			if modified {
				if err = s.handleOneFilesystemMaster(node.Node); err != nil {
					return err
				}
			}
		} else if variant == "filesystems/deleted" {
			// [x] Done - store.WatchDeleted(cb WatchDeletedCB) error
			if err = s.handleOneFilesystemDeletion(node.Node); err != nil {
				return err
			}
		} else if variant == "servers/addresses" {
			if err = updateAddresses(node.Node); err != nil {
				return err
			}
		} else if variant == "servers/snapshots" {
			if err = updateSnapshots(node.Node); err != nil {
				return err
			}
		} else if variant == "servers/states" {
			if err = updateStates(node.Node); err != nil {
				return err
			}
		} else if variant == "registry/filesystems" {
			if err = updateFilesystemRegistry(node.Node); err != nil {
				return err
			}
		} else if variant == "registry/clones" {
			if err = updateClonesRegistry(node.Node); err != nil {
				return err
			}
		} else if variant == "filesystems/containers" {
			// [x] Done - store.WatchContainers
			if err = updateFilesystemsContainers(node.Node); err != nil {
				return err
			}
		} else if variant == "filesystems/dirty" {
			// [x] Done - store.WatchDirty
			if err = updateFilesystemsDirty(node.Node); err != nil {
				return err
			}
		} else if variant == "filesystems/transfers" {
			// [x] Done - store.WatchTransfers
			if err = updateTransfers(node.Node); err != nil {
				return err
			}
		}
	}
}
