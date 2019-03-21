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
	"path/filepath"
	"strings"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/bolt"
	"golang.org/x/net/context"

	"github.com/dotmesh-io/dotmesh/pkg/store"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
	"github.com/dotmesh-io/dotmesh/pkg/uuid"

	log "github.com/sirupsen/logrus"
)

func getKVDBCfg() *store.KVDBConfig {

	if os.Getenv(types.EnvStorageBackend) == types.StorageBackendBoltdb {
		// using boltdb backend
		fp := types.DefaultBoltdbPath
		if os.Getenv(types.EnvDotmeshBoltdbPath) != "" {
			fp = os.Getenv(types.EnvDotmeshBoltdbPath)
		}
		err := os.MkdirAll(fp, os.ModePerm)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"path":  fp,
			}).Error("failed to ensure Boltdb KV store directory")
			os.Exit(1)
		}

		return &store.KVDBConfig{
			Type:   store.KVTypeBolt,
			Prefix: types.EtcdPrefix,
			Options: map[string]string{
				bolt.KvSnap: filepath.Join(fp, "dotmesh.db"),
			},
		}
	}

	endpoint := os.Getenv(types.EnvEtcdEndpoint)
	if endpoint == "" {
		log.Infof("%s not set, using default Etcd URL: '%s'", types.EnvEtcdEndpoint, types.DefaultEtcdURL)
		endpoint = types.DefaultEtcdURL
	} else {
		log.Infof("%s set, using it: '%s'", types.EnvEtcdEndpoint, endpoint)
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
	if err != nil && !store.IsKeyAlreadyExist(err) {
		log.WithFields(log.Fields{
			"error":         err,
			"filesystem_id": fsId,
			"username":      username,
		}).Error("[markFilesystemAsDeletedInEtcd] failed to set 'deleted' in filesystem store")
		return err
	}

	err = s.filesystemStore.SetCleanupPending(at, &store.SetOptions{})
	if err != nil && !store.IsKeyAlreadyExist(err) {
		log.WithFields(log.Fields{
			"error":         err,
			"filesystem_id": fsId,
			"username":      username,
		}).Error("[markFilesystemAsDeletedInEtcd] failed to set 'cleanupPending' in filesystem store")
	}
	return err
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

		log.WithFields(log.Fields{
			"filesystem_id": fsId,
			"namespace":     deletionAudit.Name.Namespace,
			"name":          deletionAudit.Name.Name,
		}).Info("[cleanupDeletedFilesystems] deleting filesystem")

		err = s.filesystemStore.DeleteContainers(fsId)
		if err != nil && !store.IsKeyNotFound(err) {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": fsId,
			}).Error("[cleanupDeletedFilesystems] failed to delete filesystem containers during cleanup")
		}
		err = s.filesystemStore.DeleteMaster(fsId)
		if err != nil && !store.IsKeyNotFound(err) {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": fsId,
			}).Error("[cleanupDeletedFilesystems] failed to delete filesystem master info during cleanup")
		}
		err = s.filesystemStore.DeleteDirty(fsId)
		if err != nil && !store.IsKeyNotFound(err) {
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

func (s *InMemoryState) handleFilesystemDeletion(fda *types.FilesystemDeletionAudit) error {
	// This is where each node is notified of a filesystem being
	// deleted.  We must inform the fsmachine.
	log.Infof("DELETING: %s=%s", fda.FilesystemID, fda.Server)

	// pieces := strings.Split(node.Key, "/")
	// fs := pieces[len(pieces)-1]

	f, err := s.InitFilesystemMachine(fda.FilesystemID)
	if err != nil {
		log.Infof("[handleFilesystemDeletion:%s] after initFs.. no fsMachine, error: %s", fda.FilesystemID, err)
	} else {
		log.Infof("[handleFilesystemDeletion:%s] after initFs.. state: %s, status: %s", fda.FilesystemID, f.GetCurrentState(), f.GetStatus())
	}

	var responseChan chan *Event
	requestId := uuid.New().String()
	responseChan, err = s.dispatchEvent(fda.FilesystemID, &types.Event{Name: "delete"}, requestId)
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

	requestID := uuid.New().String()

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

	err := s.messenger.Publish(event)
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
	fsm, err := s.InitFilesystemMachine(filesystem)
	if err != nil {
		if err == ErrFilesystemDeleted {
			// Filesystem is being deleted, ignoring it
			return nil
		}

		return fmt.Errorf("[UpdateSnapshotsFromKnownState] Error initialising filesystem machine: %s", err)
	}

	oldSnapshots := fsm.GetSnapshots(server)

	fsm.SetSnapshots(server, snapshots)

	masterNode, err := s.registry.CurrentMasterNode(filesystem)
	if err != nil {
		return fmt.Errorf("[UpdateSnapshotsFromKnownState] Error finding master node: %s", err)
	}

	if masterNode == server {
		if len(snapshots) > 0 {
			// notify any interested parties that there are some new snapshots on
			// the master

			latest := (snapshots)[len(snapshots)-1]

			// External pubsub
			if len(snapshots) > len(oldSnapshots) {
				tlf, branch, err := s.registry.LookupFilesystemById(filesystem)
				if err != nil {
					return fmt.Errorf("[UpdateSnapshotsFromKnownState] Error looking up filesystem: %s", err)
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
							}).Error("[UpdateSnapshotsFromKnownState] failed to publish ")
						}
					}
				}()
			}

			// Internal pubsub
			go func() {
				err := s.newSnapsOnMaster.Publish(filesystem, latest)
				if err != nil {
					log.Errorf(
						"[UpdateSnapshotsFromKnownState] "+
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
			"[UpdateSnapshotsFromKnownState] state machine for %s not set up yet, can't notify newSnapsOnServers",
			filesystem,
		)
	} else {
		go func() {
			err := fs.PublishNewSnaps(server, true) // TODO publish latest, as above
			if err != nil {
				log.Printf(
					"[UpdateSnapshotsFromKnownState] "+
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

	s.etcdWaitTimestampLock.Lock()
	s.etcdWaitTimestamp = time.Now().UnixNano()
	s.etcdWaitState = "initial get"
	s.etcdWaitTimestampLock.Unlock()

	err := s.watchFilesystemStoreMasters()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("failed to start watching filesystem masters")
	} else {
		log.Info("[fetchAndWatchEtcd] filesystem masters watcher started")
	}

	err = s.watchServerAddresses()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("failed to start watching server addresses")
	} else {
		log.Info("[fetchAndWatchEtcd] server addresses watcher started")
	}

	err = s.watchServerStates()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("failed to start watching server states")
	} else {
		log.Info("[fetchAndWatchEtcd] server states watcher started")
	}

	err = s.watchServerSnapshots()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("failed to start watching server snapshots")
	} else {
		log.Info("[fetchAndWatchEtcd] server snapshots watcher started")
	}

	// err = s.watchCleanupPending()
	// if err != nil {
	// 	log.WithFields(log.Fields{
	// 		"error": err,
	// 	}).Fatal("failed to start watching cleanup pending entries")
	// } else {
	// 	log.Info("[fetchAndWatchEtcd] cleanup pending filesystem watcher started")
	// }

	err = s.watchRegistryFilesystems()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("failed to start watching registry filesystems")
	} else {
		log.Info("[fetchAndWatchEtcd] registry filesystems watcher started")
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

	err = s.watchRegistryClones()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("failed to start watching registry clones")
	} else {
		log.Info("[fetchAndWatchEtcd] registry clones watcher started")
	}

	err = s.watchDirtyFilesystems()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("failed to start watching dirty filesystems")
	} else {
		log.Info("[fetchAndWatchEtcd] dirty filesystems watcher started")
	}

	err = s.watchFilesystemContainers()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("failed to start watching filesystem containers")
	} else {
		log.Info("[fetchAndWatchEtcd] filesystem containers watcher started")
	}

	err = s.watchTransfers()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("failed to start watching transfers")
	} else {
		log.Info("[fetchAndWatchEtcd] transfers watcher started")
	}

	err = s.watchFilesystemDeleted()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("failed to start watching deleted filesystems")
	} else {
		log.Info("[fetchAndWatchEtcd] deleted filesystem watcher started")
	}

	s.etcdWaitTimestampLock.Lock()
	s.etcdWaitTimestamp = time.Now().UnixNano()
	s.etcdWaitState = "insert initial admin password if not exists"
	s.etcdWaitTimestampLock.Unlock()

	// Do this every time, even if it fails.  This is to handle the case where
	// etcd gets wiped underneath us.
	err = s.insertInitialAdminPassword()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("failed to create initial admin")
	}

	// now that our state is initialized, maybe we're in a good place to
	// interrogate docker for running containers as part of initial
	// bootstrap, and also start the docker plugin
	go func() { s.fetchRelatedContainersChan <- true }()
	// it only runs after we've successfully fetched some data from etcd,
	// to avoid startup deadlock when etcd is down. run api/rpc server at same
	// time as docker plugin to avoid 'dm cluster' health-check triggering
	// before we're fully up.
	log.Info("starting RPC endpoints")
	onceAgain.Do(func() {
		go s.runServer()
		go s.runUnixDomainServer()
		go s.runPlugin()
	})
	log.Info("RPC endpoints started")

	s.etcdWaitTimestampLock.Lock()
	s.etcdWaitTimestamp = time.Now().UnixNano()
	s.etcdWaitState = "watch"
	s.etcdWaitTimestampLock.Unlock()

	// block forever
	select {}

	return nil
}
