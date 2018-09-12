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

	"github.com/coreos/etcd/client"
	"github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"

	log "github.com/sirupsen/logrus"
)

// etcd related pieces, including the parts of InMemoryState which interact with etcd

var etcdKeysAPI client.KeysAPI
var etcdConnectionOnce Once

func getEtcdKeysApi() (client.KeysAPI, error) {
	etcdConnectionOnce.Do(func() {
		var err error
		endpoint := os.Getenv("DOTMESH_ETCD_ENDPOINT")
		if endpoint == "" {
			endpoint = "https://dotmesh-etcd:42379"
		}
		transport := &http.Transport{}
		if endpoint[:5] == "https" {
			// only try to fetch PKI gubbins if we're creating an encrypted
			// connection.
			pkiPath := os.Getenv("DOTMESH_PKI_PATH")
			if pkiPath == "" {
				pkiPath = "/pki"
			}
			transport, err = transportFromTLS(
				fmt.Sprintf("%s/apiserver.pem", pkiPath),
				fmt.Sprintf("%s/apiserver-key.pem", pkiPath),
				fmt.Sprintf("%s/ca.pem", pkiPath),
			)
			if err != nil {
				panic(err)
			}
		}
		cfg := client.Config{
			Endpoints: []string{endpoint},
			Transport: transport,
			// set timeout per request to fail fast when the target endpoint is
			// unavailable
			HeaderTimeoutPerRequest: time.Second * 10,
		}
		etcdClient, err := client.New(cfg)
		if err != nil {
			// maybe retry, instead of ending it all
			panic(err)
		}
		etcdKeysAPI = client.NewKeysAPI(etcdClient)
	})
	return etcdKeysAPI, nil
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
func (state *InMemoryState) updateAddressesInEtcd() error {
	addresses, err := guessIPv4Addresses()
	if err != nil {
		return err
	}

	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}

	_, err = kapi.Set(
		context.Background(),
		fmt.Sprintf("%s/servers/addresses/%s", ETCD_PREFIX, state.myNodeId),
		strings.Join(addresses, ","),
		&client.SetOptions{TTL: 60 * time.Second},
	)
	if err != nil {
		return err
	}
	return nil
}

func isFilesystemDeletedInEtcd(fsId string) (bool, error) {
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return false, err
	}

	result, err := kapi.Get(
		context.Background(),
		fmt.Sprintf("%s/filesystems/deleted/%s", ETCD_PREFIX, fsId),
		nil,
	)

	if err != nil {
		if client.IsKeyNotFound(err) {
			return false, nil
		} else {
			return false, err
		}
	}

	if result != nil {
		return true, nil
	} else {
		return false, nil
	}
}

func (state *InMemoryState) markFilesystemAsDeletedInEtcd(
	fsId, username string,
	name VolumeName,
	tlFsId, branch string,
) error {
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}

	// Feel free to suggest additional useful things to put in the
	// deletion audit trail.  The two that the system REQUIRES are
	// "Name", "TopLevelFilesystemId" and "Clone", which are used to ensure that the registry
	// entry is cleaned up later. (Name for a top level filesystem,
	// Clone for a non-master branch). Please don't remove/rename that
	// without updating cleanupDeletedFilesystems
	auditTrail, err := json.Marshal(struct {
		Server    string
		Username  string
		DeletedAt time.Time

		// These fields are mandatory
		Name                 VolumeName
		TopLevelFilesystemId string
		Clone                string
	}{
		state.myNodeId,
		username,
		time.Now(),

		// These fields are mandatory
		name,
		tlFsId,
		branch})
	if err != nil {
		return err
	}

	_, err = kapi.Set(
		context.Background(),
		fmt.Sprintf("%s/filesystems/deleted/%s", ETCD_PREFIX, fsId),
		string(auditTrail),
		&client.SetOptions{PrevExist: client.PrevNoExist},
	)
	if err != nil {
		return err
	}

	// Now mark it for eventual cleanup (when the liveness key expires)
	_, err = kapi.Set(
		context.Background(),
		fmt.Sprintf("%s/filesystems/cleanupPending/%s", ETCD_PREFIX, fsId),
		string(auditTrail),
		&client.SetOptions{PrevExist: client.PrevNoExist},
	)
	if err != nil {
		return err
	}

	return nil
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

func (state *InMemoryState) cleanupDeletedFilesystems() error {
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}

	pending, err := listFilesystemsPendingCleanup(kapi)
	if err != nil {
		return err
	}

	for fsId, names := range pending {
		errors := make([]error, 0)
		del := func(key string) {
			_, err = kapi.Delete(
				context.Background(),
				key,
				&client.DeleteOptions{},
			)
			if err != nil && !client.IsKeyNotFound(err) {
				errors = append(errors, err)
			}
		}

		del(fmt.Sprintf("%s/filesystems/containers/%s", ETCD_PREFIX, fsId))
		del(fmt.Sprintf("%s/filesystems/dirty/%s", ETCD_PREFIX, fsId))
		del(fmt.Sprintf("%s/filesystems/masters/%s", ETCD_PREFIX, fsId))

		if names.Name.Namespace != "" && names.Name.Name != "" {
			// The name might be blank in the audit trail - this is used
			// to indicate that this was a clone, NOT the toplevel filesystem, so
			// there's no need to remove the registry entry for the whole
			// volume. We only do that when deleting the toplevel filesystem.

			// Normally, the registry entry is deleted as soon as the volume
			// is deleted, but in the event of a failure it might not have
			// been. So we try again.
			key := fmt.Sprintf(
				"%s/registry/filesystems/%s/%s",
				ETCD_PREFIX,
				names.Name.Namespace,
				names.Name.Name)

			oldNode, err := kapi.Get(
				context.Background(),
				key,
				&client.GetOptions{},
			)
			if err != nil {
				if client.IsKeyNotFound(err) {
					// we are good, it doesn't exist, nothing to delete
				} else {
					errors = append(errors, err)
				}
			} else {
				// We have an existing registry entry, but is it the one
				// we're supposed to delete, or a newly-created volume
				// with the name of the deleted one?
				currentData := struct {
					Id string
				}{}
				err = json.Unmarshal([]byte(oldNode.Node.Value), &currentData)
				if err != nil {
					errors = append(errors, err)
				}
				if currentData.Id == fsId {
					_, err = kapi.Delete(
						context.Background(),
						key,
						&client.DeleteOptions{PrevValue: oldNode.Node.Value},
					)
					if err != nil && !client.IsKeyNotFound(err) {
						errors = append(errors, err)
					}
				}
			}
		}

		if names.Clone != "" {
			// The clone name might be blank in the audit trail - this is
			// used to indicate that this was the toplevel filesystem
			// rather than a clone. But when a clone name is specified,
			// we need to delete a clone record from etc.
			del(fmt.Sprintf(
				"%s/registry/clones/%s/%s", ETCD_PREFIX,
				names.TopLevelFilesystemId, names.Clone))
		}

		if len(errors) == 0 {
			del(fmt.Sprintf("%s/filesystems/cleanupPending/%s", ETCD_PREFIX, fsId))
		} else {
			return fmt.Errorf("Errors found cleaning up after a deleted filesystem: %+v", errors)
		}
	}

	return nil
}

// The result is a map from filesystem ID to the VolumeName or branch name it once had.
func listFilesystemsPendingCleanup(kapi client.KeysAPI) (map[string]NameOrClone, error) {
	// list ETCD_PREFIX/filesystems/cleanupPending/ID without corresponding
	// ETCD_PREFIX/filesystems/live/ID

	pending, err := kapi.Get(context.Background(),
		fmt.Sprintf("%s/filesystems/cleanupPending", ETCD_PREFIX),
		&client.GetOptions{Recursive: true, Sort: false},
	)
	if err != nil {
		if client.IsKeyNotFound(err) {
			return map[string]NameOrClone{}, nil
		} else {
			return map[string]NameOrClone{}, err
		}
	}

	result := make(map[string]NameOrClone)
	for _, node := range pending.Node.Nodes {
		// /dotmesh.io/filesystems/cleanupPending/3ed24670-8fd0-4cec-4191-d3d5bae15172
		pieces := strings.Split(node.Key, "/")
		if len(pieces) == 5 {
			fsId := pieces[4]

			_, err := kapi.Get(context.Background(),
				fmt.Sprintf("%s/filesystems/live/%s", ETCD_PREFIX, fsId),
				&client.GetOptions{},
			)
			if err == nil {
				// Key was found
			} else if client.IsKeyNotFound(err) {
				// Key not found, it's no longer live
				// So extract the name from the audit trail and add it to the result
				var audit NameOrClone
				err := json.Unmarshal([]byte(node.Value), &audit)
				if err != nil {
					// We don't want one corrupted key stopping us from finding the rest
					// So log+ignore.
					log.Printf(
						"[listFilesystemsPendingCleanup] Error parsing audit trail: %s=%s",
						node.Key, node.Value)
				} else {
					result[fsId] = audit
				}
			} else {
				// Error!
				return map[string]NameOrClone{}, err
			}
		}
	}

	return result, nil
}

func (state *InMemoryState) markFilesystemAsLiveInEtcd(topLevelFilesystemId string) error {
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/filesystems/live/%s", ETCD_PREFIX, topLevelFilesystemId)
	ttl := time.Duration(state.config.FilesystemMetadataTimeout) * time.Second

	_, err = kapi.Set(
		context.Background(),
		key,
		state.myNodeId,
		&client.SetOptions{TTL: ttl},
	)
	if err != nil {
		return err
	}
	return nil
}

// shortcut for dispatching an event to a filesystem's fsMachine's event
// stream, returning the event stream for convenience so the caller can listen
// for a response
func (s *InMemoryState) dispatchEvent(
	filesystem string, e *Event, requestId string,
) (chan *Event, error) {
	if requestId == "" {
		id, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}
		requestId = id.String()
	}
	fs, err := s.maybeFilesystem(filesystem)
	if err != nil {
		return nil, err
	}
	if e.Args == nil {
		e.Args = &EventArgs{}
	}
	(*e.Args)["RequestId"] = requestId
	fs.responsesLock.Lock()
	defer fs.responsesLock.Unlock()
	rc, ok := fs.responses[requestId]
	if !ok {
		responseChan := make(chan *Event)
		fs.responses[requestId] = responseChan
		rc = responseChan
	}

	// Now we have a response channel set up, it's safe to send the request.

	// Don't block the entire etcd event-loop just because one fsMachine isn't
	// ready to receive an event. Our response is a chan anyway, so consumers
	// can synchronize on reading from that as they wish (for example, in
	// another goroutine).
	go func() {
		fs.requests <- e
	}()

	return rc, nil
}

func (s *InMemoryState) handleOneFilesystemMaster(node *client.Node) error {
	if node.Value == "" {
		// The filesystem is being deleted, and we need do nothing about it
	} else {
		pieces := strings.Split(node.Key, "/")
		fs := pieces[len(pieces)-1]

		var err error
		var deleted bool

		deleted, err = isFilesystemDeletedInEtcd(fs)
		if err != nil {
			log.Printf("[handleOneFilesystemMaster] error determining if file system is deleted: fs: %s, etcd nodeValue: %s, error: %+v", fs, node.Value, err)
			return err
		}
		if deleted {
			log.Printf("[handleOneFilesystemMaster] filesystem is deleted so no need to mount/unmount fs: %s, etcd nodeValue: %s", fs, node.Value)
			// Filesystem is being deleted, so ignore it.
			return nil
		}

		s.initFilesystemMachine(fs)
		var responseChan chan *Event
		requestId := pieces[len(pieces)-1]
		if node.Value == s.myNodeId {
			log.Printf("MOUNTING: %s=%s", fs, node.Value)
			responseChan, err = s.dispatchEvent(fs, &Event{Name: "mount"}, requestId)
			if err != nil {
				return err
			}
		} else {
			log.Printf("UNMOUNTING: %s=%s", fs, node.Value)
			responseChan, err = s.dispatchEvent(fs, &Event{Name: "unmount"}, requestId)
			if err != nil {
				return err
			}
		}
		go func() {
			<-responseChan
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

	func() {
		s.filesystemsLock.Lock()
		defer s.filesystemsLock.Unlock()
		f, ok := s.filesystems[fs]
		if ok {
			log.Infof("[handleOneFilesystemDeletion:%s] before initFs..  state: %s, status: %s", fs, f.currentState, f.status)
		} else {
			log.Infof("[handleOneFilesystemDeletion:%s] before initFs.. no fsMachine", fs)
		}
	}()

	s.initFilesystemMachine(fs)

	func() {
		s.filesystemsLock.Lock()
		defer s.filesystemsLock.Unlock()
		f, ok := s.filesystems[fs]
		if ok {
			log.Infof("[handleOneFilesystemDeletion:%s] after initFs.. state: %s, status: %s", fs, f.currentState, f.status)
		} else {
			log.Infof("[handleOneFilesystemDeletion:%s] after initFs.. no fsMachine", fs)
		}
	}()

	var responseChan chan *Event
	var err error
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}
	requestId := id.String()
	responseChan, err = s.dispatchEvent(fs, &Event{Name: "delete"}, requestId)
	if err != nil {
		return err
	}
	go func() {
		<-responseChan
	}()
	return nil
}

// make a global request, returning its id
func (s *InMemoryState) globalFsRequestId(fs string, e *Event) (chan *Event, string, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, "", err
	}
	requestId := id.String()
	kapi, err := getEtcdKeysApi()
	if err != nil {
		return nil, "", err
	}
	log.Printf("globalFsRequest %s %s", fs, e)
	serialized, err := s.serializeEvent(e)
	if err != nil {
		log.Printf("globalFsRequest - error serializing %s: %s", e, err)
		return nil, "", err
	}
	log.Printf("globalFsRequest: setting '%s' to '%s'",
		fmt.Sprintf("%s/filesystems/requests/%s/%s", ETCD_PREFIX, fs, requestId),
		serialized,
	)
	resp, err := kapi.Set(
		context.Background(),
		fmt.Sprintf("%s/filesystems/requests/%s/%s", ETCD_PREFIX, fs, requestId),
		serialized,
		&client.SetOptions{PrevExist: client.PrevNoExist, TTL: 604800 * time.Second},
	)
	if err != nil {
		log.Warnf("globalFsRequest - error setting %s: %s", e, err)
		return nil, "", err
	}
	responseChan := make(chan *Event)
	go func() {
		// TODO become able to cope with becoming disconnected from etcd and
		// then reconnecting and pick up where we left off (process any new
		// responses)...
		watcher := kapi.Watcher(
			// TODO maybe responses should get their own IDs, so that there can be
			// multiple responses to a given event (at present we just assume one)
			fmt.Sprintf("%s/filesystems/responses/%s/%s", ETCD_PREFIX, fs, requestId),
			&client.WatcherOptions{AfterIndex: resp.Node.CreatedIndex, Recursive: true},
		)
		node, err := watcher.Next(context.Background())
		if err != nil {
			responseChan <- &Event{
				Name: "error-watcher-next", Args: &EventArgs{"err": err},
			}
			close(responseChan)
			return
		}
		response, err := s.deserializeEvent(node.Node)
		if err != nil {
			responseChan <- &Event{
				Name: "error-deserialize", Args: &EventArgs{"err": err},
			}
			close(responseChan)
			return
		}
		responseChan <- response

		// the one-off response chan has done its job here.
		close(responseChan)
		// also clean up the request and response nodes in etcd. (TODO: /not/
		// doing this might actually be a good way to implement a log of all
		// actions performed on the system). TODO why are we not doing this at
		// all any more? Something to do with a race where events showed up
		// empty. (Actually, empty events correspond to a deletion, ie a TTL
		// timeout, I think.) Oh and we added a TTL.
		/*
			for _, cleanup := range []string{"requests", "responses"} {
				_, err = kapi.Delete(
					context.Background(),
					fmt.Sprintf("%s/filesystems/%s/%s/%s", ETCD_PREFIX, cleanup, fs, requestId),
					nil,
				)
				if err != nil {
					log.Printf("Error while trying to cleanup %s %s %s: %s", cleanup, fs, requestId, err)
				}
			}
		*/

	}()
	return responseChan, id.String(), nil
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

func (s *InMemoryState) deserializeDispatchAndRespond(fs string, node *client.Node, kapi client.KeysAPI) error {
	// TODO 2-phase commit to avoid doubling up events after
	// reading them, performing them, and then getting disconnected
	// before cleaning them up?
	e, err := s.deserializeEvent(node)
	if err != nil {
		return err
	}
	pieces := strings.Split(node.Key, "/")
	requestId := pieces[len(pieces)-1]

	// check that there isn't a stale response already. if so, do nothing.
	_, err = kapi.Get(
		context.Background(),
		fmt.Sprintf("%s/filesystems/responses/%s/%s", ETCD_PREFIX, fs, requestId),
		nil,
	)
	if err == nil {
		// OK, there's a stale response. Leave it alone and don't re-perform
		// the action. But this is not an error (don't error out and reconnect
		// to etcd, or you'll get stuck in a loop).
		return nil
	}
	if !client.IsKeyNotFound(err) {
		// Some error other than key not found. The key-not-found is the
		// expected, happy path.
		return err
	}

	// channel is the internal channel from etcd => state machines.
	// listen on that channel, and when a response comes back from the local
	// state machine, send it back out as an etcd response

	// don't block processing of further events on getting a response to this:
	// in particular, the "move" command depends on sub-commands being
	// processed (it requires this dispatch/response loop to work reentrantly),
	// specifically so that a server performing a handoff can be notified that
	// the server it is handing off to has received the snapshots that it made
	// available to it as part of the handoff. so, run this in a goroutine;
	// it's ok to return early because the response is just going back into
	// etcd and anyone waiting for it will be waiting on the response key to
	// show up, not on the return of this function.

	log.Printf("About to dispatch %s to %s", e, fs)
	c, err := s.dispatchEvent(fs, e, requestId)
	if err != nil {
		return err
	}
	log.Printf("Got response chan %v, %s for %v", c, err, fs)
	go func() {
		internalResponse := <-c
		log.Printf("Done putting it into internalResponse (%v, %v)", fs, c)

		serialized, err := s.serializeEvent(internalResponse)
		// TODO think more about handling error cases here, lest we cause deadlocks
		// when the network is imperfect
		// TODO can we make all etcd requests idempotent, e.g. by generating the id
		// for a new snapshot in the requester?
		if err != nil {
			log.Printf("Error while serializing an event response: %s", err)
			return
		}
		_, err = kapi.Set(
			context.Background(),
			fmt.Sprintf("%s/filesystems/responses/%s/%s", ETCD_PREFIX, fs, requestId),
			serialized,
			&client.SetOptions{PrevExist: client.PrevNoExist, TTL: 604800 * time.Second},
		)
		if err != nil {
			log.Printf("Error while setting event response in etcd: %s", err)
			return
		}
	}()
	return nil
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
func (s *InMemoryState) updateSnapshotsFromKnownState(
	server, filesystem string, snapshots *[]snapshot,
) error {
	deleted, err := isFilesystemDeletedInEtcd(filesystem)
	if err != nil {
		return err
	}
	if deleted {
		// Filesystem is being deleted, so ignore it.
		return nil
	}

	s.globalSnapshotCacheLock.Lock()
	if _, ok := s.globalSnapshotCache[server]; !ok {
		s.globalSnapshotCache[server] = map[string][]snapshot{}
	}
	s.globalSnapshotCache[server][filesystem] = *snapshots
	s.globalSnapshotCacheLock.Unlock()

	log.Printf(
		"[updateSnapshots] checking %s master: %s == %s?",
		filesystem, s.masterFor(filesystem), server,
	)
	if s.masterFor(filesystem) == server {
		if len(*snapshots) > 0 {
			// notify any interested parties that there are some new snapshots on
			// the master

			latest := (*snapshots)[len(*snapshots)-1]
			log.Printf(
				"[updateSnapshots] publishing latest snapshot %v on %s",
				latest, filesystem,
			)
			go func() {
				err := s.newSnapsOnMaster.Publish(filesystem, latest)
				if err != nil {
					log.Printf(
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
			err := fs.newSnapsOnServers.Publish(server, true) // TODO publish latest, as above
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
		fs := pieces[4]

		s.mastersCacheLock.Lock()
		defer s.mastersCacheLock.Unlock()

		var modified bool
		if node.Value == "" {
			delete(s.mastersCache, fs)
			delete(filesystemBelongsToMe, fs)
		} else {
			old, ok := s.mastersCache[fs]
			if ok && old != node.Value {
				modified = true
			}
			if !ok {
				// new value
				modified = true
			}
			s.mastersCache[fs] = node.Value
			if node.Value == s.myNodeId {
				filesystemBelongsToMe[fs] = true
			} else {
				filesystemBelongsToMe[fs] = false
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
		s.globalStateCacheLock.Lock()
		defer s.globalStateCacheLock.Unlock()

		stateMetadata := &map[string]string{}
		if node.Value == "" {
			if _, ok := s.globalStateCache[server]; !ok {
				delete(s.globalStateCache[server], filesystem)
			} else {
				// We don't know about the server anyway, so there's nothing to do
			}
		} else {
			err := json.Unmarshal([]byte(node.Value), stateMetadata)
			if err != nil {
				log.Printf("Unable to marshal for updateStates - %s: %s", node.Value, err)
				return err
			}
			if _, ok := s.globalStateCache[server]; !ok {
				s.globalStateCache[server] = map[string]map[string]string{}
			} else {
				if _, ok := s.globalStateCache[server][filesystem]; !ok {
					// ok, we'll be setting it below...
				} else {
					// state already exists. check that we're updating with a
					// revision that is the same or newer...
					currentVersion := s.globalStateCache[server][filesystem]["version"]
					i, err := strconv.ParseUint(currentVersion, 10, 64)
					if err != nil {
						return err
					}
					if i > node.ModifiedIndex {
						log.Printf(
							"Out of order updates! %s is older than %s",
							s.globalStateCache[server][filesystem],
							node,
						)
						return nil
					}
				}
			}
			s.globalStateCache[server][filesystem] = *stateMetadata
			s.globalStateCache[server][filesystem]["version"] = fmt.Sprintf(
				"%d", node.ModifiedIndex,
			)
		}
		return nil
	}

	updateSnapshots := func(node *client.Node) error {
		// (0)/(1)dotmesh.io/(2)servers/
		//     (3)snapshots/(4):server/(5):filesystem = snapshots
		pieces := strings.Split(node.Key, "/")
		server := pieces[4]
		filesystem := pieces[5]

		if server == state.myNodeId {
			// Don't listen to updates from etcd about ourselves -
			// because we update that by calling
			// updateSnapshotsFromKnownState from the discovery code, and
			// that's better information.
			return
		}

		snapshots := &[]snapshot{}
		if node.Value == "" {
			// Key was deleted, so there's no snapshots
			return s.updateSnapshotsFromKnownState(server, filesystem, snapshots)
		} else {
			err := json.Unmarshal([]byte(node.Value), snapshots)
			if err != nil {
				log.Printf(
					"updateSnapshots: error trying to unmarshal '%s' for %s on %s, %s",
					node.Value, filesystem, server, node.Key,
				)
				return err
			}
			return s.updateSnapshotsFromKnownState(server, filesystem, snapshots)
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
	var kapi client.KeysAPI
	maybeDispatchEvent := func(node *client.Node) error {
		// (0)/(1)dotmesh.io/(2)filesystems/
		//     (3)requests/(4):filesystem/(5):request_id = request
		pieces := strings.Split(node.Key, "/")
		fs := pieces[4]
		mine, ok := filesystemBelongsToMe[fs]
		if ok && mine {
			// only act on events for filesystems that etcd reports as
			// belonging to me
			if err := s.deserializeDispatchAndRespond(fs, node, kapi); err != nil {
				return err
			}
		}
		return nil
	}
	getVariant := func(node *client.Node) string {
		// e.g. "masters" in (0)/(1)dotmesh.io/(2)filesystems/(3)masters/(4)1b25b8f5...
		pieces := strings.Split(node.Key, "/")
		if len(pieces) > 3 {
			return pieces[2] + "/" + pieces[3]
		}
		return ""
	}

	func() {
		s.etcdWaitTimestampLock.Lock()
		defer s.etcdWaitTimestampLock.Unlock()
		s.etcdWaitTimestamp = time.Now().UnixNano()
		s.etcdWaitState = "connect"
	}()

	kapi, err := getEtcdKeysApi()
	if err != nil {
		return err
	}

	func() {
		s.etcdWaitTimestampLock.Lock()
		defer s.etcdWaitTimestampLock.Unlock()
		s.etcdWaitTimestamp = time.Now().UnixNano()
		s.etcdWaitState = "insert initial admin password if not exists"
	}()

	// Do this every time, even if it fails.  This is to handle the case where
	// etcd gets wiped underneath us.
	err = s.insertInitialAdminPassword()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("failed to create initial admin")
	}

	func() {
		s.etcdWaitTimestampLock.Lock()
		defer s.etcdWaitTimestampLock.Unlock()
		s.etcdWaitTimestamp = time.Now().UnixNano()
		s.etcdWaitState = "initial get"
	}()

	// on first connect, fetch all of, well, everything
	current, err := kapi.Get(context.Background(),
		fmt.Sprint(ETCD_PREFIX),
		&client.GetOptions{Recursive: true, Sort: false, Quorum: true},
	)
	if err != nil {
		return err
	}

	func() {
		s.etcdWaitTimestampLock.Lock()
		defer s.etcdWaitTimestampLock.Unlock()
		s.etcdWaitTimestamp = time.Now().UnixNano()
		s.etcdWaitState = "initial processing"
	}()

	// find the masters and requests nodes
	var masters *client.Node
	var requests *client.Node
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
			if getVariant(child) == "filesystems/masters" {
				masters = child
			} else if getVariant(child) == "filesystems/requests" {
				requests = child
			} else if getVariant(child) == "servers/addresses" {
				serverAddresses = child
			} else if getVariant(child) == "servers/snapshots" {
				serverSnapshots = child
			} else if getVariant(child) == "servers/states" {
				serverStates = child
			} else if getVariant(child) == "registry/filesystems" {
				registryFilesystems = child
			} else if getVariant(child) == "registry/clones" {
				registryClones = child
			} else if getVariant(child) == "filesystems/transfers" {
				interclusterTransfers = child
			} else if getVariant(child) == "filesystems/dirty" {
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
	if requests != nil {
		for _, requestsForFilesystem := range requests.Nodes {
			for _, node := range requestsForFilesystem.Nodes {
				if err = maybeDispatchEvent(node); err != nil {
					return err
				}
			}
		}
	}
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
	watcher := kapi.Watcher(
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
				watcher = kapi.Watcher(
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
			if err = s.handleOneFilesystemDeletion(node.Node); err != nil {
				return err
			}
		} else if variant == "filesystems/requests" {
			if err = maybeDispatchEvent(node.Node); err != nil {
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
			if err = updateFilesystemsContainers(node.Node); err != nil {
				return err
			}
		} else if variant == "filesystems/dirty" {
			if err = updateFilesystemsDirty(node.Node); err != nil {
				return err
			}
		} else if variant == "filesystems/transfers" {
			if err = updateTransfers(node.Node); err != nil {
				return err
			}
		}
	}
}
