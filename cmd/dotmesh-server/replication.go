package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os/exec"
	"strings"

	dmclient "github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
	"github.com/gorilla/mux"

	log "github.com/sirupsen/logrus"
)

// machinery for remote zfs replication

// const BUF_LEN = 131072         // 128kb of replication data sent per update (of etcd)
const BUF_LEN = types.BufLength // 128kb of replication data sent per update (of etcd)
const START_SNAPSHOT = "START"  // meaning "the start of the filesystem"

func (z *ZFSSender) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// respond to GET requests with a ZFS data stream
	vars := mux.Vars(r)
	z.fromSnap = vars["fromSnap"]
	z.toSnap = vars["toSnap"]
	z.filesystem = vars["filesystem"]

	// TODO: add a coarse grained lock to start with: stop other readers from
	// this filesystem, and also stop us moving this filesystem to another node
	// while it's being read from (although maybe avoid cancelling
	// inter-cluster replications, and instead block migrations on those)
	//
	// later, implement extent-based locking (ie, this read locks snapshots
	// B-G, but allow A or H to be deleted)

	// z.state.lockFilesystem(z.filesystem)
	// defer z.state.unlockFilesystem(z.filesystem)

	masterNodeID, err := z.state.registry.CurrentMasterNode(z.filesystem)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("[ZFSSender.ServeHTTP] master node for filesystem not found")
		http.Error(w, fmt.Sprintf("master node for filesystem %s not found", z.filesystem), 500)
		return
	}

	if masterNodeID != z.state.myNodeId {
		admin, err := z.state.userManager.Get(&user.Query{Ref: "admin"})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("Can't establish API key to proxy pull: %+v.\n", err)))
			log.Printf("[ZFSSender:%s] Can't establish API key to proxy pull: %+v.", z.filesystem, err)
			return
		}

		addresses := z.state.AddressesForServer(masterNodeID)
		url, err := dmclient.DeduceUrl(context.Background(), addresses, "internal", "admin", admin.ApiKey) // FIXME, need master->name mapping, see how handover works normally
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("%s", err)))
			log.Printf("[ZFSSender:%s] Can't establish URL to proxy pull: %+v.", z.filesystem, err)
			return
		}
		url = fmt.Sprintf(
			"%s/filesystems/%s/%s/%s",
			url,
			z.filesystem,
			z.fromSnap,
			z.toSnap,
		)

		// Proxy request to the master
		req, err := http.NewRequest(
			"GET", url,
			r.Body,
		)

		req.SetBasicAuth(
			"admin",
			admin.ApiKey,
		)

		log.Printf("[ZFSSender:%s] Proxying pull from %s: %s", z.filesystem, masterNodeID, url)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("Can't proxy pull from %s: %+v.\n", url, err)))
			log.Printf("[ZFSSender:%s] Can't proxy pull from %s: %+v.", z.filesystem, url, err)
			return
		}

		finished := make(chan bool)
		log.Printf("[ZFSSender:ServeHTTP] Got HTTP response %+v", resp.StatusCode)
		w.WriteHeader(resp.StatusCode)
		go pipe(resp.Body, url,
			w, "proxied pull recipient",
			finished,
			make(chan *Event),
			func(e *Event, c chan *Event) {},
			func(bytes int64, t int64) {},
			"none",
		)
		defer resp.Body.Close()
		log.Printf("[ZFSSender:ServeHTTP:%s] Waiting for finish signal...", z.filesystem)
		_ = <-finished
		return
	}

	log.Printf(
		"[ZFSSender:ServeHTTP] Starting to send replication stream for %s from %s => %s",
		z.filesystem, z.fromSnap, z.toSnap,
	)

	// command writes into pipe
	var cmd *exec.Cmd

	snaps, err := z.state.SnapshotsFor(masterNodeID, z.filesystem)
	if err != nil {
		log.Printf(
			"[ZFSSender:ServeHTTP] Error getting snaps for fs before calculating prelude in from zfs send of %s from %s => %s: %s",
			z.filesystem, z.fromSnap, z.toSnap, err,
		)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Unable to get snaps for filesystem to calculate prelude: %s\n", err)))
		return
	}

	prelude, err := calculatePrelude(snaps, z.toSnap)
	if err != nil {
		log.Printf(
			"[ZFSSender:ServeHTTP] Error calculating prelude in from zfs send of %s from %s => %s: %s",
			z.filesystem, z.fromSnap, z.toSnap, err,
		)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Unable to calculate prelude: %s\n", err)))
		return
	}

	if z.fromSnap == "START" {
		logZFSCommand(z.filesystem, fmt.Sprintf("%s send -p -R %s@%s", ZFS, fq(z.filesystem), z.toSnap))
		cmd = exec.Command(
			// -R sends interim snapshots as well
			ZFS, "send", "-p", "-R", fq(z.filesystem)+"@"+z.toSnap,
		)
	} else {
		var fromSnap string
		// in clone case, z.fromSnap must be fully qualified
		if strings.Contains(z.fromSnap, "@") {
			// send a clone, so make it fully qualified
			fromSnap = fq(z.fromSnap)
		} else {
			// presume it refers to a snapshot
			fromSnap = z.fromSnap
		}
		logZFSCommand(z.filesystem, fmt.Sprintf("%s send -p -I %s %s@%s", ZFS, fromSnap, fq(z.filesystem), z.toSnap))
		cmd = exec.Command(
			ZFS, "send", "-p",
			"-I", fromSnap, fq(z.filesystem)+"@"+z.toSnap,
		)
	}

	// How to set HTTP response code based on return code of process?
	// (we can't - it's too late by the time we know the return code)
	pipeReader, pipeWriter := io.Pipe()

	defer pipeWriter.Close()
	defer pipeReader.Close()

	preludeEncoded, err := encodePrelude(prelude)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Can't encode prelude: %s\n", err)))
		log.Printf("[ZFSSender:%s] Can't encode prelude: %+v.", z.filesystem, err)
		return
	}

	cmd.Stdout = pipeWriter
	cmd.Stderr = getLogfile("zfs-send-errors")

	finished := make(chan bool)
	go pipe(
		pipeReader, fmt.Sprintf("stdout of zfs send for %s", z.filesystem),
		w, "http response body",
		finished,
		make(chan *Event),
		func(e *Event, c chan *Event) {},
		func(bytes int64, t int64) {},
		"compress",
	)

	log.Printf(
		"[ZFSSender:%s] Writing prelude of %d bytes (encoded): %s",
		z.filesystem, len(preludeEncoded), preludeEncoded,
	)
	pipeWriter.Write(preludeEncoded)

	log.Printf(
		"[ZFSSender:%s] About to Run() for %s => %s",
		z.filesystem, z.fromSnap, z.toSnap,
	)
	err = cmd.Run()
	log.Printf(
		"[ZFSSender:%s] Finished Run() for %s => %s: %s",
		z.filesystem, z.fromSnap, z.toSnap, err,
	)
	if err != nil {
		log.Printf(
			"[ZFSSender:%s] Error from zfs send from %s => %s: %s, check zfs-send-errors.log",
			z.filesystem, z.fromSnap, z.toSnap, err,
		)
	}
	// XXX Adding the log messages below seemed to stop a deadlock, not sure
	// why. For now, let's just leave them in...
	log.Printf("[ZFSSender:%s] Closing pipes...", z.filesystem)
	pipeWriter.Close()
	pipeReader.Close()

	log.Printf("[ZFSSender:%s] Waiting for finish signal...", z.filesystem)
	_ = <-finished
	log.Printf("[ZFSSender:%s] Done!", z.filesystem)
}

// POST request => zfs recv command (only used by recipient of "dm push", ie pushPeerState)
func (z *ZFSReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	log.Printf("[ZFSReceiver:ServeHTTP] r: %v vars: %v", r, vars)
	z.fromSnap = vars["fromSnap"]
	z.toSnap = vars["toSnap"]
	z.filesystem = vars["filesystem"]
	// _ = make([]byte, BUF_LEN)

	// TODO: add a coarse grained lock to start with: stop other writers from
	// writing to this filesystem (unlike readers, this is strictly
	// one-at-a-time), and also stop us moving this filesystem to another node
	// while it's being written to
	// z.state.lockFilesystem(z.filesystem)
	// defer z.state.unlockFilesystem(z.filesystem)

	state, err := z.state.getCurrentState(z.filesystem)
	if err != nil {
		log.Printf("[ZFSReceiver:ServeHTTP] error calling getCurrentState(%s): %v", z.filesystem, err)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(fmt.Sprintf("Can't find state of filesystem %s.\n", z.filesystem)))
		return
	}
	masterNodeID, err := z.state.registry.CurrentMasterNode(z.filesystem)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("[ZFSReceiver.ServeHTTP] master node for filesystem not found")
		http.Error(w, fmt.Sprintf("master node for filesystem %s not found", z.filesystem), 500)
		return
	}

	if masterNodeID == z.state.myNodeId {
		if state != "pushPeerState" {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(fmt.Sprintf(
				"Host is master for this filesystem (%s), but can't write to it "+
					"because state is %s.\n", z.filesystem, state)))
			log.Printf("[ZFSReceiver:ServeHTTP] Host is master for this filesystem (%s), but can't write to it "+
				"because state is %s.\n", z.filesystem, state)
			return
		}
		// else OK, we can proceed
	} else {
		// below: a sort of proxy layer so that pushes can make it to the right
		// master, even if they land on the wrong server.

		admin, err := z.state.userManager.Get(&user.Query{Ref: "admin"})
		if err != nil {
			w.Write([]byte(fmt.Sprintf("Can't establish API key to proxy push: %+v.\n", err)))
			log.Printf("[ZFSReceiver:ServeHTTP:%s] Can't establish API key to proxy push: %+v", z.filesystem, err)
			return
		}

		addresses := z.state.AddressesForServer(masterNodeID)

		url, err := dmclient.DeduceUrl(context.Background(), addresses, "internal", "admin", admin.ApiKey)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("%s", err)))
			log.Printf("[ZFSReceiver:ServeHTTP:%s] Can't establish URL to proxy push: %+v", z.filesystem, err)
			return
		}
		url = fmt.Sprintf(
			"%s/filesystems/%s/%s/%s",
			url, // FIXME, need master->name mapping, see how handover works normally
			z.filesystem,
			z.fromSnap,
			z.toSnap,
		)

		// Proxy request to the master
		req, err := http.NewRequest(
			"POST", url,
			r.Body,
		)

		req.SetBasicAuth(
			"admin",
			admin.ApiKey,
		)
		postClient := new(http.Client)
		log.Printf("[ZFSReceiver:%s] Proxying push to %s: %s", z.filesystem, masterNodeID, url)
		resp, err := postClient.Do(req)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("Can't proxy push to %s: %+v.\n", url, err)))
			log.Printf("[ZFSReceiver:%s] Can't proxy push to %s: %+v", z.filesystem, url, err)
			return
		}

		finished := make(chan bool)
		log.Printf("[ZFSReceiver:%s] Got HTTP response %+v", z.filesystem, resp.StatusCode)
		w.WriteHeader(resp.StatusCode)
		go pipe(resp.Body, url,
			w, "proxied push recipient",
			finished,
			make(chan *Event),
			func(e *Event, c chan *Event) {},
			func(bytes int64, t int64) {},
			"compress",
		)
		defer resp.Body.Close()
		log.Printf("[ZFSReceiver:%s] Waiting for finish signal...", z.filesystem)
		_ = <-finished
		return
	}

	// Ok, so we're ready to receive the push. Our fsmachine must be in pushPeerState,
	// and is therefore blocking on us to tell it we've finished, one way or another, via
	// z.state.notifyPushCompleted(z.filesystem, true/false) so we'd better do that in every path.

	logZFSCommand(z.filesystem, fmt.Sprintf("%s recv %s", ZFS, fq(z.filesystem)))

	cmd := exec.Command(ZFS, "recv", fq(z.filesystem))
	pipeReader, pipeWriter := io.Pipe()
	defer pipeReader.Close()
	defer pipeWriter.Close()
	cmd.Stdin = pipeReader
	cmd.Stdout = w

	errBuffer := bytes.Buffer{}

	cmd.Stderr = &errBuffer
	finished := make(chan bool)

	go pipe(
		r.Body, fmt.Sprintf("http request body for %s", z.filesystem),
		pipeWriter, "zfs recv stdin", finished,
		make(chan *Event),
		func(e *Event, c chan *Event) {},
		func(bytes int64, t int64) {
			go func() {
				// ~~~~~~~~~~~~~
				// ~ THE BYTES ~
				// ~ THEY FLOW ~
				// ~ WORRY NOT ~
				// ~~~~~~~~~~~~~
				err := z.state.localReceiveProgress.Publish(z.filesystem, bytes)
				if err != nil {
					log.Printf("[ZFSReceiver:%s] error notifying localReceiveProcess", z.filesystem)
				}
			}()
		},
		"decompress",
	)

	log.Printf("[ZFSReceiver:%s] about to start consuming prelude on %v", z.filesystem, pipeReader)
	prelude, err := consumePrelude(pipeReader)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Unable to parse prelude for %s: %s\n", z.filesystem, err)))
		log.Printf("[ZFSReceiver:%s] Unable to parse prelude: %s\n", z.filesystem, err)

		go z.state.notifyPushCompleted(z.filesystem, false)

		return
	}
	log.Printf("[ZFSReceiver:%s] Got prelude %v", z.filesystem, prelude)

	err = cmd.Run()
	if err != nil {
		log.Printf(
			"[ZFSReceiver:%s] Got error %s when running zfs recv, check the logs for output that looks like it's from zfs",
			z.filesystem, err,
		)
		pipeReader.Close()
		pipeWriter.Close()
		_ = <-finished
		readErr, err := ioutil.ReadAll(&errBuffer)
		if err != nil {
			// an error with your error. this is a bad day.
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("Unable to read error: %+v\n", err)))
			log.Printf(
				"[ZFSReceiver:%s] Unable to read: %+v",
				z.filesystem, err,
			)

			go z.state.notifyPushCompleted(z.filesystem, false)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(
			"Unable to receive %s: %s, stderr: %s\n",
			z.filesystem, err, readErr,
		)))
		log.Printf(
			"[ZFSReceiver:%s] Unable to receive: %+v, stderr: %s",
			z.filesystem, err, readErr,
		)

		go z.state.notifyPushCompleted(z.filesystem, false)
		return
	}

	pipeReader.Close()
	pipeWriter.Close()
	_ = <-finished

	err = applyPrelude(prelude, fq(z.filesystem))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Unable to apply prelude for %s: %s\n", z.filesystem, err)))
		log.Printf(
			"[ZFSReceiver:%s] Unable to apply prelude: %+v",
			z.filesystem, err,
		)

		go z.state.notifyPushCompleted(z.filesystem, false)
		return
	}

	log.Printf("[ZFSReceiver:%s] Notifying fsmachine of success", z.filesystem)

	go z.state.notifyPushCompleted(z.filesystem, true)
}

type ZFSSender struct {
	state      *InMemoryState
	filesystem string
	fromSnap   string // "START" for "from the start"
	toSnap     string
}

type ZFSReceiver struct {
	state      *InMemoryState
	filesystem string
	fromSnap   string // "START" for "from the start"
	toSnap     string
}

func (s *InMemoryState) NewZFSSendingServer() http.Handler {
	return &ZFSSender{
		state: s,
	}
}

func (s *InMemoryState) NewZFSReceivingServer() http.Handler {
	return &ZFSReceiver{
		state: s,
	}
}
