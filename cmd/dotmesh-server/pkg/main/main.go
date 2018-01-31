/*
The dotmesh server.

Essentially an etcd-clustered set of state machines which correspond to ZFS
filesystems.
*/

package main

import (
	"fmt"
	"github.com/opentracing/opentracing-go"
	zipkin "github.com/openzipkin/zipkin-go-opentracing"
	"log" // TODO start using https://github.com/Sirupsen/logrus
	"os"
	"strconv"
	"strings"
	"time"
)

// initial setup
const ROOT_FS = "dmfs"
const ZFS = "zfs"
const ZPOOL = "zpool"
const META_KEY_PREFIX = "io.dotmesh:meta-"
const ETCD_PREFIX = "/dotmesh.io"
const CONTAINER_MOUNT_PREFIX = "/var/dotmesh"

var LOG_TO_STDOUT bool
var POOL string

var serverVersion string = "<uninitialized>"

func main() {

	var config Config

	//TODO:152 Migrate these env vars to be injected via scripts at startup.
	os.Setenv("CHECKPOINT_URL", "")
	os.Setenv("CHECKPOINT_DISABLE", "true")

	FILESYSTEM_METADATA_TIMEOUT_STRING := os.Getenv("FILESYSTEM_METADATA_TIMEOUT")

	if len(FILESYSTEM_METADATA_TIMEOUT_STRING) == 0 {
		FILESYSTEM_METADATA_TIMEOUT_STRING = "600"
	}

	FILESYSTEM_METADATA_TIMEOUT_INT, err := strconv.ParseInt(FILESYSTEM_METADATA_TIMEOUT_STRING, 10, 64)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// TODO: remove the different domains concept and have a proxy to services
	config = Config{
		FilesystemMetadataTimeout: FILESYSTEM_METADATA_TIMEOUT_INT,
	}

	err = installKubernetesPlugin()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	POOL = os.Getenv("POOL")
	if POOL == "" {
		POOL = "pool"
	}
	traceAddr := os.Getenv("TRACE_ADDR")
	if traceAddr != "" {
		collector, err := zipkin.NewHTTPCollector(
			fmt.Sprintf("http://%s:9411/api/v1/spans", traceAddr),
		)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		tracer, err := zipkin.NewTracer(
			zipkin.NewRecorder(collector, false, "127.0.0.1:0", "dotmesh"),
			zipkin.ClientServerSameSpan(true),
			zipkin.TraceID128Bit(true),
		)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		opentracing.InitGlobalTracer(tracer)
	}
	// TODO proper flag parsing
	if len(os.Args) > 1 && os.Args[1] == "--guess-ipv4-addresses" {
		addresses, err := guessIPv4Addresses()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println(strings.Join(addresses, ","))
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "--temporary-error-plugin" {
		s := NewInMemoryState("<unknown>", config)
		s.runErrorPlugin()
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "--debug" {
		LOG_TO_STDOUT = false
	} else {
		LOG_TO_STDOUT = true
	}
	setupLogging()

	localPoolId, err := findLocalPoolId()
	if err != nil {
		out("Unable to determine pool ID. Make sure to run me as root.\n" +
			"Please create a ZFS pool called '" + POOL + "'.\n" +
			"The following commands will create a toy pool-in-a-file:\n\n" +
			"    sudo truncate -s 10G /pool-datafile\n" +
			"    sudo zpool create pool /pool-datafile\n\n" +
			"Otherwise, see 'man zpool' for how to create a real pool.\n" +
			"If you don't have the 'zpool' tool installed, on Ubuntu 16.04, run:\n\n" +
			"    sudo apt-get install zfsutils-linux\n\n" +
			"On other distributions, follow the instructions at http://zfsonlinux.org/\n")
		log.Fatalf("Unable to find pool ID, I don't know who I am :( %s %s", err, localPoolId)
	}
	ips, _ := guessIPv4Addresses()
	log.Printf("Detected my node ID as %s (%s)", localPoolId, ips)
	s := NewInMemoryState(localPoolId, config)

	for _, filesystemId := range findFilesystemIdsOnSystem() {
		log.Printf("Initializing fsMachine for %s", filesystemId)
		go func() {
			s.initFilesystemMachine(filesystemId)
		}()
	}

	go runForever(
		s.checkForUpdates, "checkForUpdates",
		4*time.Hour, 4*time.Hour,
	)
	go runForever(
		s.updateAddressesInEtcd, "updateAddressesInEtcd",
		// ttl on address keys will be 60 seconds, so update them every 30
		// (hopefully updating them doesn't take >30 seconds)
		1*time.Second, 30*time.Second,
	)
	// kick off an on-startup perusal of which dm containers are running
	go runForever(s.fetchRelatedContainers, "fetchRelatedContainers",
		1*time.Second, 1*time.Second,
	)
	// kick off cleanup of deleted filesystems
	go runForever(s.cleanupDeletedFilesystems, "cleanupDeletedFilesystems",
		1*time.Second, 1*time.Second,
	)
	// TODO proper flag parsing
	if len(os.Args) > 1 && os.Args[1] == "--debug" {
		go runForever(s.fetchAndWatchEtcd, "fetchAndWatchEtcd",
			1*time.Second, 1*time.Second,
		)
		s.repl()
	} else {
		runForever(s.fetchAndWatchEtcd, "fetchAndWatchEtcd",
			1*time.Second, 1*time.Second,
		)
	}
}
