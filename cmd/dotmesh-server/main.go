/*
The dotmesh server.

Essentially an etcd-clustered set of state machines which correspond to ZFS
filesystems.
*/

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/dotmesh-io/dotmesh/pkg/messaging/nats"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"

	// registering metric counters
	_ "github.com/dotmesh-io/dotmesh/pkg/metrics"

	// notification provider
	_ "github.com/dotmesh-io/dotmesh/pkg/notification/nats"

	"github.com/opentracing/opentracing-go"
	zipkin "github.com/openzipkin/zipkin-go-opentracing"

	log "github.com/sirupsen/logrus"
)

// initial setup
const ROOT_FS = types.RootFS
const META_KEY_PREFIX = types.MetaKeyPrefix
const ETCD_PREFIX = types.EtcdPrefix

var ZFS string
var MOUNT_ZFS string
var ZPOOL string

var LOG_TO_STDOUT bool
var POOL string
var CONTAINER_MOUNT_PREFIX string

var serverVersion string = "<uninitialized>"

var containerMountDirLock sync.Mutex

func main() {
	logLevel := os.Getenv("LOG_LEVEL")
	levelEnum, err := log.ParseLevel(logLevel)
	if err != nil {
		// TODO put this back to info once we're done debugging, and make it so we can configure this in agent/operator/yaml/etc without a code change
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(levelEnum)
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

	var config Config

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
		NatsConfig:                nats.DefaultConfig(),
	}

	POOL = os.Getenv("POOL")
	if POOL == "" {
		fmt.Printf("Environment variable POOL must be set\n")
		os.Exit(1)
	}
	CONTAINER_MOUNT_PREFIX = os.Getenv("CONTAINER_MOUNT_PREFIX")
	if CONTAINER_MOUNT_PREFIX == "" {
		fmt.Printf("Environment variable CONTAINER_MOUNT_PREFIX must be set\n")
		os.Exit(1)
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
	if len(os.Args) > 1 && os.Args[1] == "--temporary-error-plugin" {
		s := NewInMemoryState(config)
		s.runErrorPlugin()
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "--debug" {
		LOG_TO_STDOUT = false
	} else {
		LOG_TO_STDOUT = true
	}
	setupLogging()

	err = installKubernetesPlugin()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	zRoot := os.Getenv("ZFS_USERLAND_ROOT")
	if zRoot == "" {
		fmt.Println("Must specify ZFS_USERLAND_ROOT, e.g. /opt/zfs-0.7")
		os.Exit(1)
	}
	ZFS = zRoot + "/sbin/zfs"
	MOUNT_ZFS = zRoot + "/sbin/mount.zfs"
	ZPOOL = zRoot + "/sbin/zpool"
	ips, _ := guessIPv4Addresses()
	log.Printf("Detected my node IPs as %s", ips)

	// etcdClient, err := getEtcdKeysApi()
	// if err != nil {
	// 	log.Fatalf("Unable to get Etcd client: '%s'", err)
	// }
	// config.EtcdClient = etcdClient

	fsStore, regStore, serverStore, usersIdxStore := getKVDBStores()
	config.FilesystemStore = fsStore
	config.RegistryStore = regStore
	config.ServerStore = serverStore

	config.ZFSExecPath = ZFS
	config.ZPoolPath = ZPOOL
	config.PoolName = POOL

	if os.Getenv("DOTMESH_SERVER_PORT") != "" {
		config.APIServerPort = os.Getenv("DOTMESH_SERVER_PORT")
	} else {
		config.APIServerPort = client.SERVER_PORT
	}

	// kvClient := kv.New(etcdClient, ETCD_PREFIX)
	config.UserManager = user.New(usersIdxStore)

	s := NewInMemoryState(config)

	// Set the URL to an empty string (or leave it unset) to disable checkpoints
	checkpointUrl := os.Getenv("DOTMESH_UPGRADES_URL")
	if checkpointUrl != "" {
		var checkInterval int
		// If the URL is specified, a valid interval must also be specified
		ci := os.Getenv("DOTMESH_UPGRADES_INTERVAL_SECONDS")
		checkInterval, err = strconv.Atoi(ci)
		if err != nil {
			fmt.Printf("Error parsing DOTMESH_UPGRADES_INTERVAL_SECONDS value %+v: %+v\n", ci, err)
			os.Exit(1)
		}

		// This is the name that the checkpoint library looks for
		os.Setenv("CHECKPOINT_URL", checkpointUrl)

		go runForever(
			s.checkForUpdates, "checkForUpdates",
			time.Duration(checkInterval)*time.Second, time.Duration(checkInterval)*time.Second,
		)
	}

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
	// TODO: remove, replaced with watcher
	go runForever(s.cleanupDeletedFilesystems, "cleanupDeletedFilesystems",
		1*time.Second, 1*time.Second,
	)
	// kick off reporting on zpool status
	go runForever(s.zfs.ReportZpoolCapacity, "reportZPoolUsageReporter",
		10*time.Minute, 10*time.Minute,
	)
	// kick off watching etcd
	go runForever(s.fetchAndWatchEtcd, "fetchAndWatchEtcd",
		1*time.Second, 1*time.Second,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		err := s.subscribeToClusterEvents(ctx)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("failed to subscribe to cluster events")
		}
	}()

	// tell k8s we're live
	s.runLivenessServer()
}
