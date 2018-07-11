package main

import (
	"github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/statemachine"
	"sync"
)

type InMemoryState struct {
	config                     Config
	filesystems                map[string]*fsMachine
	filesystemsLock            *sync.RWMutex
	myNodeId                   string
	mastersCache               map[string]string
	mastersCacheLock           *sync.RWMutex
	serverAddressesCache       map[string]string
	serverAddressesCacheLock   *sync.RWMutex
	globalSnapshotCache        map[string]map[string][]snapshot
	globalSnapshotCacheLock    *sync.RWMutex
	globalStateCache           map[string]map[string]map[string]string
	globalStateCacheLock       *sync.RWMutex
	globalContainerCache       map[string]containerInfo
	globalContainerCacheLock   *sync.RWMutex
	etcdWaitTimestamp          int64
	etcdWaitState              string
	etcdWaitTimestampLock      *sync.Mutex
	localReceiveProgress       *Observer
	newSnapsOnMaster           *Observer
	registry                   *Registry
	containers                 *DockerClient
	containersLock             *sync.RWMutex
	fetchRelatedContainersChan chan bool
	interclusterTransfers      map[string]TransferPollResult
	interclusterTransfersLock  *sync.RWMutex
	globalDirtyCacheLock       *sync.RWMutex
	globalDirtyCache           map[string]dirtyInfo
	userManager                user.UserManager

	debugPartialFailCreateFilesystem bool
	versionInfo                      *VersionInfo
}
