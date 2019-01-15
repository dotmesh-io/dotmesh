package fsm

import (
	"fmt"
	"sync"

	"github.com/coreos/etcd/client"

	dmclient "github.com/dotmesh-io/dotmesh/pkg/client"
	"github.com/dotmesh-io/dotmesh/pkg/container"
	"github.com/dotmesh-io/dotmesh/pkg/observer"
	"github.com/dotmesh-io/dotmesh/pkg/registry"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
)

// state machinery
type StateFn func(*FsMachine) StateFn

type StateManager interface {
	InitFilesystemMachine(filesystemId string) (FSM, error)
	GetFilesystemMachine(filesystemId string) (FSM, error)

	// ActivateFilesystem(filesystemId string) error
	AlignMountStateWithMasters(filesystemId string) error
	ActivateClone(topLevelFilesystemId, originFilesystemId, originSnapshotId, newCloneFilesystemId, newBranchName string) (string, error)
	DeleteFilesystem(filesystemId string) error
	DeleteFilesystemFromMap(filesystemId string)
	// current node ID
	NodeID() string

	UpdateSnapshotsFromKnownState(server, filesystem string, snapshots []*types.Snapshot) error
	SnapshotsFor(server string, filesystemId string) ([]types.Snapshot, error)
	SnapshotsForCurrentMaster(filesystemId string) ([]types.Snapshot, error)

	AddressesForServer(server string) []string

	RegisterNewFork(originFilesystemId, originSnapshotId, forkNamespace, forkName, forkFilesystemId string) error

	// TODO: move under a separate interface for Etcd related things
	MarkFilesystemAsLiveInEtcd(topLevelFilesystemId string) error
}

// a "filesystem machine" or "filesystem state machine"
type FsMachine struct {
	// which ZFS filesystem this statemachine is operating on
	filesystemId string
	filesystem   *types.Filesystem

	// channels for uploading and downloading file data
	fileInputIO  chan *types.InputFile
	fileOutputIO chan *types.OutputFile

	// channel of requests going in to the state machine
	requests chan *types.Event
	// inner versions of the above
	innerRequests chan *types.Event
	// inner responses don't need to be parameterized on request id because
	// they're guaranteed to only have one goroutine reading on the channel.
	innerResponses chan *types.Event
	// channel of responses coming out of the state machine, indexed by request
	// id so that multiple goroutines reading responses for the same filesystem
	// id won't get the wrong result.
	responses     map[string]chan *types.Event
	responsesLock *sync.Mutex
	// channel notifying etcd-updater whenever snapshot state changes
	snapshotsModified chan bool
	// pointer to global state, because it's convenient to have access to it
	// state *InMemoryState

	containerClient container.Client
	etcdClient      client.KeysAPI
	state           StateManager
	userManager     user.UserManager
	registry        registry.Registry

	localReceiveProgress observer.Observer
	newSnapsOnMaster     observer.Observer
	deathObserver        observer.Observer

	// fsMachines live forever, whereas filesystem structs do not. so
	// filesystem struct's snapshotLock can live here so that it doesn't get
	// clobbered
	snapshotsLock *sync.Mutex
	// a place to store arguments to pass to the next state
	handoffRequest *types.Event
	// filesystem-sliced view of new snapshot events
	newSnapsOnServers observer.Observer
	// current state, status field for reporting/debugging and transition observer
	currentState            string
	status                  string
	lastTransitionTimestamp int64
	transitionObserver      observer.Observer
	lastS3TransferRequest   types.S3TransferRequest
	lastTransferRequest     types.TransferRequest
	lastTransferRequestId   string
	pushCompleted           chan bool
	dirtyDelta              int64
	sizeBytes               int64
	transferUpdates         chan types.TransferUpdate
	// only to be accessed via the updateEtcdAboutTransfers goroutine!
	currentPollResult types.TransferPollResult

	// state machine metadata
	// Moved from InMemoryState:
	// server id => filesystem id => state machine metadata
	//globalStateCache:     make(map[string]map[string]map[string]string),

	// new structure: NodeID => State machine metadata
	stateMachineMetadata   map[string]map[string]string
	stateMachineMetadataMu *sync.RWMutex

	// NodeID => snapshot metadata
	snapshotCache   map[string][]*types.Snapshot
	snapshotCacheMu *sync.RWMutex

	filesystemMetadataTimeout int64

	// path to zfs executable (ZFS_USERLAND_ROOT + /sbin/zfs)
	zfsPath string
	// path to zpool exec
	zpoolPath string

	// poolName must be set through POOL environment variable
	poolName string

	mountZFS string
}

type dirtyInfo struct {
	Server     string
	DirtyBytes int64
	SizeBytes  int64
}

func castToMetadata(val interface{}) (types.Metadata, error) {

	switch v := val.(type) {
	case types.Metadata:
		return v, nil
	case *types.Metadata:
		return *v, nil
	case map[string]interface{}:
		meta := types.Metadata{}
		// massage the data into the right type
		cast := val.(map[string]interface{})
		for k, v := range cast {
			meta[k] = v.(string)
		}
		return meta, nil
	default:
		return nil, fmt.Errorf("unknown type: %v", val)
	}
}

type transferFn func(
	f *FsMachine,
	fromFilesystemId, fromSnapshotId, toFilesystemId, toSnapshotId string,
	transferRequestId string,
	client *dmclient.JsonRpcClient, transferRequest *types.TransferRequest,
) (*types.Event, StateFn)
