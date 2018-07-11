package statemachine

// state machinery
type stateFn func(*fsMachine) stateFn

// a "filesystem machine" or "filesystem state machine"
type fsMachine struct {
	// which ZFS filesystem this statemachine is operating on
	filesystemId string
	filesystem   *filesystem
	// channel of requests going in to the state machine
	requests chan *Event
	// inner versions of the above
	innerRequests chan *Event
	// inner responses don't need to be parameterized on request id because
	// they're guaranteed to only have one goroutine reading on the channel.
	innerResponses chan *Event
	// channel of responses coming out of the state machine, indexed by request
	// id so that multiple goroutines reading responses for the same filesystem
	// id won't get the wrong result.
	responses     map[string]chan *Event
	responsesLock *sync.Mutex
	// channel notifying etcd-updater whenever snapshot state changes
	snapshotsModified chan bool
	// pointer to global state, because it's convenient to have access to it
	state *InMemoryState
	// fsMachines live forever, whereas filesystem structs do not. so
	// filesystem struct's snapshotLock can live here so that it doesn't get
	// clobbered
	snapshotsLock *sync.Mutex
	// a place to store arguments to pass to the next state
	handoffRequest *Event
	// filesystem-sliced view of new snapshot events
	newSnapsOnServers *Observer
	// current state, status field for reporting/debugging and transition observer
	currentState            string
	status                  string
	lastTransitionTimestamp int64
	transitionObserver      *Observer
	lastS3TransferRequest   S3TransferRequest
	lastTransferRequest     TransferRequest
	lastTransferRequestId   string
	pushCompleted           chan bool
	dirtyDelta              int64
	sizeBytes               int64
	lastPollResult          *TransferPollResult
}

type S3TransferRequest struct {
	KeyID           string
	SecretKey       string
	Endpoint        string
	Direction       string
	LocalNamespace  string
	LocalName       string
	LocalBranchName string
	RemoteName      string
}

func (transferRequest S3TransferRequest) String() string {
	v := reflect.ValueOf(transferRequest)
	toString := ""
	for i := 0; i < v.NumField(); i++ {
		fieldName := v.Type().Field(i).Name
		if fieldName == "SecretKey" {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, "****")
		} else {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, v.Field(i).Interface())
		}
	}
	return toString
}

type TransferRequest struct {
	Peer             string // hostname
	User             string
	Port             int
	ApiKey           string //protected value in toString
	Direction        string // "push" or "pull"
	LocalNamespace   string
	LocalName        string
	LocalBranchName  string
	RemoteNamespace  string
	RemoteName       string
	RemoteBranchName string
	// TODO could also include SourceSnapshot here
	TargetCommit string // optional, "" means "latest"
}

func (transferRequest TransferRequest) String() string {
	v := reflect.ValueOf(transferRequest)
	toString := ""
	for i := 0; i < v.NumField(); i++ {
		fieldName := v.Type().Field(i).Name
		if fieldName == "ApiKey" {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, "****")
		} else {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, v.Field(i).Interface())
		}
	}
	return toString
}

type EventArgs map[string]interface{}
type Event struct {
	Name string
	Args *EventArgs
}

func (ea EventArgs) String() string {
	aggr := []string{}
	for k, v := range ea {
		aggr = append(aggr, fmt.Sprintf("%s: %+q", k, v))
	}
	return strings.Join(aggr, ", ")
}

func (e Event) String() string {
	return fmt.Sprintf("<Event %s: %s>", e.Name, e.Args)
}

type TransferPollResult struct {
	TransferRequestId string
	Peer              string // hostname
	User              string
	ApiKey            string
	Direction         string // "push" or "pull"

	// Hold onto this information, it might become useful for e.g. recursive
	// receives of clone filesystems.
	LocalNamespace   string
	LocalName        string
	LocalBranchName  string
	RemoteNamespace  string
	RemoteName       string
	RemoteBranchName string

	// Same across both clusters
	FilesystemId string

	// TODO add clusterIds? probably comes from etcd. in fact, could be the
	// discovery id (although that is only for bootstrap... hmmm).
	InitiatorNodeId string
	PeerNodeId      string

	// XXX a Transfer that spans multiple filesystem ids won't have a unique
	// starting/target snapshot, so this is in the wrong place right now.
	// although maybe it makes sense to talk about a target *final* snapshot,
	// with interim snapshots being an implementation detail.
	StartingCommit string
	TargetCommit   string

	Index              int    // i.e. transfer 1/4 (Index=1)
	Total              int    //                   (Total=4)
	Status             string // one of "starting", "running", "finished", "error"
	NanosecondsElapsed int64
	Size               int64 // size of current segment in bytes
	Sent               int64 // number of bytes of current segment sent so far
	Message            string
}
