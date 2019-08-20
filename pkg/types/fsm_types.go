package types

import (
	"fmt"
	"io"
	"reflect"
)

// InputFile is used to write files to the disk on the local node.
type InputFile struct {
	Filename string
	Contents io.Reader
	User     string
	Response chan *Event
}

// OutputFile is used to read files from the disk on the local node
// this is always done against a specific, already mounted snapshotId
// the mount path of the snapshot is passed through via SnapshotMountPath
type OutputFile struct {
	Filename          string
	SnapshotMountPath string
	Contents          io.Writer
	User              string
	Response          chan *Event
}

type TransferUpdateKind int

const (
	TransferStart TransferUpdateKind = iota
	TransferGotIds
	TransferCalculatedSize
	TransferTotalAndSize
	TransferProgress
	TransferIncrementIndex
	TransferStartS3Bucket
	TransferNextS3File
	TransferFinishedS3File
	TransferS3Stuck
	TransferSent
	TransferFinished
	TransferStatus

	TransferGetCurrentPollResult
)

type TransferUpdate struct {
	Kind TransferUpdateKind

	Changes TransferPollResult

	GetResult chan TransferPollResult
}

type TransferPollResult struct {
	// Meta is populated by the KV store implementer
	Meta *KVMeta `json:"-"`

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

func (t TransferPollResult) String() string {
	v := reflect.ValueOf(t)
	protectedValue := "****"
	toString := "TransferPollResult : "
	for i := 0; i < v.NumField(); i++ {
		fieldName := v.Type().Field(i).Name
		if fieldName == "ApiKey" {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, protectedValue)
		} else {
			toString = toString + fmt.Sprintf(" %v=%v,", fieldName, v.Field(i).Interface())
		}
	}
	return toString
}
