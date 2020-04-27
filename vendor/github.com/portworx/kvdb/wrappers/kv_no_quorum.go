package wrappers

import (
	"math/rand"
	"time"

	"github.com/portworx/kvdb"
	"github.com/sirupsen/logrus"
)

type noKvdbQuorumWrapper struct {
	kvBaseWrapper
	randGen *rand.Rand
}

// NewNoKvdbQuorumWrapper constructs a new kvdb.Kvdb.
func NewNoKvdbQuorumWrapper(
	kv kvdb.Kvdb,
	options map[string]string,
) (kvdb.Kvdb, error) {
	logrus.Infof("creating quorum check wrapper")
	return &noKvdbQuorumWrapper{
		kvBaseWrapper: kvBaseWrapper{
			name:        kvdb.Wrapper_NoQuorum,
			wrappedKvdb: kv,
		},
		randGen: rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Version returns the supported version of the mem implementation
func Version(url string, kvdbOptions map[string]string) (string, error) {
	return kvdb.MemVersion1, nil
}

func (k *noKvdbQuorumWrapper) String() string {
	return k.wrappedKvdb.String()
}

func (k *noKvdbQuorumWrapper) Capabilities() int {
	return k.wrappedKvdb.Capabilities()
}

func (k *noKvdbQuorumWrapper) Get(key string) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) Snapshot(prefixes []string, consistent bool) (kvdb.Kvdb, uint64, error) {
	return nil, 0, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) Put(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) GetVal(key string, v interface{}) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) Create(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) Update(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) Enumerate(prefix string) (kvdb.KVPairs, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) Delete(key string) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) DeleteTree(prefix string) error {
	return kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) Keys(prefix, sep string) ([]string, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) CompareAndSet(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
	prevValue []byte,
) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) CompareAndDelete(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) delayWatch() {
	time.Sleep(time.Duration(20+k.randGen.Intn(20)) * time.Second)
}

func (k *noKvdbQuorumWrapper) WatchKey(
	key string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	// slow-down retrying watch since we don't know if kvdb is still up
	k.delayWatch()
	return k.wrappedKvdb.WatchKey(key, waitIndex, opaque, cb)
}

func (k *noKvdbQuorumWrapper) WatchTree(
	prefix string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	// slow-down retrying watch since we don't know if kvdb is still up
	k.delayWatch()
	return k.wrappedKvdb.WatchTree(prefix, waitIndex, opaque, cb)
}

func (k *noKvdbQuorumWrapper) Lock(key string) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) LockWithID(
	key string,
	lockerID string,
) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) LockWithTimeout(
	key string,
	lockerID string,
	lockTryDuration time.Duration,
	lockHoldDuration time.Duration,
) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) Unlock(kvp *kvdb.KVPair) error {
	return k.wrappedKvdb.Unlock(kvp)
}

func (k *noKvdbQuorumWrapper) EnumerateWithSelect(
	prefix string,
	enumerateSelect kvdb.EnumerateSelect,
	copySelect kvdb.CopySelect,
) ([]interface{}, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) GetWithCopy(
	key string,
	copySelect kvdb.CopySelect,
) (interface{}, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) TxNew() (kvdb.Tx, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) SnapPut(snapKvp *kvdb.KVPair) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) AddUser(username string, password string) error {
	return kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) RemoveUser(username string) error {
	return kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) GrantUserAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	return kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) RevokeUsersAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	return kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) SetFatalCb(f kvdb.FatalErrorCB) {
	k.wrappedKvdb.SetFatalCb(f)
}

func (k *noKvdbQuorumWrapper) SetLockTimeout(timeout time.Duration) {
	k.wrappedKvdb.SetLockTimeout(timeout)
}

func (k *noKvdbQuorumWrapper) GetLockTimeout() time.Duration {
	return k.wrappedKvdb.GetLockTimeout()
}

func (k *noKvdbQuorumWrapper) Serialize() ([]byte, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) Deserialize(b []byte) (kvdb.KVPairs, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) AddMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	return nil, kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) RemoveMember(nodeName, nodeIP string) error {
	return kvdb.ErrNoQuorum
}

func (k *noKvdbQuorumWrapper) UpdateMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	return k.wrappedKvdb.UpdateMember(nodeIP, nodePeerPort, nodeName)
}

func (k *noKvdbQuorumWrapper) ListMembers() (map[string]*kvdb.MemberInfo, error) {
	return k.wrappedKvdb.ListMembers()
}

func (k *noKvdbQuorumWrapper) SetEndpoints(endpoints []string) error {
	return k.wrappedKvdb.SetEndpoints(endpoints)
}

func (k *noKvdbQuorumWrapper) GetEndpoints() []string {
	return k.wrappedKvdb.GetEndpoints()
}

func (k *noKvdbQuorumWrapper) Defragment(endpoint string, timeout int) error {
	return k.wrappedKvdb.Defragment(endpoint, timeout)
}

func init() {
	if err := kvdb.RegisterWrapper(kvdb.Wrapper_NoQuorum, NewNoKvdbQuorumWrapper); err != nil {
		panic(err.Error())
	}
	if err := kvdb.RegisterWrapper(kvdb.Wrapper_Log, NewLogWrapper); err != nil {
		panic(err.Error())
	}
}
