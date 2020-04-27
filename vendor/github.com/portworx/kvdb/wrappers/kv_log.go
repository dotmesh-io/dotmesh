package wrappers

import (
	"fmt"
	"os"
	"time"

	"github.com/portworx/kvdb"
	"github.com/sirupsen/logrus"
)

const (
	opType         = "operation"
	errString      = "error"
	output         = "output"
	defaultLogPath = "kvdb_audit.log"
)

type kvBaseWrapper struct {
	// name is the name of this wrapper
	name kvdb.WrapperName
	// wrappedKvdb is the Kvdb wrapped by this wrapper
	wrappedKvdb kvdb.Kvdb
}

func (b *kvBaseWrapper) WrapperName() kvdb.WrapperName {
	return b.name
}

func (b *kvBaseWrapper) WrappedKvdb() kvdb.Kvdb {
	return b.wrappedKvdb
}

func (b *kvBaseWrapper) Removed() {
}

func (b *kvBaseWrapper) SetWrappedKvdb(kvdb kvdb.Kvdb) error {
	b.wrappedKvdb = kvdb
	return nil
}

type logKvWrapper struct {
	kvBaseWrapper
	logger *logrus.Logger
	fd     *os.File
}

func (b *logKvWrapper) Removed() {
	b.logger.SetOutput(os.Stdout)
	if b.fd != nil {
		b.fd.Close()
		b.fd = nil
	}
}

// New constructs a new kvdb.Kvdb.
func NewLogWrapper(
	kv kvdb.Kvdb,
	options map[string]string,
) (kvdb.Kvdb, error) {
	logrus.Infof("creating kvdb logging wrapper, options: %v", options)
	wrapper := &logKvWrapper{
		kvBaseWrapper{
			name:        kvdb.Wrapper_Log,
			wrappedKvdb: kv,
		},
		nil,
		nil,
	}

	path, ok := options[kvdb.LogPathOption]
	if !ok {
		path = defaultLogPath
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create log at %s: %v", path, err)
	}
	wrapper.fd = file
	wrapper.logger = logrus.New()
	wrapper.logger.SetFormatter(&logrus.TextFormatter{})
	wrapper.logger.SetOutput(file)
	wrapper.logger.SetLevel(logrus.InfoLevel)
	return wrapper, nil
}

func (k *logKvWrapper) String() string {
	return k.wrappedKvdb.String()
}

func (k *logKvWrapper) Capabilities() int {
	return k.wrappedKvdb.Capabilities()
}

func (k *logKvWrapper) Get(key string) (*kvdb.KVPair, error) {
	pair, err := k.wrappedKvdb.Get(key)
	k.logger.WithFields(logrus.Fields{
		opType:    "Get",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) Snapshot(prefixes []string, consistent bool) (kvdb.Kvdb, uint64, error) {
	kv, version, err := k.wrappedKvdb.Snapshot(prefixes, consistent)
	k.logger.WithFields(logrus.Fields{
		opType:    "Snapshot",
		errString: err,
	}).Info()
	return kv, version, err
}

func (k *logKvWrapper) Put(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	pair, err := k.wrappedKvdb.Put(key, value, ttl)
	k.logger.WithFields(logrus.Fields{
		opType:    "Put",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) GetVal(key string, v interface{}) (*kvdb.KVPair, error) {
	pair, err := k.wrappedKvdb.GetVal(key, v)
	k.logger.WithFields(logrus.Fields{
		opType:    "GetValue",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) Create(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	pair, err := k.wrappedKvdb.Create(key, value, ttl)
	k.logger.WithFields(logrus.Fields{
		opType:    "Create",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) Update(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	pair, err := k.wrappedKvdb.Update(key, value, ttl)
	k.logger.WithFields(logrus.Fields{
		opType:    "Update",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) Enumerate(prefix string) (kvdb.KVPairs, error) {
	pairs, err := k.wrappedKvdb.Enumerate(prefix)
	k.logger.WithFields(logrus.Fields{
		opType:    "Enumerate",
		"length":  len(pairs),
		errString: err,
	}).Info()
	return pairs, err
}

func (k *logKvWrapper) Delete(key string) (*kvdb.KVPair, error) {
	pair, err := k.wrappedKvdb.Delete(key)
	k.logger.WithFields(logrus.Fields{
		opType:    "Delete",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) DeleteTree(prefix string) error {
	err := k.wrappedKvdb.DeleteTree(prefix)
	k.logger.WithFields(logrus.Fields{
		opType:    "DeleteTree",
		errString: err,
	}).Info()
	return err
}

func (k *logKvWrapper) Keys(prefix, sep string) ([]string, error) {
	keys, err := k.wrappedKvdb.Keys(prefix, sep)
	k.logger.WithFields(logrus.Fields{
		opType:    "Keys",
		"length":  len(keys),
		errString: err,
	}).Info()
	return keys, err
}

func (k *logKvWrapper) CompareAndSet(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
	prevValue []byte,
) (*kvdb.KVPair, error) {
	pair, err := k.wrappedKvdb.CompareAndSet(kvp, flags, prevValue)
	k.logger.WithFields(logrus.Fields{
		opType:    "CompareAndSet",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) CompareAndDelete(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
) (*kvdb.KVPair, error) {
	pair, err := k.wrappedKvdb.CompareAndDelete(kvp, flags)
	k.logger.WithFields(logrus.Fields{
		opType:    "CompareAndDelete",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) WatchKey(
	key string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	err := k.wrappedKvdb.WatchKey(key, waitIndex, opaque, cb)
	k.logger.WithFields(logrus.Fields{
		opType:    "WatchKey",
		errString: err,
	}).Info()
	return err
}

func (k *logKvWrapper) WatchTree(
	prefix string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	err := k.wrappedKvdb.WatchTree(prefix, waitIndex, opaque, cb)
	k.logger.WithFields(logrus.Fields{
		opType:    "WatchTree",
		errString: err,
	}).Info()
	return err
}

func (k *logKvWrapper) Lock(key string) (*kvdb.KVPair, error) {
	pair, err := k.wrappedKvdb.Lock(key)
	k.logger.WithFields(logrus.Fields{
		opType:    "Lock",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) LockWithID(
	key string,
	lockerID string,
) (*kvdb.KVPair, error) {
	pair, err := k.wrappedKvdb.LockWithID(key, lockerID)
	k.logger.WithFields(logrus.Fields{
		opType:    "LockWithID",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) LockWithTimeout(
	key string,
	lockerID string,
	lockTryDuration time.Duration,
	lockHoldDuration time.Duration,
) (*kvdb.KVPair, error) {
	pair, err := k.wrappedKvdb.LockWithTimeout(key, lockerID, lockTryDuration, lockHoldDuration)
	k.logger.WithFields(logrus.Fields{
		opType:    "LockWithTimeout",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) Unlock(kvp *kvdb.KVPair) error {
	err := k.wrappedKvdb.Unlock(kvp)
	k.logger.WithFields(logrus.Fields{
		opType:    "Unlock",
		errString: err,
	}).Info()
	return err
}

func (k *logKvWrapper) EnumerateWithSelect(
	prefix string,
	enumerateSelect kvdb.EnumerateSelect,
	copySelect kvdb.CopySelect,
) ([]interface{}, error) {
	vals, err := k.wrappedKvdb.EnumerateWithSelect(prefix, enumerateSelect, copySelect)
	k.logger.WithFields(logrus.Fields{
		opType:    "EnumerateWithSelect",
		"length":  len(vals),
		errString: err,
	}).Info()
	return vals, err
}

func (k *logKvWrapper) GetWithCopy(
	key string,
	copySelect kvdb.CopySelect,
) (interface{}, error) {
	pair, err := k.wrappedKvdb.GetWithCopy(key, copySelect)
	k.logger.WithFields(logrus.Fields{
		opType:    "GetWithCopy",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) TxNew() (kvdb.Tx, error) {
	tx, err := k.wrappedKvdb.TxNew()
	k.logger.WithFields(logrus.Fields{
		opType:    "Snapshot",
		errString: err,
	}).Info()
	return tx, err
}

func (k *logKvWrapper) SnapPut(snapKvp *kvdb.KVPair) (*kvdb.KVPair, error) {
	pair, err := k.wrappedKvdb.SnapPut(snapKvp)
	k.logger.WithFields(logrus.Fields{
		opType:    "SnapPut",
		output:    pair,
		errString: err,
	}).Info()
	return pair, err
}

func (k *logKvWrapper) AddUser(username string, password string) error {
	err := k.wrappedKvdb.AddUser(username, password)
	k.logger.WithFields(logrus.Fields{
		opType:    "AddUser",
		errString: err,
	}).Info()
	return err
}

func (k *logKvWrapper) RemoveUser(username string) error {
	err := k.wrappedKvdb.RemoveUser(username)
	k.logger.WithFields(logrus.Fields{
		opType:    "RemoveUser",
		errString: err,
	}).Info()
	return err
}

func (k *logKvWrapper) GrantUserAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	err := k.wrappedKvdb.GrantUserAccess(username, permType, subtree)
	k.logger.WithFields(logrus.Fields{
		opType:    "GrantUserAccess",
		errString: err,
	}).Info()
	return err
}

func (k *logKvWrapper) RevokeUsersAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	err := k.wrappedKvdb.RevokeUsersAccess(username, permType, subtree)
	k.logger.WithFields(logrus.Fields{
		opType:    "RevokeUsersAccess",
		errString: err,
	}).Info()
	return err
}

func (k *logKvWrapper) SetFatalCb(f kvdb.FatalErrorCB) {
	k.wrappedKvdb.SetFatalCb(f)
}

func (k *logKvWrapper) SetLockTimeout(timeout time.Duration) {
	k.wrappedKvdb.SetLockTimeout(timeout)
}

func (k *logKvWrapper) GetLockTimeout() time.Duration {
	return k.wrappedKvdb.GetLockTimeout()
}

func (k *logKvWrapper) Serialize() ([]byte, error) {
	return k.wrappedKvdb.Serialize()
}

func (k *logKvWrapper) Deserialize(b []byte) (kvdb.KVPairs, error) {
	return k.wrappedKvdb.Deserialize(b)
}

func (k *logKvWrapper) AddMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	members, err := k.wrappedKvdb.AddMember(nodeIP, nodePeerPort, nodeName)
	k.logger.WithFields(logrus.Fields{
		opType:    "AddMember",
		output:    members,
		errString: err,
	}).Info()
	return members, err
}

func (k *logKvWrapper) RemoveMember(nodeName, nodeIP string) error {
	err := k.wrappedKvdb.RemoveMember(nodeName, nodeIP)
	k.logger.WithFields(logrus.Fields{
		opType:    "RemoveMember",
		errString: err,
	}).Info()
	return err
}

func (k *logKvWrapper) UpdateMember(nodeIP, nodePeerPort, nodeName string) (map[string][]string, error) {
	members, err := k.wrappedKvdb.UpdateMember(nodeIP, nodePeerPort, nodeName)
	k.logger.WithFields(logrus.Fields{
		opType:    "UpdateMember",
		output:    members,
		errString: err,
	}).Info()
	return members, err
}

func (k *logKvWrapper) ListMembers() (map[string]*kvdb.MemberInfo, error) {
	members, err := k.wrappedKvdb.ListMembers()
	k.logger.WithFields(logrus.Fields{
		opType:    "ListMembers",
		output:    members,
		errString: err,
	}).Info()
	return members, err
}

func (k *logKvWrapper) SetEndpoints(endpoints []string) error {
	err := k.wrappedKvdb.SetEndpoints(endpoints)
	k.logger.WithFields(logrus.Fields{
		opType:    "SetEndpoints",
		errString: err,
	}).Info()
	return err
}

func (k *logKvWrapper) GetEndpoints() []string {
	endpoints := k.wrappedKvdb.GetEndpoints()
	k.logger.WithFields(logrus.Fields{
		opType: "GetEndpoints",
		output: endpoints,
	}).Info()
	return endpoints
}

func (k *logKvWrapper) Defragment(endpoint string, timeout int) error {
	return k.wrappedKvdb.Defragment(endpoint, timeout)
}
