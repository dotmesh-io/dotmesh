package bolt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/memberlist"
	"github.com/portworx/kvdb"
	"github.com/portworx/kvdb/common"
	"github.com/sirupsen/logrus"
)

const (
	// Name is the name of this kvdb implementation.
	Name = "bolt-kv"
	// KvSnap is an option passed to designate this kvdb as a snap.
	KvSnap = "KvSnap"
	// KvUseInterface is an option passed that configures the mem to store
	// the values as interfaces instead of bytes. It will not create a
	// copy of the interface that is passed in. USE WITH CAUTION
	KvUseInterface = "KvUseInterface"
	// bootstrapkey is the name of the KV bootstrap key space.
	bootstrapKey = "bootstrap"
	// dbName is the name of the bolt database file.
	dbPath = "px.db"
)

var (
	// ErrSnap is returned if an operation is not supported on a snap.
	ErrSnap = errors.New("operation not supported on snap")
	// ErrSnapWithInterfaceNotSupported is returned when a snap kv-mem is
	// created with KvUseInterface flag on
	ErrSnapWithInterfaceNotSupported = errors.New("snap kvdb not supported with interfaces")
	// ErrIllegalSelect is returned when an incorrect select function
	// implementation is detected.
	ErrIllegalSelect = errors.New("Illegal Select implementation")
	// pxBucket is the name of the default PX keyspace in the internal KVDB.
	pxBucket = []byte("px")
)

func init() {
	logrus.Infof("Registering internal KVDB provider")
	if err := kvdb.Register(Name, New, Version); err != nil {
		panic(err.Error())
	}
}

type boltKV struct {
	common.BaseKvdb

	// db is the handle to the bolt DB.
	db *bolt.DB

	// locks is the map of currently held locks
	locks map[string]chan int

	// machines is a list of peers in the cluster
	machines []string

	// updates is the list of latest few updates
	dist WatchDistributor

	// mutex protects m, w, wt
	mutex sync.Mutex

	// index current kvdb index
	index uint64

	// domain scoping for this KVDB instance
	domain string

	kvdb.Controller
}

// watchUpdate refers to an update to this kvdb
type watchUpdate struct {
	// key is the key that was updated
	key string
	// kvp is the key-value that was updated
	kvp kvdb.KVPair
	// err is any error on update
	err error
}

// WatchUpdateQueue is a producer consumer queue.
type WatchUpdateQueue interface {
	// Enqueue will enqueue an update. It is non-blocking.
	Enqueue(update *watchUpdate)
	// Dequeue will either return an element from front of the queue or
	// will block until element becomes available
	Dequeue() *watchUpdate
}

// WatchDistributor distributes updates to the watchers
type WatchDistributor interface {
	// Add creates a new watch queue to send updates
	Add() WatchUpdateQueue
	// Remove removes an existing watch queue
	Remove(WatchUpdateQueue)
	// NewUpdate is invoked to distribute a new update
	NewUpdate(w *watchUpdate)
}

// distributor implements WatchDistributor interface
type distributor struct {
	sync.Mutex
	// updates is the list of latest few updates
	updates []*watchUpdate
	// watchers watch for updates
	watchers []WatchUpdateQueue
}

// NewWatchDistributor returns a new instance of
// the WatchDistrubtor interface
func NewWatchDistributor() WatchDistributor {
	return &distributor{}
}

func (d *distributor) Add() WatchUpdateQueue {
	d.Lock()
	defer d.Unlock()
	q := NewWatchUpdateQueue()
	for _, u := range d.updates {
		q.Enqueue(u)
	}
	d.watchers = append(d.watchers, q)
	return q
}

func (d *distributor) Remove(r WatchUpdateQueue) {
	d.Lock()
	defer d.Unlock()
	for i, q := range d.watchers {
		if q == r {
			copy(d.watchers[i:], d.watchers[i+1:])
			d.watchers[len(d.watchers)-1] = nil
			d.watchers = d.watchers[:len(d.watchers)-1]
		}
	}
}

func (d *distributor) NewUpdate(u *watchUpdate) {
	d.Lock()
	defer d.Unlock()
	// collect update
	d.updates = append(d.updates, u)
	if len(d.updates) > 100 {
		d.updates = d.updates[100:]
	}
	// send update to watchers
	for _, q := range d.watchers {
		q.Enqueue(u)
	}
}

// watchQueue implements WatchUpdateQueue interface for watchUpdates
type watchQueue struct {
	// updates is the list of updates
	updates []*watchUpdate
	// m is the mutex to protect updates
	m *sync.Mutex
	// cv is used to coordinate the producer-consumer threads
	cv *sync.Cond
}

// NewWatchUpdateQueue returns an instance of WatchUpdateQueue
func NewWatchUpdateQueue() WatchUpdateQueue {
	mtx := &sync.Mutex{}
	return &watchQueue{
		m:       mtx,
		cv:      sync.NewCond(mtx),
		updates: make([]*watchUpdate, 0)}
}

func (w *watchQueue) Dequeue() *watchUpdate {
	w.m.Lock()
	for {
		if len(w.updates) > 0 {
			update := w.updates[0]
			w.updates = w.updates[1:]
			w.m.Unlock()
			return update
		}
		w.cv.Wait()
	}
}

// Enqueue enqueues and never blocks
func (w *watchQueue) Enqueue(update *watchUpdate) {
	w.m.Lock()
	w.updates = append(w.updates, update)
	w.cv.Signal()
	w.m.Unlock()
}

type watchData struct {
	cb        kvdb.WatchCB
	opaque    interface{}
	waitIndex uint64
}

// New constructs a new kvdb.Kvdb.
func New(
	domain string,
	machines []string,
	options map[string]string,
	fatalErrorCb kvdb.FatalErrorCB,
) (kvdb.Kvdb, error) {
	if domain != "" && !strings.HasSuffix(domain, "/") {
		domain = domain + "/"
	}

	logrus.Infof("Initializing a new internal KVDB client with domain %v and pairs %v",
		domain,
		machines,
	)

	path := dbPath
	if p, ok := options[KvSnap]; ok {
		path = p
		logrus.Infof("Creating a new Bolt KVDB using snapshot path %v", path)
	} else {
		logrus.Infof("Creating a new Bolt KVDB using path %v", path)
	}

	handle, err := bolt.Open(
		path,
		0777,
		nil,
		// &bolt.Options{Timeout: 1 * time.Second},
	)
	if err != nil {
		logrus.Fatalf("Could not open internal KVDB: %v", err)
		return nil, err
	}

	tx, err := handle.Begin(true)
	if err != nil {
		logrus.Fatalf("Could not open KVDB transaction: %v", err)
		return nil, err
	}
	defer tx.Rollback()

	if _, err = tx.CreateBucketIfNotExists(pxBucket); err != nil {
		logrus.Fatalf("Could not create default KVDB bucket: %v", err)
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		logrus.Fatalf("Could not commit default KVDB bucket: %v", err)
		return nil, err
	}

	kv := &boltKV{
		BaseKvdb:   common.BaseKvdb{FatalCb: fatalErrorCb},
		db:         handle,
		dist:       NewWatchDistributor(),
		domain:     domain,
		Controller: kvdb.ControllerNotSupported,
		locks:      make(map[string]chan int),
	}

	return kv, nil
}

// Version returns the supported version of the mem implementation
func Version(url string, kvdbOptions map[string]string) (string, error) {
	return kvdb.BoltVersion1, nil
}

func (kv *boltKV) String() string {
	return Name
}

func (kv *boltKV) Capabilities() int {
	return kvdb.KVCapabilityOrderedUpdates
}

func (kv *boltKV) get(key string) (*kvdb.KVPair, error) {
	if kv.db == nil {
		return nil, kvdb.ErrNotFound
	}

	// XXX FIXME some bug above this cases the prefix to be pre-loaded.
	key = strings.TrimPrefix(key, kv.domain)
	key = kv.domain + key

	tx, err := kv.db.Begin(false)
	if err != nil {
		logrus.Fatalf("Could not open KVDB transaction in GET: %v", err)
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(pxBucket)

	if strings.HasPrefix(key, "pwx/test/pwx/test") {
		logrus.Panicf("Double pre")
	}
	val := bucket.Get([]byte(key))
	logrus.Warnf("XXX getting on %v = %v", key, string(val))
	if val == nil {
		return nil, kvdb.ErrNotFound
	}

	var kvp *kvdb.KVPair
	err = json.Unmarshal(val, &kvp)
	if err != nil {
		logrus.Warnf("Requested key could not be parsed from KVDB: %v, %v (%v)",
			key,
			val,
			err,
		)
		return nil, err
	}

	kv.normalize(kvp)

	return kvp, nil
}

func (kv *boltKV) put(
	key string,
	value interface{},
	ttl uint64,
	action kvdb.KVAction,
) (*kvdb.KVPair, error) {
	var (
		kvp *kvdb.KVPair
		b   []byte
		err error
	)

	if kv.db == nil {
		return nil, kvdb.ErrNotFound
	}

	// XXX FIXME some bug above this cases the prefix to be pre-loaded.
	key = strings.TrimPrefix(key, kv.domain)
	key = kv.domain + key

	tx, err := kv.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(pxBucket)
	if bucket == nil {
		logrus.Warnf("Requested bucket not found in internal KVDB: %v (%v)",
			pxBucket,
			err,
		)
		return nil, kvdb.ErrNotFound
	}

	// XXX FIXME is this going to work across restarts?
	index := atomic.AddUint64(&kv.index, 1)

	b, err = common.ToBytes(value)
	if err != nil {
		return nil, err
	}

	kvp = &kvdb.KVPair{
		Key:           key,
		Value:         b,
		TTL:           int64(ttl),
		KVDBIndex:     index,
		ModifiedIndex: index,
		CreatedIndex:  index,
		Action:        action,
	}

	kv.normalize(kvp)

	enc, err := json.Marshal(kvp)
	if err != nil {
		logrus.Warnf("Requested KVP cannot be marshalled into internal KVDB: %v (%v)",
			kvp,
			err,
		)
		return nil, err
	}

	logrus.Warnf("XXX putting on %v with %v %v", key, ttl, time.Duration(ttl))
	if err = bucket.Put([]byte(key), enc); err != nil {
		logrus.Warnf("Requested KVP could not be inserted into internal KVDB: %v (%v)",
			kvp,
			err,
		)
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		logrus.Fatalf("Could not commit put transaction in KVDB bucket for %v (%v): %v",
			key,
			enc,
			err,
		)
		return nil, err
	}

	kv.dist.NewUpdate(&watchUpdate{kvp.Key, *kvp, nil})

	// XXX FIXME - need to re-instate the timers after a crash
	if ttl != 0 {
		// time.AfterFunc(time.Second*time.Duration(ttl), func() {
		time.AfterFunc(time.Duration(ttl), func() {
			// TODO: handle error
			kv.mutex.Lock()
			defer kv.mutex.Unlock()
			logrus.Warnf("XXX TRIGGERING auto delete on %v %v", key, ttl)
			if _, err := kv.delete(key); err != nil {
				logrus.Warnf("Error while performing a timed DB delete on key %v: %v", key, err)
			}
		})
	}

	return kvp, nil
}

// enumerate returns a list of values and creates a copy if specified
func (kv *boltKV) enumerate(prefix string) (kvdb.KVPairs, error) {
	if kv.db == nil {
		return nil, kvdb.ErrNotFound
	}

	// XXX FIXME some bug above this cases the prefix to be pre-loaded.
	prefix = strings.TrimPrefix(prefix, kv.domain)
	prefix = kv.domain + prefix

	var kvps = make(kvdb.KVPairs, 0, 100)

	kv.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(pxBucket)
		if bucket == nil {
			logrus.Warnf("Requested bucket not found in internal KVDB: %v",
				pxBucket,
			)
			return kvdb.ErrNotFound
		}

		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if strings.HasPrefix(string(k), prefix) && !strings.Contains(string(k), "/_") {
				var kvp *kvdb.KVPair
				if err := json.Unmarshal(v, &kvp); err != nil {
					logrus.Warnf("Enumerated prefix could not be parsed from KVDB: %v, %v (%v)",
						prefix,
						v,
						err,
					)
					logrus.Fatalf("Could not enumerate internal KVDB: %v: %v",
						v,
						err,
					)
					return err
				}
				kv.normalize(kvp)
				kvps = append(kvps, kvp)
			}
		}
		return nil
	})

	return kvps, nil
}

func (kv *boltKV) delete(key string) (*kvdb.KVPair, error) {
	if kv.db == nil {
		return nil, kvdb.ErrNotFound
	}

	// XXX FIXME some bug above this cases the prefix to be pre-loaded.
	key = strings.TrimPrefix(key, kv.domain)
	key = kv.domain + key

	kvp, err := kv.get(key)
	if err != nil {
		return nil, err
	}
	kvp.KVDBIndex = atomic.AddUint64(&kv.index, 1)
	kvp.ModifiedIndex = kvp.KVDBIndex
	kvp.Action = kvdb.KVDelete

	tx, err := kv.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket := tx.Bucket(pxBucket)
	if bucket == nil {
		logrus.Warnf("Requested bucket for delete not found in internal KVDB: %v (%v)",
			pxBucket,
			err,
		)
		return nil, kvdb.ErrNotFound
	}

	logrus.Warnf("XXX deleting on %v", key)
	if err = bucket.Delete([]byte(key)); err != nil {
		logrus.Warnf("Requested KVP for delete could not be deleted from internal KVDB: %v (%v)",
			kvp,
			err,
		)
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		logrus.Fatalf("Could not commit delete transaction in KVDB bucket for %v: %v",
			key,
			err,
		)
		return nil, err
	}

	kv.dist.NewUpdate(&watchUpdate{kvp.Key, *kvp, nil})
	return kvp, nil
}

func (kv *boltKV) exists(key string) (*kvdb.KVPair, error) {
	return kv.get(key)
}

func (kv *boltKV) Get(key string) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	v, err := kv.get(key)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (kv *boltKV) snapDB() (string, error) {
	snapPath := dbPath + ".snap." + time.Now().String()

	from, err := os.Open(dbPath)
	if err != nil {
		logrus.Fatalf("Could not open bolt DB: %v", err)
		return "", err
	}
	defer from.Close()

	to, err := os.OpenFile(snapPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		logrus.Fatalf("Could not create bolt DB snap: %v", err)
		return "", err
	}
	defer to.Close()

	_, err = io.Copy(to, from)
	if err != nil {
		logrus.Fatalf("Could not copy bolt DB snap: %v", err)
		return "", err
	}

	return snapPath, nil
}

func (kv *boltKV) Snapshot(prefix string) (kvdb.Kvdb, uint64, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	_, err := kv.put(bootstrapKey, time.Now().UnixNano(), 0, kvdb.KVCreate)
	if err != nil {
		logrus.Fatalf("Could not create bootstrap key during snapshot: %v", err)
		return nil, 0, err
	}

	snapPath, err := kv.snapDB()
	if err != nil {
		logrus.Fatalf("Could not create DB snapshot: %v", err)
		return nil, 0, err
	}

	options := make(map[string]string)
	options[KvSnap] = snapPath

	snapKV, err := New(
		kv.domain,
		kv.machines,
		options,
		kv.FatalCb,
	)

	highestKvPair, _ := kv.delete(bootstrapKey)

	return snapKV, highestKvPair.ModifiedIndex, nil
}

func (kv *boltKV) Put(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	return kv.put(key, value, ttl, kvdb.KVSet)
}

func (kv *boltKV) GetVal(key string, v interface{}) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	kvp, err := kv.get(key)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(kvp.Value, v)
	return kvp, err
}

func (kv *boltKV) Create(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	result, err := kv.exists(key)
	if err != nil {
		return kv.put(key, value, ttl, kvdb.KVCreate)
	}
	return result, kvdb.ErrExist
}

// XXX needs to be atomic
func (kv *boltKV) Update(
	key string,
	value interface{},
	ttl uint64,
) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	if _, err := kv.exists(key); err != nil {
		return nil, kvdb.ErrNotFound
	}
	kvp, err := kv.put(key, value, ttl, kvdb.KVSet)
	if err != nil {
		return nil, err
	}

	kvp.Action = kvdb.KVSet
	return kvp, nil
}

func (kv *boltKV) Enumerate(prefix string) (kvdb.KVPairs, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	return kv.enumerate(prefix)
}

func (kv *boltKV) Delete(key string) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	return kv.delete(key)
}

func (kv *boltKV) DeleteTree(prefix string) error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	if len(prefix) > 0 && !strings.HasSuffix(prefix, kvdb.DefaultSeparator) {
		prefix += kvdb.DefaultSeparator
	}

	kvp, err := kv.enumerate(prefix)
	if err != nil {
		return err
	}
	for _, v := range kvp {
		// TODO: multiple errors
		if _, iErr := kv.delete(v.Key); iErr != nil {
			err = iErr
		}
	}
	return err
}

func (kv *boltKV) Keys(prefix, sep string) ([]string, error) {
	if "" == sep {
		sep = "/"
	}

	// XXX FIXME some bug above this cases the prefix to be pre-loaded.
	prefix = strings.TrimPrefix(prefix, kv.domain)
	prefix = kv.domain + prefix

	lenPrefix := len(prefix)
	lenSep := len(sep)
	if prefix[lenPrefix-lenSep:] != sep {
		prefix += sep
		lenPrefix += lenSep
	}
	seen := make(map[string]bool)
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	kv.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(pxBucket)
		if bucket == nil {
			logrus.Warnf("Requested bucket not found in internal KVDB: %v",
				pxBucket,
			)
			return kvdb.ErrNotFound
		}

		c := bucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if strings.HasPrefix(string(k), prefix) && !strings.Contains(string(k), "/_") {
				key := k[lenPrefix:]
				if idx := strings.Index(string(key), sep); idx > 0 {
					key = key[:idx]
				}
				seen[string(key)] = true
			}
		}
		return nil
	})

	retList := make([]string, len(seen))
	i := 0
	for k := range seen {
		retList[i] = strings.TrimPrefix(k, kv.domain)
		i++
	}

	return retList, nil
}

func (kv *boltKV) CompareAndSet(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
	prevValue []byte,
) (*kvdb.KVPair, error) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	logrus.Infof("XXX CompareAndSet %v", kvp)

	result, err := kv.exists(kvp.Key)
	if err != nil {
		return nil, err
	}
	if prevValue != nil {
		if !bytes.Equal(result.Value, prevValue) {
			return nil, kvdb.ErrValueMismatch
		}
	}
	if flags == kvdb.KVModifiedIndex {
		if kvp.ModifiedIndex != result.ModifiedIndex {
			return nil, kvdb.ErrValueMismatch
		}
	}
	return kv.put(kvp.Key, kvp.Value, 0, kvdb.KVSet)
}

func (kv *boltKV) CompareAndDelete(
	kvp *kvdb.KVPair,
	flags kvdb.KVFlags,
) (*kvdb.KVPair, error) {
	logrus.Infof("XXX CompareAndDelete %v", kvp)

	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	// XXX FIXME this needs to be atomic cluster wide

	logrus.Warnf("XXX Checking %v", kvp.Key)
	result, err := kv.exists(kvp.Key)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(result.Value, kvp.Value) {
		logrus.Warnf("CompareAndDelete failed because of value mismatch %v != %v",
			result.Value, kvp.Value)
		return nil, kvdb.ErrNotFound
	}
	if kvp.ModifiedIndex != result.ModifiedIndex {
		logrus.Warnf("CompareAndDelete failed because of modified index mismatch %v != %v",
			result.ModifiedIndex, kvp.ModifiedIndex)
		return nil, kvdb.ErrNotFound
	}
	return kv.delete(kvp.Key)
}

func (kv *boltKV) WatchKey(
	key string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	go kv.watchCb(
		kv.dist.Add(),
		key,
		&watchData{
			cb:        cb,
			waitIndex: waitIndex,
			opaque:    opaque,
		},
		false,
	)

	return nil
}

func (kv *boltKV) WatchTree(
	prefix string,
	waitIndex uint64,
	opaque interface{},
	cb kvdb.WatchCB,
) error {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()

	// XXX FIXME - some top level code has a bug and sends the prefix preloaded
	prefix = strings.TrimPrefix(prefix, kv.domain)

	go kv.watchCb(
		kv.dist.Add(),
		prefix,
		&watchData{
			cb:        cb,
			waitIndex: waitIndex,
			opaque:    opaque,
		},
		true,
	)

	return nil
}

func (kv *boltKV) Lock(key string) (*kvdb.KVPair, error) {
	return kv.LockWithID(key, "locked")
}

func (kv *boltKV) LockWithID(
	key string,
	lockerID string,
) (*kvdb.KVPair, error) {
	return kv.LockWithTimeout(key, lockerID, kvdb.DefaultLockTryDuration, kv.GetLockTimeout())
}

func (kv *boltKV) LockWithTimeout(
	key string,
	lockerID string,
	lockTryDuration time.Duration,
	lockHoldDuration time.Duration,
) (*kvdb.KVPair, error) {
	logrus.Infof("XXX Lock %v %v %v", key, lockTryDuration, lockHoldDuration)

	duration := time.Second

	// XXX FIXME - if we crash, we need to cleanup this lock.
	result, err := kv.Create(key, lockerID, uint64(lockHoldDuration))
	startTime := time.Now()
	for count := 0; err != nil; count++ {
		time.Sleep(duration)
		result, err = kv.Create(key, lockerID, uint64(lockHoldDuration))
		if err != nil && count > 0 && count%15 == 0 {
			var currLockerID string
			if _, errGet := kv.GetVal(key, currLockerID); errGet == nil {
				logrus.Infof("Lock %v locked for %v seconds, tag: %v",
					key, count, currLockerID)
			}
		}

		if err != nil && time.Since(startTime) > lockTryDuration {
			logrus.Warnf("Timeout waiting for lock on %v: count=%v err=%v", key, count, err)
			return nil, err
		}
	}

	lockChan := make(chan int)
	kv.mutex.Lock()
	logrus.Warnf("XXX Locked %v", key)
	kv.locks[key] = lockChan
	kv.mutex.Unlock()
	if lockHoldDuration > 0 {
		go func() {
			timeout := time.After(lockHoldDuration)
			for {
				select {
				case <-timeout:
					logrus.Warnf("XXX LOCK timeout on %v after %v", key, lockHoldDuration)
					kv.LockTimedout(key)
				case <-lockChan:
					logrus.Warnf("XXX LOCK chan wakeup on %v", key)
					return
				}
			}
		}()
	}

	return result, err
}

func (kv *boltKV) Unlock(kvp *kvdb.KVPair) error {
	logrus.Warnf("XXX Unlocking %v", kvp)
	if kvp == nil {
		logrus.Panicf("Unlock on a nil kvp")
	}
	kv.mutex.Lock()
	lockChan, ok := kv.locks[kvp.Key]
	logrus.Warnf("XXX Unlock chan %v on %v", lockChan, kvp.Key)
	if ok {
		delete(kv.locks, kvp.Key)
	}
	kv.mutex.Unlock()
	if lockChan != nil {
		logrus.Warnf("XXX Waking up chan on %v", kvp.Key)
		close(lockChan)
	}

	_, err := kv.CompareAndDelete(kvp, kvdb.KVFlags(0))

	return err
}

func (kv *boltKV) EnumerateWithSelect(
	prefix string,
	enumerateSelect kvdb.EnumerateSelect,
	copySelect kvdb.CopySelect,
) ([]interface{}, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *boltKV) GetWithCopy(
	key string,
	copySelect kvdb.CopySelect,
) (interface{}, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *boltKV) TxNew() (kvdb.Tx, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *boltKV) normalize(kvp *kvdb.KVPair) {
	kvp.Key = strings.TrimPrefix(kvp.Key, kv.domain)
}

func copyWatchKeys(w map[string]*watchData) []string {
	keys := make([]string, len(w))
	i := 0
	for key := range w {
		keys[i] = key
		i++
	}
	return keys
}

func (kv *boltKV) watchCb(
	q WatchUpdateQueue,
	prefix string,
	v *watchData,
	treeWatch bool,
) {
	for {
		logrus.Warnf("XXX watchCb on %v", prefix)
		update := q.Dequeue()
		logrus.Warnf("XXX watchCb compare on %v %v %v %v %v",
			treeWatch, update.key, prefix, v.waitIndex, update.kvp.ModifiedIndex)
		if ((treeWatch && strings.HasPrefix(update.key, prefix)) ||
			(!treeWatch && update.key == prefix)) &&
			(v.waitIndex == 0 || v.waitIndex < update.kvp.ModifiedIndex) {
			logrus.Warnf("XXX watchCb FIRED on %v", prefix)
			err := v.cb(update.key, v.opaque, &update.kvp, update.err)
			if err != nil {
				_ = v.cb("", v.opaque, nil, kvdb.ErrWatchStopped)
				kv.dist.Remove(q)
				return
			}
		}
	}
}

func (kv *boltKV) SnapPut(snapKvp *kvdb.KVPair) (*kvdb.KVPair, error) {
	return nil, kvdb.ErrNotSupported
}

func (kv *boltKV) AddUser(username string, password string) error {
	return kvdb.ErrNotSupported
}

func (kv *boltKV) RemoveUser(username string) error {
	return kvdb.ErrNotSupported
}

func (kv *boltKV) GrantUserAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	return kvdb.ErrNotSupported
}

func (kv *boltKV) RevokeUsersAccess(
	username string,
	permType kvdb.PermissionType,
	subtree string,
) error {
	return kvdb.ErrNotSupported
}

func (kv *boltKV) Serialize() ([]byte, error) {

	kvps, err := kv.Enumerate("")
	if err != nil {
		return nil, err
	}
	return kv.SerializeAll(kvps)
}

func (kv *boltKV) Deserialize(b []byte) (kvdb.KVPairs, error) {
	return kv.DeserializeAll(b)
}

// MemberList based Bolt implementation
var (
	mtx        sync.RWMutex
	items      = map[string]string{}
	broadcasts *memberlist.TransmitLimitedQueue
)

type broadcast struct {
	msg    []byte
	notify chan<- struct{}
}

type delegate struct{}

type update struct {
	Action string // add, del
	Data   map[string]string
}

func (b *broadcast) Invalidates(other memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {
	if b.notify != nil {
		close(b.notify)
	}
}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

func (d *delegate) NotifyMsg(b []byte) {
	if len(b) == 0 {
		return
	}

	switch b[0] {
	case 'd': // data
		var updates []*update
		if err := json.Unmarshal(b[1:], &updates); err != nil {
			return
		}
		mtx.Lock()
		for _, u := range updates {
			for k, v := range u.Data {
				switch u.Action {
				case "add":
					items[k] = v
				case "del":
					delete(items, k)
				}
			}
		}
		mtx.Unlock()
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return broadcasts.GetBroadcasts(overhead, limit)
}

func (d *delegate) LocalState(join bool) []byte {
	mtx.RLock()
	m := items
	mtx.RUnlock()
	b, _ := json.Marshal(m)
	return b
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}
	if !join {
		return
	}
	var m map[string]string
	if err := json.Unmarshal(buf, &m); err != nil {
		return
	}
	mtx.Lock()
	for k, v := range m {
		items[k] = v
	}
	mtx.Unlock()
}

func mlPut(key string, val string) error {
	mtx.Lock()
	defer mtx.Unlock()

	b, err := json.Marshal([]*update{
		{
			Action: "add",
			Data: map[string]string{
				key: val,
			},
		},
	})

	if err != nil {
		return err
	}

	items[key] = val

	broadcasts.QueueBroadcast(&broadcast{
		msg:    append([]byte("d"), b...),
		notify: nil,
	})

	return nil
}

func mlDel(key string) error {
	mtx.Lock()
	defer mtx.Unlock()

	b, err := json.Marshal([]*update{
		{
			Action: "del",
			Data: map[string]string{
				key: "",
			},
		},
	})

	if err != nil {
		return err
	}

	delete(items, key)

	broadcasts.QueueBroadcast(&broadcast{
		msg:    append([]byte("d"), b...),
		notify: nil,
	})

	return nil
}

func mlGet(key string) (error, []byte) {
	mtx.Lock()
	defer mtx.Unlock()

	val := items[key]
	return nil, []byte(val)
}

func mlStart() error {
	hostname, _ := os.Hostname()
	c := memberlist.DefaultLocalConfig()
	c.Delegate = &delegate{}
	c.BindPort = 0
	c.Name = hostname + "-" + "UUIDXXX"
	m, err := memberlist.Create(c)
	if err != nil {
		return err
	}

	// XXX TODO
	members := []string{"127.0.0.1"}

	if len(members) > 0 {
		if members, err := m.Join(members); err != nil {
			return err
		} else {
			logrus.Infof("Internal KVDB joining members: %v", members)
		}
	}

	broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return m.NumMembers()
		},
		RetransmitMult: 3,
	}
	node := m.LocalNode()
	fmt.Printf("Local member %s:%d\n", node.Addr, node.Port)
	return nil
}
