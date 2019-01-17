package test

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/portworx/kvdb"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type watchData struct {
	t            *testing.T
	key          string
	otherKey     string
	stop         string
	localIndex   uint64
	updateIndex  uint64
	kvp          *kvdb.KVPair
	watchStopped bool
	iterations   int
	action       kvdb.KVAction
	writer       int32
	reader       int32
	whichKey     int32
}

type lockTag struct {
	nodeID   string
	funcName string
}

func fatalErrorCb() kvdb.FatalErrorCB {
	return func(format string, args ...interface{}) {
		logrus.Panicf(format, args)
	}
}

// StartKvdb is a func literal.
type StartKvdb func(bool) error

// StopKvdb is a func literal.
type StopKvdb func() error

// Run runs the test suite.
func Run(datastoreInit kvdb.DatastoreInit, t *testing.T, start StartKvdb, stop StopKvdb) {
	err := start(true)
	assert.NoError(t, err, "Unable to start kvdb")
	// Wait for kvdb to start
	time.Sleep(5 * time.Second)
	kv, err := datastoreInit("pwx/test", nil, nil, fatalErrorCb())
	if err != nil {
		t.Fatalf(err.Error())
	}
	create(kv, t)
	createWithTTL(kv, t)
	cas(kv, t)
	cad(kv, t)
	snapshot(kv, t)
	get(kv, t)
	getInterface(kv, t)
	update(kv, t)
	deleteKey(kv, t)
	deleteTree(kv, t)
	enumerate(kv, t)
	keys(kv, t)
	concurrentEnum(kv, t)
	watchKey(kv, t)
	watchTree(kv, t)
	watchWithIndex(kv, t)
	collect(kv, t)
	lockBasic(kv, t)
	lock(kv, t)
	lockBetweenRestarts(kv, t, start, stop)
	serialization(kv, t)
	err = stop()
	assert.NoError(t, err, "Unable to stop kvdb")
}

// RunLock runs the lock test suite.
func RunLock(datastoreInit kvdb.DatastoreInit, t *testing.T, start StartKvdb, stop StopKvdb) {
	err := start(true)
	time.Sleep(3 * time.Second)
	assert.NoError(t, err, "Unable to start kvdb")
	kv, err := datastoreInit("pwx/test", nil, nil, fatalErrorCb())
	if err != nil {
		t.Fatalf(err.Error())
	}

	lockBasic(kv, t)
	lock(kv, t)
	lockBetweenRestarts(kv, t, start, stop)

	err = stop()
	assert.NoError(t, err, "Unable to stop kvdb")
}

// RunWatch runs the watch test suite.
func RunWatch(datastoreInit kvdb.DatastoreInit, t *testing.T, start StartKvdb, stop StopKvdb) {
	err := start(true)
	time.Sleep(3 * time.Second)
	assert.NoError(t, err, "Unable to start kvdb")
	kv, err := datastoreInit("pwx/test", nil, nil, fatalErrorCb())
	if err != nil {
		t.Fatalf(err.Error())
	}

	// watchKey(kv, t)
	// watchTree(kv, t)
	// watchWithIndex(kv, t)
	collect(kv, t)
	// serialization(kv, t)
	// concurrentEnum(kv, t)

	err = stop()
	assert.NoError(t, err, "Unable to stop kvdb")
}

// RunAuth runs the authentication test suite for kvdb
func RunAuth(datastoreInit kvdb.DatastoreInit, t *testing.T) {
	options := make(map[string]string)
	// In order to run these tests, either configure your local etcd
	// to use these options (username/password/ca-file) or modify them here.
	options[kvdb.UsernameKey] = "root"
	kv, err := datastoreInit("pwx/test", nil, options, fatalErrorCb())
	if err == nil {
		t.Fatalf("Expected an error when no password provided")
	}
	options[kvdb.PasswordKey] = "test123"
	kv, err = datastoreInit("pwx/test", nil, options, fatalErrorCb())
	if err == nil {
		t.Fatalf("Expected an error when no certificate provided")
	}
	options[kvdb.CAFileKey] = "/etc/pwx/pwx-ca.crt"
	options[kvdb.CertFileKey] = "/etc/pwx/pwx-user-cert.crt"
	options[kvdb.CertKeyFileKey] = "/etc/pwx/pwx-user-key.key"

	machines := []string{"https://192.168.56.101:2379"}
	fmt.Println("Last one")
	kv, err = datastoreInit("pwx/test", machines, options, fatalErrorCb())
	if err != nil {
		t.Fatalf(err.Error())
	}
	if kv == nil {
		t.Fatalf("Expected kv to be not nil")
	}
	// Run the basic put get update tests using auth
	get(kv, t)
	create(kv, t)
	update(kv, t)
	// Run the auth tests
	addUser(kv, t)
	grantRevokeUser(kv, datastoreInit, t)
	removeUser(kv, t)
}

func get(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("get")

	kvPair, err := kv.Get("DEADCAFE")
	assert.Error(t, err, "Expecting error value for non-existent value")

	key := "foo/docker"
	val := "great"
	defer func() {
		kv.Delete(key)
	}()

	kvPair, err = kv.Put(key, []byte(val), 0)
	assert.NoError(t, err, "Unexpected error in Put")

	kvPair, err = kv.Get(key)
	assert.NoError(t, err, "Failed in Get")

	assert.Equal(t, key, kvPair.Key, "Key mismatch in Get")
	assert.Equal(t, string(kvPair.Value), val, "value mismatch in Get")
}

func getInterface(kv kvdb.Kvdb, t *testing.T) {

	fmt.Println("getInterface")
	expected := struct {
		N int
		S string
	}{
		N: 10,
		S: "Ten",
	}

	actual := expected
	actual.N = 0
	actual.S = "zero"

	key := "DEADBEEF"
	_, err := kv.Delete(key)
	_, err = kv.Put(key, &expected, 0)
	assert.NoError(t, err, "Failed in Put")

	_, err = kv.GetVal(key, &actual)
	assert.NoError(t, err, "Failed in Get")

	assert.Equal(t, expected, actual, "Expected %#v but got %#v",
		expected, actual)
}

func create(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("create")

	key := "///create/foo"
	kv.Delete(key)

	kvp, err := kv.Create(key, []byte("bar"), 0)
	require.NoError(t, err, "Error on create")

	defer func() {
		kv.Delete(key)
	}()
	assert.Equal(t, kvp.Action, kvdb.KVCreate,
		"Expected action KVCreate, actual %v", kvp.Action)

	_, err = kv.Create(key, []byte("bar"), 0)
	assert.True(t, err == kvdb.ErrExist,
		"Create on existing key should have errored.")
}

func createWithTTL(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("create with ttl")
	key := "create/foottl"
	kv.Delete(key)
	assert.NotNil(t, kv, "Default KVDB is not set")
	_, err := kv.Create(key, []byte("bar"), 6)
	if err != nil {
		// Consul does not support ttl less than 10
		assert.EqualError(t, err, kvdb.ErrTTLNotSupported.Error(), "ttl not supported")
		_, err := kv.Create(key, []byte("bar"), 20)
		assert.NoError(t, err, "Error on create")
		// Consul doubles the ttl value
		time.Sleep(time.Second * 21)
		_, err = kv.Get(key)
		assert.Error(t, err, "Expecting error value for expired value")

	} else {
		assert.NoError(t, err, "Error on create")
		time.Sleep(time.Second * 7)
		_, err = kv.Get(key)
		assert.Error(t, err, "Expecting error value for expired value")
	}
}

func update(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("update")

	key := "update/foo"
	kv.Delete(key)

	kvp, err := kv.Update(key, []byte("bar"), 0)
	assert.Error(t, err, "Update should error on non-existent key")

	defer func() {
		kv.Delete(key)
	}()

	kvp, err = kv.Create(key, []byte("bar"), 0)
	assert.NoError(t, err, "Unexpected error on create")

	kvp, err = kv.Update(key, []byte("bar"), 0)
	assert.NoError(t, err, "Unexpected error on update")

	assert.Equal(t, kvp.Action, kvdb.KVSet,
		"Expected action KVSet, actual %v", kvp.Action)
}

func deleteKey(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("deleteKey")

	key := "delete_key"
	_, err := kv.Delete(key)

	_, err = kv.Put(key, []byte("delete_me"), 0)
	assert.NoError(t, err, "Unexpected error on Put")

	_, err = kv.Get(key)
	assert.NoError(t, err, "Unexpected error on Get")

	_, err = kv.Delete(key)
	assert.NoError(t, err, "Unexpected error on Delete")

	_, err = kv.Get(key)
	assert.Error(t, err, "Get should fail on deleted key")

	_, err = kv.Delete(key)
	assert.Error(t, err, "Delete should fail on non existent key")
	assert.EqualError(t, err, kvdb.ErrNotFound.Error(), "Invalid error returned : %v", err)
}

func deleteTree(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("deleteTree")

	prefix := "tree"
	keys := map[string]string{
		prefix + "/1cbc9a98-072a-4793-8608-01ab43db96c8": "bar",
		prefix + "/foo":                                  "baz",
	}

	for key, val := range keys {
		_, err := kv.Put(key, []byte(val), 0)
		assert.NoError(t, err, "Unexpected error on Put")
	}

	keyWithSamePrefix := prefix + "_some"
	_, err := kv.Put(keyWithSamePrefix, []byte("val"), 0)
	assert.NoError(t, err, "Unexpected error on Put")

	_, err = kv.Get(keyWithSamePrefix)
	assert.NoError(t, err, "Unexpected error on Get")

	for key := range keys {
		_, err := kv.Get(key)
		assert.NoError(t, err, "Unexpected error on Get")
	}
	err = kv.DeleteTree(prefix)
	assert.NoError(t, err, "Unexpected error on DeleteTree")

	for key := range keys {
		_, err := kv.Get(key)
		assert.Error(t, err, "Get should fail on all keys after DeleteTree")
	}
	_, err = kv.Get(keyWithSamePrefix)
	assert.NoError(t, err, "Unexpected error on Get")
}

func enumerate(kv kvdb.Kvdb, t *testing.T) {

	fmt.Println("enumerate")

	prefix := "enumerate"
	keys := map[string]string{
		prefix + "/1cbc9a98-072a-4793-8608-01ab43db96c8": "bar",
		prefix + "/foo":                                  "baz",
	}

	kv.DeleteTree(prefix)
	defer func() {
		kv.DeleteTree(prefix)
	}()

	errPairs, err := kv.Enumerate(prefix)
	assert.Equal(t, 0, len(errPairs), "Expected 0 pairs")

	folderKey := prefix + "/folder/"
	_, err = kv.Put(folderKey, []byte(""), 0)
	assert.NoError(t, err, "Unexpected error on Put")
	kvPairs, err := kv.Enumerate(folderKey)
	assert.Equal(t, nil, err, "Unexpected error on Enumerate")
	kv.DeleteTree(prefix)

	for key, val := range keys {
		_, err := kv.Put(key, []byte(val), 0)
		assert.NoError(t, err, "Unexpected error on Put")
	}
	kvPairs, err = kv.Enumerate(prefix)
	assert.NoError(t, err, "Unexpected error on Enumerate")

	assert.Equal(t, len(kvPairs), len(keys),
		"Expecting %d keys under %s got: %d",
		len(keys), prefix, len(kvPairs))

	for i := range kvPairs {
		v, ok := keys[kvPairs[i].Key]
		assert.True(t, ok, "unexpected kvpair (%s)->(%s)",
			kvPairs[i].Key, kvPairs[i].Value)
		assert.Equal(t, v, string(kvPairs[i].Value),
			"Invalid kvpair (%s)->(%s) expect value %s",
			kvPairs[i].Key, kvPairs[i].Value, v)
	}
}

func keys(kv kvdb.Kvdb, t *testing.T) {

	fmt.Println("keys")

	prefix := "keys"
	testKeys := []string{"testkey1", "testkey2", "testkey99999999999999999"}
	testRows := map[string]string{
		prefix + "/" + testKeys[0]:                    "bar",
		prefix + "/" + testKeys[1] + "/testkey3":      "baz",
		prefix + "/" + testKeys[2] + "/here/or/there": "foo",
	}

	kv.DeleteTree(prefix)
	defer func() {
		kv.DeleteTree(prefix)
	}()

	keys, err := kv.Keys(prefix, "")
	assert.Equal(t, 0, len(keys), "Expected 0 keys")

	for key, val := range testRows {
		_, err := kv.Put(key, []byte(val), 0)
		assert.NoError(t, err, "Unexpected error on Put")
	}

	keys, err = kv.Keys(prefix, "")
	assert.NoError(t, err, "Unexpected error on Keys")

	assert.Equal(t, len(testRows), len(keys),
		"Expecting %d keys under %s got: %d",
		len(testRows), prefix, len(keys))

	sort.Strings(keys)
	sort.Strings(testKeys)
	assert.Equal(t, testKeys, keys,
		"Expecting array %v to be equal to %v",
		testKeys, keys)
}

func concurrentEnum(kv kvdb.Kvdb, t *testing.T) {

	fmt.Println("concurrentEnum")

	prefix := "concEnum"
	var wg sync.WaitGroup
	quit, latch := make(chan bool), make(chan bool)
	shared, numGoroutines := int32(0), 0

	kv.DeleteTree(prefix)
	defer func() {
		kv.DeleteTree(prefix)
	}()

	// start 3 "adders"
	for i := 0; i < 3; i++ {
		go func() {
			id := atomic.AddInt32(&shared, 1)
			fmt.Printf("+> Adder #%d started\n", id)
			content := []byte(fmt.Sprintf("adder #%d", id))
			wg.Add(1)
			defer wg.Done()
			_ = <-latch // sync-point
			numFails := 0
			for j := 0; ; j++ {
				select {
				case <-quit:
					fmt.Printf("+> Adder #%d quit (stored %d keys)\n", id, j)
					return
				default:
					key := fmt.Sprintf("%s/%d-%d", prefix, id, j%100000)
					_, err := kv.Put(key, content, 0)
					if err != nil {
						logrus.WithError(err).Warnf("Error inserting key $s", key)
						numFails++
						// let's tolerate some errors, since we're in race w/ Deleter
						assert.True(t, numFails < 10, "Too many failures on PUT")
					}
					// sleep a bit, not to be overly aggressive
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				}
			}
		}()
		numGoroutines++
	}

	// start 1 "deleter"
	go func() {
		wg.Add(1)
		defer wg.Done()
		fmt.Printf("+> Deleter started\n")
		_ = <-latch     // sync-point
		for j := 0; ; { // cap at max 100k
			select {
			case <-quit:
				fmt.Printf("+> Deleter quit (deleted %d keys)\n", j)
				return
			default:
				key := fmt.Sprintf("%s/%d-%d", prefix, rand.Intn(numGoroutines), j%100000)
				_, err := kv.Delete(key)
				if err == nil {
					// advance only on successful delete
					j++
				}
				// sleep a bit, not to be overly aggressive
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			}
		}
	}()
	numGoroutines++

	fmt.Printf("> MAIN waiting for workers\n")
	time.Sleep(50 * time.Millisecond)
	close(latch) // release sync-points
	time.Sleep(5500 * time.Millisecond)

	// make sure these two just work, otherwise we cannot assume how many elements found

	sep := ':'
	fmt.Printf("> MAIN run")
	for i := 0; i < 5; i++ {
		fmt.Printf("%c Enumerate", sep)
		_, err := kv.Enumerate(prefix)
		assert.NoError(t, err)

		sep = ','
		fmt.Printf("%c Keys", sep)
		_, err = kv.Keys(prefix, "")
		assert.NoError(t, err)
	}

	fmt.Printf("%c stop workers\n", sep)
	for i := 0; i < numGoroutines; i++ {
		quit <- true
	}
	close(quit)
	fmt.Printf("> MAIN waiting ...")
	wg.Wait()
	fmt.Printf("DONE.\n")
}

func snapshot(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("snapshot")
	prefix := "snapshot/"
	kv.DeleteTree(prefix)
	defer kv.DeleteTree(prefix)

	preSnapData := make(map[string]string)
	preSnapDataVersion := make(map[string]uint64)
	inputData := make(map[string]string)
	inputDataVersion := make(map[string]uint64)
	deleteKeys := make(map[string]string)

	key := "key"
	value := "bar"
	keyd := "key-delete"
	valued := "bar-delete"
	count := 100
	doneUpdate := make(chan bool, 2)
	emptyKeyName := ""
	updateFn := func(count int, v string, dataMap map[string]string,
		versionMap map[string]uint64, isDelete bool) {
		for i := 0; i < count; i++ {
			suffix := strconv.Itoa(i)
			inputKey := prefix + key + suffix
			inputValue := v
			if i == 0 {
				emptyKeyName = inputKey
				inputValue = ""
			}
			kvp, err := kv.Put(inputKey, []byte(inputValue), 0)
			assert.NoError(t, err, "Unexpected error on Put")
			dataMap[inputKey] = inputValue
			versionMap[inputKey] = kvp.ModifiedIndex
			deleteKey := prefix + keyd + suffix
			if !isDelete {
				_, err := kv.Put(deleteKey, []byte(valued), 0)
				assert.NoError(t, err, "Unexpected error on Put")
				deleteKeys[deleteKey] = deleteKey
			} else {
				_, err := kv.Delete(deleteKey)
				assert.NoError(t, err, "Unexpected error on Delete")
			}

		}
		doneUpdate <- true
	}

	updateFn(count, value, preSnapData, preSnapDataVersion, false)
	<-doneUpdate
	newValue := "bar2"
	go updateFn(count, newValue, inputData, inputDataVersion, true)
	time.Sleep(20 * time.Millisecond)

	snap, snapVersion, err := kv.Snapshot(prefix)
	assert.NoError(t, err, "Unexpected error on Snapshot")
	<-doneUpdate

	kvPairs, err := snap.Enumerate(prefix)
	assert.NoError(t, err, "Unexpected error on Enumerate")

	processedKeys := 0
	for i := range kvPairs {
		if strings.Contains(kvPairs[i].Key, keyd) {
			_, ok := deleteKeys[kvPairs[i].Key]
			assert.True(t, ok, "Key not found in deleteKeys list (%v)", kvPairs[i].Key)
			delete(deleteKeys, kvPairs[i].Key)
			continue
		}
		processedKeys++
		currValue, ok1 := inputData[kvPairs[i].Key]
		currVersion, ok2 := inputDataVersion[kvPairs[i].Key]
		preSnapValue, ok3 := preSnapData[kvPairs[i].Key]
		preSnapKeyVersion, ok4 := preSnapDataVersion[kvPairs[i].Key]
		assert.True(t, ok1 && ok2 && ok3 && ok4, "unexpected kvpair (%s)->(%s)",
			kvPairs[i].Key, kvPairs[i].Value)

		assert.True(t, kvPairs[i].Key == emptyKeyName ||
			len(string(kvPairs[i].Value)) != 0,
			"Empty value for key %f", kvPairs[i].Key)

		assert.True(t, kvPairs[i].ModifiedIndex < snapVersion,
			"snap db key (%v) has version greater than snap version (%v)",
			kvPairs[i].ModifiedIndex, snapVersion)

		if kvPairs[i].ModifiedIndex == currVersion {
			assert.True(t, string(kvPairs[i].Value) == currValue,
				"snapshot db does not have correct value, "+
					" expected %v got %v", currValue, string(kvPairs[i].Value))
		} else {
			assert.True(t, kvPairs[i].ModifiedIndex == preSnapKeyVersion,
				"snapshot db does not have correct version, expected %v got %v",
				kvPairs[i].ModifiedIndex, preSnapKeyVersion)
			assert.True(t, string(kvPairs[i].Value) == preSnapValue,
				"snapshot db does not have correct value, "+
					" expected %v got %v", preSnapValue, string(kvPairs[i].Value))
		}
	}
	assert.Equal(t, count, processedKeys,
		"Expecting %d keys under %s got: %d, kv: %v",
		processedKeys, prefix, len(kvPairs), kvPairs)

}

func getLockMethods(kv kvdb.Kvdb) []func(string) (*kvdb.KVPair, error) {
	lockMethods := make([]func(string) (*kvdb.KVPair, error), 2)
	lockMethods[0] = func(key string) (*kvdb.KVPair, error) {
		return kv.Lock(key)
	}
	lockMethods[1] = func(key string) (*kvdb.KVPair, error) {
		tag := "node:node_1,func:testfunc"
		return kv.LockWithID(key, tag)
	}
	return lockMethods
}

func lock(kv kvdb.Kvdb, t *testing.T) {
	lockMethods := getLockMethods(kv)

	for _, lockMethod := range lockMethods {
		kv.SetLockTimeout(time.Duration(0))
		fmt.Println("lock")

		key := "locktest"
		kvPair, err := lockMethod(key)
		assert.NoError(t, err, "Unexpected error in lock")

		if kvPair == nil {
			return
		}

		// For consul unlock does not deal with the value part of the kvPair
		/*
			stash := *kvPair
			stash.Value = []byte("hoohah")
			fmt.Println("bad unlock")
			err = kv.Unlock(&stash)
			assert.Error(t, err, "Unlock should fail for bad KVPair")
		*/

		fmt.Println("unlock")
		err = kv.Unlock(kvPair)
		assert.NoError(t, err, "Unexpected error from Unlock")

		fmt.Println("relock")
		kvPair, err = lockMethod(key)
		assert.NoError(t, err, "Failed to lock after unlock")

		fmt.Println("reunlock")
		err = kv.Unlock(kvPair)
		assert.NoError(t, err, "Unexpected error from Unlock")

		fmt.Println("repeat lock once")
		kvPair, err = lockMethod(key)
		assert.NoError(t, err, "Failed to lock unlock")

		done := 0
		go func() {
			time.Sleep(time.Second * 10)
			done = 1
			err = kv.Unlock(kvPair)
			fmt.Println("repeat lock unlock once")
			assert.NoError(t, err, "Unexpected error from Unlock")
		}()
		fmt.Println("repeat lock lock twice")
		kvPair, err = lockMethod(key)
		assert.NoError(t, err, "Failed to lock")
		assert.Equal(t, done, 1, "Locked before unlock")
		if done != 1 {
			logrus.Fatalf("Exiting because done != 1")
		}
		fmt.Println("repeat lock unlock twice")
		err = kv.Unlock(kvPair)
		assert.NoError(t, err, "Unexpected error from Unlock")

		for done == 0 {
			time.Sleep(time.Second)
		}

		key = "doubleLock"
		kvPair, err = lockMethod(key)
		assert.NoError(t, err, "Unexpected error in lock")
		go func() {
			time.Sleep(10 * time.Second)
			err = kv.Unlock(kvPair)
			assert.NoError(t, err, "Unexpected error from Unlock")
		}()
		kvPair2, err := lockMethod(key)
		assert.NoError(t, err, "Double lock")
		err = kv.Unlock(kvPair2)
		assert.NoError(t, err, "Unexpected error from Unlock")

		kvPair, err = lockMethod("key1")
		assert.NoError(t, err, "Unexpected error in lock")
		go func() {
			time.Sleep(1 * time.Second)
			err = kv.Unlock(kvPair)
			assert.NoError(t, err, "Unexpected error from Unlock")
		}()
		kvPair2, err = lockMethod("key2")
		assert.NoError(t, err, "diff lock")
		err = kv.Unlock(kvPair2)
		assert.NoError(t, err, "Unexpected error from Unlock")

		lockTimedout := false
		fatalLockCb := func(format string, args ...interface{}) {
			logrus.Infof("Lock timeout called: "+format, args...)
			lockTimedout = true
		}
		kv.SetFatalCb(fatalLockCb)
		kv.SetLockTimeout(5 * time.Second)
		assert.Equal(t, kv.GetLockTimeout(), 5*time.Second, "get lock timeout")
		kvPair2, err = lockMethod("key2")
		time.Sleep(15 * time.Second)
		assert.True(t, lockTimedout, "lock timeout not called")
		err = kv.Unlock(kvPair2)
		kv.SetLockTimeout(5 * time.Second)
	}
}

func lockBetweenRestarts(kv kvdb.Kvdb, t *testing.T, start StartKvdb, stop StopKvdb) {
	lockMethods := getLockMethods(kv)
	for _, lockMethod := range lockMethods {
		// Try to take the lock again
		// We need this test for consul to check if LockDelay is not set
		fmt.Println("lock before restarting kvdb")
		kvPair3, err := lockMethod("key3")
		assert.NoError(t, err, "Unable to take a lock")
		fmt.Println("stopping kvdb")
		err = stop()
		assert.NoError(t, err, "Unable to stop kvdb")
		// Unlock the key
		go func() { kv.Unlock(kvPair3) }()

		time.Sleep(30 * time.Second)

		fmt.Println("starting kvdb")
		err = start(false)
		assert.NoError(t, err, "Unable to start kvdb")
		time.Sleep(30 * time.Second)

		lockChan := make(chan int)
		go func() {
			kvPair3, err = lockMethod("key3")
			lockChan <- 1
		}()
		select {
		case <-time.After(5 * time.Second):
			assert.Fail(t, "Unable to take a lock whose session is expired")
		case <-lockChan:
		}
		err = kv.Unlock(kvPair3)
		assert.NoError(t, err, "unable to unlock")
	}
}

func lockBasic(kv kvdb.Kvdb, t *testing.T) {

	lockMethods := getLockMethods(kv)

	for _, lockMethod := range lockMethods {
		fmt.Println("lock")

		key := "locktest"
		kvPair, err := lockMethod(key)
		assert.NoError(t, err, "Unexpected error in lock")

		if kvPair == nil {
			return
		}

		err = kv.Unlock(kvPair)
		assert.NoError(t, err, "Unexpected error from Unlock")

		kvPair, err = lockMethod(key)
		assert.NoError(t, err, "Failed to lock after unlock")

		err = kv.Unlock(kvPair)
		assert.NoError(t, err, "Unexpected error from Unlock")
	}

	// lock with timeout
	key := "testTimeoutKey"
	holdDuration := 30 * time.Second
	tryDuration := 5 * time.Second
	kvPair, err := kv.LockWithTimeout(key, "test", tryDuration, holdDuration)
	assert.NoError(t, err, "Unexpected error in lock")

	tryStart := time.Now()
	_, err = kv.LockWithTimeout(key, "test", tryDuration, holdDuration)
	if err == nil {
		logrus.Fatalf("Exiting because expected error %v %v", tryDuration, holdDuration)
	}
	duration := time.Since(tryStart)
	assert.True(t, duration < tryDuration+time.Second, "try duration")
	assert.Error(t, err, "lock expired before timeout")

	time.Sleep(holdDuration - time.Since(tryStart) - 5*time.Second)

	_, err = kv.LockWithTimeout(key, "test", 1*time.Second, holdDuration)
	duration = time.Since(tryStart)
	assert.Error(t, err, "lock expired before timeout")

	err = kv.Unlock(kvPair)
	assert.NoError(t, err, "Unexpected error from Unlock")

}

func watchFn(
	prefix string,
	opaque interface{},
	kvp *kvdb.KVPair,
	err error,
) error {
	data := opaque.(*watchData)
	time.Sleep(100 * time.Millisecond)
	if err != nil {
		assert.Equal(data.t, err, kvdb.ErrWatchStopped)
		data.watchStopped = true
		return err

	}
	fmt.Printf("+")

	// Doesn't work for ETCD because HTTP header contains Etcd-Index
	/*
		assert.True(data.t, kvp.KVDBIndex >= data.updateIndex,
			"KVDBIndex %v must be >= than updateIndex %v",
			kvp.KVDBIndex, data.updateIndex)

		assert.True(data.t, kvp.KVDBIndex > data.localIndex,
			"For Key (%v) : KVDBIndex %v must be > than localIndex %v action %v %v",
			kvp.Key, kvp.KVDBIndex, data.localIndex, kvp.Action, kvdb.KVCreate)
	*/

	assert.True(data.t, kvp.ModifiedIndex > data.localIndex,
		"For Key (%v) : ModifiedIndex %v must be > than localIndex %v",
		kvp.Key, kvp.ModifiedIndex, data.localIndex)

	data.localIndex = kvp.ModifiedIndex

	if data.whichKey == 0 {
		assert.Contains(data.t, data.otherKey, kvp.Key,
			"Bad kvpair key %s expecting %s with action %v",
			kvp.Key, data.key, kvp.Action)
	} else {
		assert.Contains(data.t, data.key, kvp.Key,
			"Bad kvpair key %s expecting %s with action %v",
			kvp.Key, data.key, kvp.Action)
	}

	assert.Equal(data.t, data.action, kvp.Action,
		"For Key (%v) : Expected action %v actual %v",
		kvp.Key, data.action, kvp.Action)
	if data.action != kvp.Action {
		value := string(kvp.Value)
		logrus.Panicf("Aborting because (%v != %v) (%v) %v != %v (Value = %v)",
			data.action, kvp.Action, prefix, data, kvp, value)
	}

	if string(kvp.Value) == data.stop {
		return errors.New(data.stop)
	}

	atomic.AddInt32(&data.reader, 1)
	return nil
}

func watchUpdate(kv kvdb.Kvdb, data *watchData) error {
	var err error
	var kvp *kvdb.KVPair

	data.reader, data.writer = 0, 0
	atomic.AddInt32(&data.writer, 1)
	// whichKey = 1 : key
	// whichKey = 0 : otherKey
	atomic.SwapInt32(&data.whichKey, 1)
	data.action = kvdb.KVCreate
	fmt.Printf("-")
	fmt.Printf("XXX Creating watchUpdate key %v\n", data.key)
	kvp, err = kv.Create(data.key, []byte("bar"), 0)
	for i := 0; i < data.iterations && err == nil; i++ {
		fmt.Printf("-")

		for data.writer != data.reader {
			time.Sleep(time.Millisecond * 100)
		}
		atomic.AddInt32(&data.writer, 1)
		data.action = kvdb.KVSet
		fmt.Printf("XXX Putting watchUpdate key %v\n", data.key)
		kvp, err = kv.Put(data.key, []byte("bar"), 0)

		data.updateIndex = kvp.KVDBIndex
		assert.NoError(data.t, err, "Unexpected error in Put")
	}

	fmt.Printf("-")
	for data.writer != data.reader {
		time.Sleep(time.Millisecond * 100)
	}
	atomic.AddInt32(&data.writer, 1)
	// Delete key
	data.action = kvdb.KVDelete
	kv.Delete(data.key)

	fmt.Printf("-")
	for data.writer != data.reader {
		time.Sleep(time.Millisecond * 100)
	}
	atomic.AddInt32(&data.writer, 1)

	atomic.SwapInt32(&data.whichKey, 0)
	data.action = kvdb.KVDelete
	// Delete otherKey
	kv.Delete(data.otherKey)

	fmt.Printf("-")
	for data.writer != data.reader {
		time.Sleep(time.Millisecond * 100)
	}
	atomic.AddInt32(&data.writer, 1)

	atomic.SwapInt32(&data.whichKey, 1)
	data.action = kvdb.KVCreate
	_, err = kv.Create(data.key, []byte(data.stop), 0)
	return err
}

func watchKey(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("\nwatchKey")

	watchData := watchData{
		t:          t,
		key:        "tree/key1",
		otherKey:   "tree/otherKey1",
		stop:       "stop",
		iterations: 2,
	}

	kv.Delete(watchData.key)
	kv.Delete(watchData.otherKey)
	// First create a key. We should not get update for this create.
	kvp, err := kv.Create(watchData.otherKey, []byte("bar"), 0)
	// Let the create operation finish and then start the watch
	time.Sleep(2 * time.Second)

	err = kv.WatchKey(watchData.otherKey, kvp.ModifiedIndex, &watchData, watchFn)
	if err != nil {
		fmt.Printf("Cannot test watchKey: %v\n", err)
		return
	}

	err = kv.WatchKey(watchData.key, 0, &watchData, watchFn)
	if err != nil {
		fmt.Printf("Cannot test watchKey: %v\n", err)
		return
	}

	// Sleep for sometime before calling the watchUpdate go routine.
	time.Sleep(time.Second * 2)

	go watchUpdate(kv, &watchData)

	for watchData.watchStopped == false {
		time.Sleep(time.Millisecond * 100)
	}

	// Stop the second watch
	atomic.SwapInt32(&watchData.whichKey, 0)
	watchData.action = kvdb.KVCreate
	_, err = kv.Create(watchData.otherKey, []byte(watchData.stop), 0)
}

func randomUpdate(kv kvdb.Kvdb, w *watchData) {
	for w.watchStopped == false {
		kv.Put("randomKey", []byte("bar"), 0)
		time.Sleep(time.Millisecond * 80)
	}
}

func watchTree(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("\nwatchTree")

	tree := "tree"

	watchData := watchData{
		t:          t,
		key:        tree + "/key",
		otherKey:   tree + "/otherKey",
		stop:       "stop",
		iterations: 2,
	}
	_, err := kv.Delete(watchData.key)
	_, err = kv.Delete(watchData.otherKey)

	// First create a tree to watch for. We should not get update for this create.
	kvp, err := kv.Create(watchData.otherKey, []byte("bar"), 0)
	// Let the create operation finish and then start the watch

	time.Sleep(time.Second)
	err = kv.WatchTree(tree, kvp.ModifiedIndex, &watchData, watchFn)
	if err != nil {
		fmt.Printf("Cannot test watchKey: %v\n", err)
		return
	}

	// Sleep for sometime before calling the watchUpdate go routine.
	time.Sleep(time.Millisecond * 100)

	go randomUpdate(kv, &watchData)
	go watchUpdate(kv, &watchData)

	for watchData.watchStopped == false {
		time.Sleep(time.Millisecond * 100)
	}
}

func watchWithIndexCb(
	prefix string,
	opaque interface{},
	kvp *kvdb.KVPair,
	err error,
) error {

	data, ok := opaque.(*watchData)
	if !ok {
		data.t.Fatalf("Unable to parse waitIndex in watch callback")
	}
	if err != nil {
		assert.Equal(data.t, err, kvdb.ErrWatchStopped)
		data.watchStopped = true
		return err

	}
	if kvp != nil && len(kvp.Value) != 0 && string(kvp.Value) == "stop" {
		// Stop the watch
		return fmt.Errorf("stop the watch")
	}
	assert.True(data.t, kvp.ModifiedIndex >= data.localIndex,
		"For key: %v. ModifiedIndex (%v) should be > than waitIndex (%v)",
		kvp.Key, kvp.ModifiedIndex, data.localIndex,
	)
	assert.True(data.t, kvp.ModifiedIndex <= data.localIndex+3,
		"For key: %v. Got extra updates. Current ModifiedIndex: %v. Expected last update index: %v",
		kvp.Key, kvp.ModifiedIndex, data.localIndex+2,
	)
	if kvp.ModifiedIndex == data.localIndex+3 {
		assert.True(data.t, kvp.Action == kvdb.KVDelete,
			"Expected action: %v, action action: %v",
			kvdb.KVDelete, kvp.Action,
		)
	}
	return nil
}

func watchWithIndex(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("\nwatchWithIndex")

	tree := "indexTree"
	subtree := tree + "/subtree"
	key := subtree + "/watchWithIndex"
	key1 := subtree + "/watchWithIndex21"

	kv.DeleteTree(tree)

	kvp, err := kv.Create(key, []byte("bar"), 0)
	assert.NoError(t, err, "Unexpected error in create: %v", err)

	time.Sleep(time.Millisecond * 100)

	waitIndex := kvp.ModifiedIndex + 2
	watchData := watchData{
		t:          t,
		key:        key,
		localIndex: waitIndex,
	}

	err = kv.WatchTree(tree, waitIndex, &watchData, watchWithIndexCb)
	if err != nil {
		fmt.Printf("Cannot test watchTree for watchWithIndex: %v", err)
		return
	}
	// Should not get updates for these
	kv.Put(key, []byte("bar1"), 0)
	kv.Put(key, []byte("bar1"), 0)
	// Should get updates for these
	kv.Put(key, []byte("bar2"), 0)
	kv.Create(key1, []byte("bar"), 0)
	kv.Delete(key)
	kv.Put(key, []byte("stop"), 0)
}

func cas(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("\ncas")

	key := "foo/docker"
	val := "great"
	defer func() {
		kv.DeleteTree(key)
	}()

	kvPair, err := kv.Put(key, []byte(val), 0)
	assert.NoError(t, err, "Unxpected error in Put")

	kvPair, err = kv.Get(key)
	assert.NoError(t, err, "Failed in Get")

	_, err = kv.CompareAndSet(kvPair, kvdb.KVFlags(0), []byte("badval"))
	assert.Error(t, err, "CompareAndSet should fail on an incorrect previous value")

	copyKVPair := *kvPair
	copyKVPair.ModifiedIndex++
	_, err = kv.CompareAndSet(&copyKVPair, kvdb.KVModifiedIndex, nil)
	assert.Error(t, err, "CompareAndSet should fail on an incorrect modified index")

	//kvPair.ModifiedIndex--
	copyKVPair.ModifiedIndex--
	kvPair, err = kv.CompareAndSet(&copyKVPair, kvdb.KVModifiedIndex, nil)
	assert.NoError(t, err, "CompareAndSet should succeed on an correct modified index")

	kvPairNew, err := kv.CompareAndSet(kvPair, kvdb.KVFlags(0), []byte(val))
	assert.NoError(t, err, "CompareAndSet should succeed on an correct value")

	if kvPairNew != nil {
		kvPair = kvPairNew
	}

	kvPair, err = kv.CompareAndSet(kvPair, kvdb.KVModifiedIndex, []byte(val))
	assert.NoError(t, err, "CompareAndSet should succeed on an correct value and modified index")
}

func cad(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("\ncad")

	key := "foo/docker"
	val := "great"
	defer func() {
		kv.DeleteTree(key)
	}()

	kvPair, err := kv.Put(key, []byte(val), 0)
	assert.NoError(t, err, "Unxpected error in Put")

	kvPair, err = kv.Get(key)
	assert.NoError(t, err, "Failed in Get")

	copyKVPair1 := *kvPair
	copyKVPair1.Value = []byte("badval")
	_, err = kv.CompareAndDelete(&copyKVPair1, kvdb.KVFlags(0))
	assert.Error(t, err, "CompareAndDelete should fail on an incorrect previous value")

	copyKVPair2 := *kvPair
	copyKVPair2.ModifiedIndex++
	_, err = kv.CompareAndDelete(&copyKVPair2, kvdb.KVModifiedIndex)
	assert.Error(t, err, "CompareAndDelete should fail on an incorrect modified index")

	//kvPair.ModifiedIndex--
	copyKVPair2.ModifiedIndex--
	kvPair, err = kv.CompareAndDelete(&copyKVPair2, kvdb.KVModifiedIndex)
	assert.NoError(t, err, "CompareAndDelete should succeed on an correct modified index")

	kvPair, err = kv.Put(key, []byte(val), 0)
	assert.NoError(t, err, "Unxpected error in Put")

	_, err = kv.CompareAndDelete(kvPair, kvdb.KVFlags(0))
	assert.NoError(t, err, "CompareAndDelete should succeed on an correct value")
}

func addUser(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("addUser")

	err := kv.AddUser("test", "test123")
	assert.NoError(t, err, "Error in Adding User")
}

func removeUser(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("removeUser")

	err := kv.RemoveUser("test")
	assert.NoError(t, err, "Error in Removing User")
}

func grantRevokeUser(kvRootUser kvdb.Kvdb, datastoreInit kvdb.DatastoreInit, t *testing.T) {
	fmt.Println("grantRevokeUser")

	kvRootUser.Create("allow1/foo", []byte("bar"), 0)
	kvRootUser.Create("allow2/foo", []byte("bar"), 0)
	kvRootUser.Create("disallow/foo", []byte("bar"), 0)
	err := kvRootUser.GrantUserAccess("test", kvdb.ReadWritePermission, "allow1/*")
	assert.NoError(t, err, "Error in Grant User")
	err = kvRootUser.GrantUserAccess("test", kvdb.ReadWritePermission, "allow2/*")
	assert.NoError(t, err, "Error in Grant User")
	err = kvRootUser.GrantUserAccess("test", kvdb.ReadWritePermission, "disallow/*")
	assert.NoError(t, err, "Error in Grant User")
	err = kvRootUser.RevokeUsersAccess("test", kvdb.ReadWritePermission, "disallow/*")
	assert.NoError(t, err, "Error in Revoke User")

	options := make(map[string]string)
	options[kvdb.UsernameKey] = "test"
	options[kvdb.PasswordKey] = "test123"
	options[kvdb.CAFileKey] = "/etc/pwx/pwx-ca.crt"
	options[kvdb.CertFileKey] = "/etc/pwx/pwx-user-cert.crt"
	options[kvdb.CertKeyFileKey] = "/etc/pwx/pwx-user-key.key"
	machines := []string{"https://192.168.56.101:2379"}
	kvTestUser, _ := datastoreInit("pwx/test", machines, options, fatalErrorCb())

	actual := "actual"
	_, err = kvTestUser.Put("allow1/foo", []byte(actual), 0)
	assert.NoError(t, err, "Error in writing to allowed tree")
	kvPair, err := kvTestUser.Get("allow1/foo")
	assert.NoError(t, err, "Error in accessing allowed values")
	if err == nil {
		assert.Equal(t, string(kvPair.Value), "actual")
	}

	_, err = kvTestUser.Put("allow2/foo", []byte(actual), 0)
	assert.NoError(t, err, "Error in writing to allowed tree")
	kvPair, err = kvTestUser.Get("allow2/foo")
	assert.NoError(t, err, "Error in accessing allowed values")
	if err == nil {
		assert.Equal(t, string(kvPair.Value), "actual")
	}

	actual2 := "actual2"
	_, err = kvTestUser.Put("disallow/foo", []byte(actual2), 0)
	assert.Error(t, err, "Expected error in writing to disallowed tree")
	kvPair, err = kvTestUser.Get("disallow/foo")
	assert.Error(t, err, "Expected error in accessing disallowed values")

	kvRootUser.DeleteTree("allow1")
	kvRootUser.DeleteTree("allow2")
	kvRootUser.DeleteTree("disallow")
}

func collect(kv kvdb.Kvdb, t *testing.T) {
	for _, useStartVersion := range []bool{false, true} {

		startVersion := func(modifiedVersion uint64) uint64 {
			if useStartVersion {
				return modifiedVersion
			}
			return 0
		}

		fmt.Println("collect")

		// XXX FIXME this is a bug... root should not have the prefix
		root := "pwx/test/collect"
		firstLevel := root + "/first"
		secondLevel := root + "/second"

		kv.DeleteTree(root)

		kvp, _ := kv.Create(firstLevel, []byte("bar"), 0)
		fmt.Printf("KVP is %v and KV is %v\n", kvp, kv)
		collector, _ := kvdb.NewUpdatesCollector(kv, secondLevel,
			startVersion(kvp.CreatedIndex))
		time.Sleep(time.Second)
		var updates []*kvdb.KVPair
		updateFn := func(start, end int) uint64 {
			lastUpdateVersion := uint64(0)
			for i := start; i < end; i++ {
				newLeaf := strconv.Itoa(i)
				// First update the tree being watched.
				kvp1, _ := kv.Create(secondLevel+"/"+newLeaf, []byte(newLeaf), 0)
				// Next update another value in kvdb which is not being
				// watched, this update will cause kvdb index to move forward.
				kv.Update(firstLevel, []byte(newLeaf), 0)
				updates = append(updates, kvp1)
				lastUpdateVersion = kvp1.ModifiedIndex
			}
			return lastUpdateVersion
		}
		lastUpdateVersion := updateFn(0, 10)
		// Allow watch updates to come back.
		time.Sleep(time.Millisecond * 500)
		collector.Stop()

		updateFn(10, 20)
		lastKVIndex := kvp.CreatedIndex
		lastLeafIndex := -1
		cb := func(prefix string, opaque interface{}, kvp *kvdb.KVPair,
			err error) error {
			fmt.Printf("\nReplaying KVP: %v", *kvp)
			assert.True(t, err == nil, "Error is nil %v", err)
			assert.True(t, kvp.ModifiedIndex > lastKVIndex,
				"Modified index %v lower than last index %v",
				kvp.ModifiedIndex, lastKVIndex)
			lastKVIndex = kvp.ModifiedIndex
			strValue := string(kvp.Value)
			value := secondLevel + "/" + strValue
			assert.True(t, strings.Compare(kvp.Key, value) == 0,
				"Key: %v, Value: %v", kvp.Key, value)
			leafIndex, _ := strconv.Atoi(strValue)
			assert.True(t, leafIndex == lastLeafIndex+1,
				"Last leaf: %v, leaf: %v", kvp.Key,
				value)
			lastLeafIndex = leafIndex
			return nil
		}

		replayCb := make([]kvdb.ReplayCb, 1)
		replayCb[0].Prefix = secondLevel
		replayCb[0].WatchCB = cb
		replayCb[0].WaitIndex = kvp.CreatedIndex

		lastVersion, err := collector.ReplayUpdates(replayCb)
		assert.True(t, err == nil, "Replay encountered error %v", err)
		assert.True(t, lastLeafIndex == 9, "Last leaf index %v, expected : 9",
			lastLeafIndex)
		assert.True(t, lastVersion == lastUpdateVersion,
			"Last update %d and last replay %d version mismatch",
			lastVersion, lastUpdateVersion)

		// Test with no updates.
		thirdLevel := root + "/third"
		kvp, _ = kv.Create(thirdLevel, []byte("bar_update"), 0)
		collector, _ = kvdb.NewUpdatesCollector(kv, thirdLevel,
			startVersion(kvp.ModifiedIndex))
		time.Sleep(2 * time.Second)
		replayCb[0].WaitIndex = kvp.ModifiedIndex
		_, err = collector.ReplayUpdates(replayCb)
		assert.True(t, err == nil, "Replay encountered error %v", err)
		assert.True(t, lastLeafIndex == 9, "Last leaf index %v, expected : 9",
			lastLeafIndex)
		if lastLeafIndex != 9 {
			logrus.Fatalf("lastLeafIndex is %v", lastLeafIndex)
		}

		// Test with kvdb returning error because update index was too old.
		fourthLevel := root + "/fourth"
		kv.Create(fourthLevel, []byte(strconv.Itoa(0)), 0)
		for i := 1; i < 2000; i++ {
			kv.Update(fourthLevel, []byte(strconv.Itoa(i)), 0)
		}
		collector, _ = kvdb.NewUpdatesCollector(kv, fourthLevel,
			startVersion(kvp.ModifiedIndex))
		kv.Update(fourthLevel, []byte(strconv.Itoa(2000)), 0)
		time.Sleep(500 * time.Millisecond)
		cb = func(prefix string, opaque interface{}, kvp *kvdb.KVPair,
			err error) error {
			fmt.Printf("Error is %v", err)
			assert.True(t, err != nil, "Error is nil %v", err)
			return nil
		}
		replayCb[0].WatchCB = cb
		replayCb[0].WaitIndex = kvp.ModifiedIndex
		collector.ReplayUpdates(replayCb)
	}
}

func serialization(kv kvdb.Kvdb, t *testing.T) {
	fmt.Println("serialization")
	kv.DeleteTree("")
	prefix := "/folder"
	type A struct {
		A1 int
		A2 string
	}
	type B struct {
		B1 string
	}
	a1 := A{1, "a"}
	b1 := B{"2"}
	c1 := A{2, "b"}
	_, err := kv.Put(prefix+"/a/a1", a1, 0)
	assert.NoError(t, err, "Unexpected error on put")
	_, err = kv.Put(prefix+"/b/b1", b1, 0)
	assert.NoError(t, err, "Unexpected error on put")
	_, err = kv.Put(prefix+"/c/c1", c1, 0)
	assert.NoError(t, err, "Unexpected error on put")
	kv.Delete(prefix + "/c/c1")
	b, err := kv.Serialize()
	assert.NoError(t, err, "Unexpected error on serialize")
	kvps, err := kv.Deserialize(b)
	assert.NoError(t, err, "Unexpected error on de-serialize")
	for _, kvp := range kvps {
		if strings.Contains(kvp.Key, "/a/a1") {
			aa := A{}
			err = json.Unmarshal(kvp.Value, &aa)
			assert.NoError(t, err, "Unexpected error on unmarshal")
			assert.Equal(t, aa.A1, a1.A1, "Unequal A values")
			assert.Equal(t, aa.A2, a1.A2, "Unequal A values")
		} else if strings.Contains(kvp.Key, "/b/b1") {
			bb := B{}
			err = json.Unmarshal(kvp.Value, &bb)
			assert.NoError(t, err, "Unexpected error on unmarshal")
			assert.Equal(t, bb.B1, b1.B1, "Unequal B values")
		} else if strings.Contains(kvp.Key, "/c") {
			// ETCD v2 will return an empty folder, but v3 and consul will not
			assert.Equal(t, len(kvp.Value), 0, "Unexpected C values")
		} else {
			t.Fatalf("Unexpected values returned by deserialize: %v", kvp.Key)
		}
	}
}
