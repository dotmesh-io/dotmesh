package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/portworx/kvdb"
	"github.com/stretchr/testify/require"
)

const (
	urlPrefix = "http://"
	localhost = "localhost"
)

var (
	names      = []string{"infra0", "infra1", "infra2", "infra3", "infra4"}
	clientUrls = []string{"http://127.0.0.1:20379", "http://127.0.0.1:21379", "http://127.0.0.1:22379", "http://127.0.0.1:23379", "http://127.0.0.1:24379"}
	peerPorts  = []string{"20380", "21380", "22380", "23380", "24380"}
	dataDirs   = []string{"/tmp/node0", "/tmp/node1", "/tmp/node2", "/tmp/node3", "/tmp/node4"}
	cmds       map[int]*exec.Cmd
)

// RunControllerTests is a test suite for kvdb controller APIs
func RunControllerTests(datastoreInit kvdb.DatastoreInit, t *testing.T) {
	cleanup()
	// Initialize node 0
	cmds = make(map[int]*exec.Cmd)
	index := 0
	initCluster := make(map[string][]string)
	peerURL := urlPrefix + localhost + ":" + peerPorts[index]
	initCluster[names[index]] = []string{peerURL}
	cmd, err := startEtcd(index, initCluster, "new")
	if err != nil {
		t.Fatalf(err.Error())
	}
	cmds[index] = cmd
	kv, err := datastoreInit("pwx/test", clientUrls, nil, fatalErrorCb())
	if err != nil {
		cmd.Process.Kill()
		t.Fatalf(err.Error())
	}

	testAddMember(kv, t)
	testRemoveMember(kv, t)
	testReAdd(kv, t)
	testUpdateMember(kv, t)
	testMemberStatus(kv, t)
	testDefrag(kv, t)
	controllerLog("Stopping all etcd processes")
	for _, cmd := range cmds {
		cmd.Process.Kill()
	}
}

func testAddMember(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testAddMember")
	// Add node 1
	index := 1
	controllerLog("Adding node 1")
	initCluster, err := kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	require.Equal(t, 2, len(initCluster), "Init Cluster length does not match")
	cmd, err := startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd

	// Check the list returned
	list, err := kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 2, len(list), "List returned different length of cluster")
}

func testRemoveMember(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testRemoveMember")
	// Add node 2
	index := 2
	controllerLog("Adding node 2")
	initCluster, err := kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	require.Equal(t, 3, len(initCluster), "Init Cluster length does not match")
	cmd, err := startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd
	// Check the list returned
	list, err := kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 3, len(list), "List returned different length of cluster")

	// Remove node 1
	index = 1
	controllerLog("Removing node 1")
	err = kv.RemoveMember(names[index], localhost)
	require.NoError(t, err, "Error on RemoveMember")

	cmd, _ = cmds[index]
	cmd.Process.Kill()
	delete(cmds, index)
	// Check the list returned
	list, err = kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 2, len(list), "List returned different length of cluster")
}

func testReAdd(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testReAdd")
	// Add node 1 back
	index := 1
	controllerLog("Re-adding node 1")
	// For re-adding we need to delete the data-dir of this member
	os.RemoveAll(dataDirs[index])
	initCluster, err := kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	require.Equal(t, 3, len(initCluster), "Init Cluster length does not match")
	cmd, err := startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd

	// Check the list returned
	list, err := kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 3, len(list), "List returned different length of cluster")
}

func testUpdateMember(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testUpdateMember")

	// Stop node 1
	index := 1
	cmd, _ := cmds[index]
	cmd.Process.Kill()
	delete(cmds, index)
	// Change the port
	peerPorts[index] = "33380"

	// Update the member
	initCluster, err := kv.UpdateMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on UpdateMember")
	require.Equal(t, 3, len(initCluster), "Initial cluster length does not match")
	cmd, err = startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd

	list, err := kv.ListMembers()
	require.NoError(t, err, "Error on ListMembers")
	require.Equal(t, 3, len(list), "List returned different length of cluster")
}

func testDefrag(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testDefrag")

	// Run defrag with 0 timeout
	index := 1
	err := kv.Defragment(clientUrls[index], 0)
	require.NoError(t, err, "Unexpected error on Defragment")

	// Run defrag with 60 timeout
	index = 4
	err = kv.Defragment(clientUrls[index], 60)
	require.NoError(t, err, "Unexpected error on Defragment")
}

func testMemberStatus(kv kvdb.Kvdb, t *testing.T) {
	controllerLog("testMemberStatus")

	index := 3
	controllerLog("Adding node 3")
	initCluster, err := kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	cmd, err := startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd

	// Wait for some time for etcd to detect a node offline
	time.Sleep(5 * time.Second)

	index = 4
	controllerLog("Adding node 4")
	initCluster, err = kv.AddMember(localhost, peerPorts[index], names[index])
	require.NoError(t, err, "Error on AddMember")
	cmd, err = startEtcd(index, initCluster, "existing")
	require.NoError(t, err, "Error on start etcd")
	cmds[index] = cmd

	// Wait for some time for etcd to detect a node offline
	time.Sleep(5 * time.Second)

	// Stop node 2
	stoppedIndex := 2
	cmd, _ = cmds[stoppedIndex]
	cmd.Process.Kill()
	delete(cmds, stoppedIndex)

	// Stop node 3
	stoppedIndex2 := 3
	cmd, _ = cmds[stoppedIndex2]
	cmd.Process.Kill()
	delete(cmds, stoppedIndex2)

	// Wait for some time for etcd to detect a node offline
	time.Sleep(5 * time.Second)

	numOfGoroutines := 10
	var wg sync.WaitGroup
	wg.Add(numOfGoroutines)

	checkMembers := func(id string, wait int) {
		defer wg.Done()
		// Add a sleep so that all go routines run just around the same time
		time.Sleep(time.Duration(wait) * time.Second)
		controllerLog("Listing Members for goroutine no. " + id)
		list, err := kv.ListMembers()
		require.NoError(t, err, "%v: Error on ListMembers", id)
		require.Equal(t, 5, len(list), "%v: List returned different length of cluster", id)

		downMember, ok := list[names[stoppedIndex]]
		require.True(t, ok, "%v: Could not find down member", id)
		require.Equal(t, len(downMember.ClientUrls), 0, "%v: Unexpected no. of client urls on down member", id)
		require.False(t, downMember.IsHealthy, "%v: Unexpected health of down member", id)

		for name, m := range list {
			if name == names[stoppedIndex] {
				continue
			}
			if name == names[stoppedIndex2] {
				continue
			}
			require.True(t, m.IsHealthy, "%v: Expected member %v to be healthy", id, name)
		}
		fmt.Println("checkMembers done for ", id)
	}
	for i := 0; i < numOfGoroutines; i++ {
		go checkMembers(strconv.Itoa(i), numOfGoroutines-1)
	}
	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()

	select {
	case <-c:
		return
	case <-time.After(5 * time.Minute):
		t.Fatalf("testMemberStatus timeout")
	}
}

func startEtcd(index int, initCluster map[string][]string, initState string) (*exec.Cmd, error) {
	peerURL := urlPrefix + localhost + ":" + peerPorts[index]
	clientURL := clientUrls[index]
	initialCluster := ""
	for name, ip := range initCluster {
		initialCluster = initialCluster + name + "=" + ip[0] + ","
	}
	fmt.Println("Starting etcd for node ", index, "with initial cluster: ", initialCluster)
	initialCluster = strings.TrimSuffix(initialCluster, ",")
	etcdArgs := []string{
		"--name=" +
			names[index],
		"--initial-advertise-peer-urls=" +
			peerURL,
		"--listen-peer-urls=" +
			peerURL,
		"--listen-client-urls=" +
			clientURL,
		"--advertise-client-urls=" +
			clientURL,
		"--initial-cluster=" +
			initialCluster,
		"--data-dir=" +
			dataDirs[index],
		"--initial-cluster-state=" +
			initState,
	}

	cmd := exec.Command("/tmp/test-etcd/etcd", etcdArgs...)
	cmd.Stdout = ioutil.Discard
	cmd.Stderr = ioutil.Discard
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("Failed to run %v(%v) : %v",
			names[index], etcdArgs, err.Error())
	}
	// XXX: Replace with check for etcd is up
	time.Sleep(10 * time.Second)
	return cmd, nil
}

func cleanup() {
	for _, dir := range dataDirs {
		os.RemoveAll(dir)
		os.MkdirAll(dir, 0777)
	}
}

func controllerLog(log string) {
	fmt.Println("--------------------")
	fmt.Println(log)
	fmt.Println("--------------------")
}
