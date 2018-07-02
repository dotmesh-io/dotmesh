package testutil

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/coreos/etcd/embed"
	"github.com/nu7hatch/gouuid"
)

func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func newTestServer() (clientPort int, teardown func(), err error) {
	cfg := embed.NewConfig()
	clientPort, err = GetFreePort()
	if err != nil {
		return
	}

	peerPort, err := GetFreePort()
	if err != nil {
		return
	}

	lpurl, _ := url.Parse(fmt.Sprintf("http://localhost:%d", peerPort))
	lcurl, _ := url.Parse(fmt.Sprintf("http://localhost:%d", clientPort))

	cfg.LPUrls = []url.URL{*lpurl}
	cfg.LCUrls = []url.URL{*lcurl}

	dir, err := ioutil.TempDir(os.TempDir(), "dotmesh-test")
	if err != nil {
		return
	}

	cfg.Dir = dir
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Fatal(err)
	}
	select {
	case <-e.Server.ReadyNotify():
		log.Printf("Server is ready!")
	case <-time.After(10 * time.Second):
		e.Server.Stop() // trigger a shutdown
		log.Printf("Server took too long to start!")
	}
	go func() {
		log.Fatal(<-e.Err())
	}()

	teardown = func() {
		// shutdown embedded Etcd server
		e.Close()
		// remove data directory
		os.RemoveAll(dir)
	}

	return clientPort, teardown, nil
}

// GetEtcdClient - is a helper function that starts an embedded Etcd server on a free random port
// and returns a configured Etcd client, teardown function and error (if any). User's responsibility is
// to call a teardown function once test is completed
func GetEtcdClient() (client.KeysAPI, func(), error) {

	port, teardown, err := newTestServer()
	if err != nil {
		return nil, nil, err
	}

	endpoint := fmt.Sprintf("http://localhost:%d", port)
	cfg := client.Config{
		Endpoints:               []string{endpoint},
		HeaderTimeoutPerRequest: time.Second * 10,
	}
	etcdClient, err := client.New(cfg)
	if err != nil {
		teardown()
		return nil, nil, err
	}
	return client.NewKeysAPI(etcdClient), teardown, nil

}

func GetTestPrefix() string {
	id, err := uuid.NewV4()
	if err != nil {
		panic(err)
	}

	return "test" + id.String()
}
