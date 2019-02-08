package commands

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"time"

	"github.com/dotmesh-io/dotmesh/pkg/types"
)

func startEtcdContainer(clusterUrl, adminPassword, adminKey, pkiPath string) error {

	// - Start etcd with discovery token on non-standard ports (to avoid
	//   conflicting with an existing etcd).
	fmt.Printf("Guessing docker host's IPv4 address (should be routable from other cluster nodes)... ")
	ipAddrs, err := guessHostIPv4Addresses()
	if err != nil {
		return err
	}
	// TODO add an argument to override this
	fmt.Printf("got: %s.\n", strings.Join(ipAddrs, ","))

	fmt.Printf("Guessing unique name for docker host (using hostname, must be unique wrt other cluster nodes)... ")
	hostnameString, err := guessHostname()
	if err != nil {
		return err
	}
	// TODO add an argument to override this
	fmt.Printf("got: %s.\n", hostnameString)

	peerURLs := []string{}
	clientURLs := []string{}
	for _, ipAddr := range ipAddrs {
		peerURLs = append(peerURLs, fmt.Sprintf("https://%s:42380", ipAddr))
		clientURLs = append(clientURLs, fmt.Sprintf("https://%s:42379", ipAddr))
	}

	fmt.Printf("Starting etcd... ")
	args := []string{
		"docker", "run", "--restart=always",
		"-d", "--name=dotmesh-etcd",
		"-p", "42379:42379", "-p", "42380:42380",
		"-v", "dotmesh-etcd-data:/var/lib/etcd",
		// XXX assuming you can bind-mount a path from the host is dubious,
		// hopefully it works well enough on docker for mac and docker machine.
		// An alternative approach could be to pass in the cluster secret as an
		// env var and download it and cache it in a docker volume.
		"-v", fmt.Sprintf("%s:/pki", maybeEscapeLinuxEmulatedPathOnWindows(pkiPath)),
		etcdDockerImage,
		"etcd", "--name", hostnameString,
		"--data-dir", "/var/lib/etcd",

		// Client-to-server communication:
		// Certificate used for SSL/TLS connections to etcd. When this option
		// is set, you can set advertise-client-urls using HTTPS schema.
		"--cert-file=/pki/apiserver.pem",
		// Key for the certificate. Must be unencrypted.
		"--key-file=/pki/apiserver-key.pem",
		// When this is set etcd will check all incoming HTTPS requests for a
		// client certificate signed by the trusted CA, requests that don't
		// supply a valid client certificate will fail.
		"--client-cert-auth",
		// Trusted certificate authority.
		"--trusted-ca-file=/pki/ca.pem",

		// Peer (server-to-server / cluster) communication:
		// The peer options work the same way as the client-to-server options:
		// Certificate used for SSL/TLS connections between peers. This will be
		// used both for listening on the peer address as well as sending
		// requests to other peers.
		"--peer-cert-file=/pki/apiserver.pem",
		// Key for the certificate. Must be unencrypted.
		"--peer-key-file=/pki/apiserver-key.pem",
		// When set, etcd will check all incoming peer requests from the
		// cluster for valid client certificates signed by the supplied CA.
		"--peer-client-cert-auth",
		// Trusted certificate authority.
		"--peer-trusted-ca-file=/pki/ca.pem",

		// TODO stop using the same certificate for peer and client/server
		// connection validation

		// listen
		"--listen-peer-urls", "https://0.0.0.0:42380",
		"--listen-client-urls", "https://0.0.0.0:42379",
		// advertise
		"--initial-advertise-peer-urls", strings.Join(peerURLs, ","),
		"--advertise-client-urls", strings.Join(clientURLs, ","),
	}
	if etcdInitialCluster == "" {
		args = append(args, []string{"--discovery", clusterUrl}...)
	} else {
		args = append(args, []string{
			"--initial-cluster-state", "existing",
			"--initial-cluster", etcdInitialCluster,
		}...)
	}
	resp, err := exec.Command(args[0], args[1:]...).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", resp)
		return err
	}
	fmt.Printf("done.\n")
	return nil

}

type DiscoveryResponse struct {
	Action string `json:"action"`
	Node   struct {
		Key   string `json:"key"`
		Dir   bool   `json:"dir"`
		Nodes []struct {
			Key           string `json:"key"`
			Value         string `json:"value"`
			ModifiedIndex int    `json:"modifiedIndex"`
			CreatedIndex  int    `json:"createdIndex"`
		} `json:"nodes"`
		ModifiedIndex int `json:"modifiedIndex"`
		CreatedIndex  int `json:"createdIndex"`
	} `json:"node"`
}

func shouldStartEtcd(clusterURL string) (yes bool, existingAddress string, err error) {
	resp, err := http.Get(clusterURL)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	bts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	var dr DiscoveryResponse
	err = json.Unmarshal(bts, &dr)
	if err != nil {
		return
	}

	// start a new cluster if there aren't any nodes
	if len(dr.Node.Nodes) == 0 {
		return true, "", nil
	}
	fmt.Printf("Extracting Etcd addresses from '%s' \n", dr.Node.Nodes[0].Value)
	addresses := extractAddresses(dr.Node.Nodes[0].Value)
	if len(addresses) == 0 {
		// no valid addresses?
		return true, "", nil
	}

	fmt.Printf("Extracted addresses: %s \n", strings.Join(addresses, ", "))

	filtered := []string{}
	for _, a := range addresses {
		fmt.Printf("Checking connection to host: %s\n", a)
		err := checkConn(a)
		if err != nil {
			fmt.Printf("Connection to host '%s' failed: %s \n", a, err)
		} else {
			filtered = append(filtered, a)
			fmt.Printf("Connection to host '%s' successful\n", a)
		}
	}

	if len(filtered) == 0 {
		return true, "", nil
	}

	// don't start Etcd. Picking the first one if there are more
	return false, filtered[0], nil

}

func checkConn(host string) error {
	_, err := net.DialTimeout("tcp", host+":"+types.DefaultEtcdClientPort, 500*time.Millisecond)
	return err
}

func extractAddresses(joinURL string) []string {
	addresses := []string{}

	pieces := strings.Split(joinURL, ",")
	if len(pieces) == 0 {
		return addresses
	}

	for _, piece := range pieces {

		shards := strings.Split(piece, "=")
		if len(shards) != 2 {
			continue
		}

		u, err := url.Parse(shards[1])
		if err != nil {
			continue
		}

		h, _, err := net.SplitHostPort(u.Host)
		if err != nil {
			continue
		}

		addresses = append(addresses, h)
	}
	return addresses
}
