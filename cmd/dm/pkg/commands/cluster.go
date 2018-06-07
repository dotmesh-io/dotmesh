package commands

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/sys/unix"

	"github.com/blang/semver"
	"github.com/coreos/etcd/client"
	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/pki"
	"github.com/dotmesh-io/dotmesh/cmd/dm/pkg/remotes"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/scrypt"
)

const DOTMESH_DOCKER_IMAGE = "quay.io/dotmesh/dotmesh-server"

const DOTMESH_UPGRADES_URL = "https://checkpoint.dotmesh.com/"
const DOTMESH_UPGRADES_INTERVAL_SECONDS = 14400 // 4 hours

// The following consts MUST MATCH those defined in cmd/dotmesh-server/pkg/main/users.go
//
// FIXME: When we have a shared library betwixt client and server, we can put all this in there.
const ADMIN_USER_UUID = "00000000-0000-0000-0000-000000000000"

// How many bytes of entropy in an API key
const API_KEY_BYTES = 32

// And in a salt
const SALT_BYTES = 32

// And in a password hash
const HASH_BYTES = 32

// Scrypt parameters, these are considered good as of 2017 according to https://godoc.org/golang.org/x/crypto/scrypt
const SCRYPT_N = 32768
const SCRYPT_R = 8
const SCRYPT_P = 1

var (
	serverCount        int
	traceAddr          string
	logAddr            string
	etcdInitialCluster string
	offline            bool
	dotmeshDockerImage string
	checkpointUrl      string
	checkpointInterval int
	etcdDockerImage    string
	dockerApiVersion   string
	usePoolDir         string
	usePoolName        string
	discoveryUrl       string
)

// names of environment variables we pass from the content of `dm cluster {init,join}`
// and pass into the dotmesh-server-outer container
// require_zfs.sh will pass these into dotmesh-server-inner
var inheritedEnvironment = []string{
	"FILESYSTEM_METADATA_TIMEOUT",
	"EXTRA_HOST_COMMANDS",
}

var timings map[string]float64
var lastTiming int64

var logFile *os.File

func startTiming() {
	var err error
	logFile, err = os.Create("dotmesh_install_log.txt")
	if err != nil {
		panic(err)
	}
	lastTiming = time.Now().UnixNano()
	timings = make(map[string]float64)
}

func logTiming(tag string) {
	now := time.Now().UnixNano()
	timings[tag] = float64(now-lastTiming) / (1000 * 1000 * 1000)
	lastTiming = now
}

func dumpTiming() {
	fmt.Fprintf(logFile, "=== TIMING ===\n")
	for tag, timing := range timings {
		fmt.Fprintf(logFile, "%s => %.2f\n", tag, timing)
	}
	fmt.Fprintf(logFile, "=== END TIMING ===\n")
	timings = map[string]float64{}
}

func NewCmdCluster(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Install a dotmesh server on a docker host, creating or joining a cluster",
		Long: `Either initialize a new cluster or join an existing one.

Requires: Docker >= 1.10.0. Must be run on the same machine where the docker
daemon is running. (Also works on Docker for Mac.)

Run 'dm cluster init' on one node, and then 'dm cluster join <cluster-url>' on
another.`,
	}
	cmd.AddCommand(NewCmdClusterInit(os.Stdout))
	cmd.AddCommand(NewCmdClusterJoin(os.Stdout))
	cmd.AddCommand(NewCmdClusterReset(os.Stdout))
	cmd.AddCommand(NewCmdClusterUpgrade(os.Stdout))

	cmd.AddCommand(NewCmdClusterBackupEtcd(os.Stdout))
	cmd.AddCommand(NewCmdClusterRestoreEtcd(os.Stdout, os.Stdin))

	cmd.PersistentFlags().StringVar(
		&traceAddr, "trace", "",
		"Hostname for Zipkin host to enable distributed tracing",
	)
	cmd.PersistentFlags().StringVar(
		&logAddr, "log", "",
		"Hostname for dotmesh logs to be forwarded to enable log aggregation",
	)
	cmd.PersistentFlags().StringVar(
		&dotmeshDockerImage, "image", DOTMESH_DOCKER_IMAGE+":"+dockerTag,
		"dotmesh-server docker image to use",
	)
	cmd.PersistentFlags().StringVar(
		&checkpointUrl, "dotmesh-upgrades-url", DOTMESH_UPGRADES_URL,
		"Dotmesh upgrades server URL, to check for Dotmesh updates",
	)
	cmd.PersistentFlags().IntVar(
		&checkpointInterval, "dotmesh-upgrades-seconds", DOTMESH_UPGRADES_INTERVAL_SECONDS,
		"How many seconds to wait been polls for new version data",
	)
	cmd.PersistentFlags().StringVar(
		&etcdDockerImage, "etcd-image",
		"quay.io/dotmesh/etcd:v3.0.15",
		"etcd docker image to use",
	)
	cmd.PersistentFlags().StringVar(
		&dockerApiVersion, "docker-api-version",
		"", "specific docker API version to use, if you're using a < 1.12 "+
			"docker daemon and getting a 'client is newer than server' error in the "+
			"logs (specify the 'server API version' from the error message here)",
	)
	cmd.PersistentFlags().StringVar(
		&usePoolDir, "use-pool-dir",
		"", "directory in which to make a file-based-pool; useful for testing",
	)
	cmd.PersistentFlags().StringVar(
		&usePoolName, "use-pool-name",
		"", "name of pool to import or create; useful for testing",
	)
	cmd.PersistentFlags().StringVar(
		&discoveryUrl, "discovery-url",
		"https://discovery.dotmesh.io", "URL of discovery service. "+
			"Use one you trust. Use HTTPS otherwise your private key will"+
			"be transmitted in plain text!",
	)
	cmd.PersistentFlags().BoolVar(
		&offline, "offline", false,
		"Do not attempt any operations that require internet access "+
			"(assumes dotmesh-server docker image has already been pulled)",
	)
	return cmd
}

func NewCmdClusterBackupEtcd(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup-etcd",
		Short: "Backup the contents of etcd for a cluster (the current remote)",
		Long:  "Online help: FIXME",
		Run: func(cmd *cobra.Command, args []string) {

			dm, err := remotes.NewDotmeshAPI(configPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
			dump, err := dm.BackupEtcd()
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
			out.Write([]byte(dump))

		},
	}
	return cmd
}

func NewCmdClusterRestoreEtcd(out io.Writer, in io.Reader) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore-etcd",
		Short: "Restore users (except admin) and registry from an etcd backup on stdin",
		Long:  "Online help: FIXME",
		Run: func(cmd *cobra.Command, args []string) {

			bs, err := ioutil.ReadAll(in)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}

			dm, err := remotes.NewDotmeshAPI(configPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
			err = dm.RestoreEtcd(string(bs))
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}

		},
	}
	return cmd
}

func NewCmdClusterInit(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize a dotmesh cluster",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#create-a-cluster-dm-cluster-init",
		Run: func(cmd *cobra.Command, args []string) {
			err := clusterInit(cmd, args, out)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
		},
	}
	cmd.Flags().IntVar(
		&serverCount, "count", 1,
		"Initial cluster size",
	)
	return cmd
}

func NewCmdClusterJoin(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "join",
		Short: "Join a node into an existing dotmesh cluster",
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#join-a-cluster-dm-cluster-join-discovery-url",
		Run: func(cmd *cobra.Command, args []string) {
			err := clusterJoin(cmd, args, out)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
		},
	}
	cmd.PersistentFlags().StringVar(
		&etcdInitialCluster, "etcd-initial-cluster", "",
		"Node was previously in etcd cluster, set this to the value of "+
			"'ETCD_INITIAL_CLUSTER' as given by 'etcdctl member add'",
	)
	return cmd
}

func NewCmdClusterUpgrade(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: fmt.Sprintf("Upgrade the Dotmesh server on this node to the client version (%s)", clientVersion),
		Long:  "Online help: https://docs.dotmesh.com/references/cli/#upgrade-your-node-dm-cluster-upgrade",
		Run: func(cmd *cobra.Command, args []string) {
			err := clusterUpgrade(cmd, args, out)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
		},
	}
	return cmd
}

func NewCmdClusterReset(out io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset",
		Short: "Uninstall dotmesh from a docker host",
		Long: `Remove the dotmesh-server and etcd containers. Deletes etcd data so that a new
cluster can be initialized. Does not delete any ZFS data (the data can be
'adopted' by a new cluster, but will lose name->filesystem data
associations since that 'registry' is stored in etcd). Also deletes cached
kernel modules. Also deletes certificates.

Online help: https://docs.dotmesh.com/references/cli/#remove-dotmesh-from-your-node-dm-cluster-reset`,
		Run: func(cmd *cobra.Command, args []string) {
			err := clusterReset(cmd, args, out)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				os.Exit(1)
			}
		},
	}
	return cmd
}

func clusterUpgrade(cmd *cobra.Command, args []string, out io.Writer) error {
	fmt.Printf("Upgrading local Dotmesh server to version %s (docker image %s)\n", clientVersion, dotmeshDockerImage)

	if !offline {
		fmt.Printf("Pulling dotmesh-server docker image...\n")
		resp, err := exec.Command(
			"docker", "pull", dotmeshDockerImage,
		).CombinedOutput()
		if err != nil {
			fmt.Printf("response: %s\n", resp)
			return err
		}
		fmt.Printf("done.\n")
	}
	fmt.Printf("Stopping dotmesh-server...")
	resp, err := exec.Command(
		"docker", "rm", "-f", "dotmesh-server",
	).CombinedOutput()
	if err != nil {
		fmt.Printf("error, attempting to continue: %s\n", resp)
	} else {
		fmt.Printf("done.\n")
	}
	fmt.Printf("Stopping dotmesh-server-inner...")
	resp, err = exec.Command(
		"docker", "rm", "-f", "dotmesh-server-inner",
	).CombinedOutput()
	if err != nil {
		fmt.Printf("error, attempting to continue: %s\n", resp)
	} else {
		fmt.Printf("done.\n")
	}

	pkiPath := getPkiPath()
	fmt.Printf("Starting dotmesh server... ")
	err = startDotmeshContainer(pkiPath)
	if err != nil {
		return err
	}
	fmt.Printf("done.\n")
	return nil
}

func clusterCommonPreflight() error {
	// - Pre-flight check, can I exec docker? Is it new enough (v1.10.0+)?
	startTiming()
	fmt.Printf("Checking suitable Docker is installed... ")
	clientVersion, err := exec.Command(
		"docker", "version", "-f", "{{.Client.Version}}",
	).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", clientVersion)
		return err
	}
	v1_10_0, err := semver.Make("1.10.0")
	if err != nil {
		return err
	}
	cv, err := semver.Make(strings.TrimSpace(string(clientVersion)))
	if err != nil {
		fmt.Printf("assuming post-semver Docker client is sufficient.\n")
	} else {
		if cv.LT(v1_10_0) {
			return fmt.Errorf("Docker client version is < 1.10.0, please upgrade")
		}
	}

	serverVersion, err := exec.Command(
		"docker", "version", "-f", "{{.Server.Version}}",
	).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", serverVersion)
		return err
	}
	sv, err := semver.Make(strings.TrimSpace(string(serverVersion)))
	if err != nil {
		fmt.Printf("assuming post-semver Docker server is sufficient.\n")
	} else {
		if sv.LT(v1_10_0) {
			return fmt.Errorf("Docker server version is < 1.10.0, please upgrade")
		}
		fmt.Printf("yes, got %s.\n", strings.TrimSpace(string(serverVersion)))
	}

	logTiming("check docker version")

	containers := []string{"dotmesh-etcd", "dotmesh-server", "dotmesh-server-inner"}

	fmt.Printf("Checking dotmesh isn't running...\n")
	// - Are all the containers running?
	//   If yes, exit: We're good already.
	anyContainersMissing := false
	anyContainersRunning := false
	for _, c := range containers {
		ret, err := returnCode("docker", "inspect", "--type=container", c)
		if err != nil {
			return err
		}
		if ret == 0 {
			fmt.Printf("  * %s: is running\n", c)
			anyContainersRunning = true
		} else {
			fmt.Printf("  * %s: is not running\n", c)
			anyContainersMissing = true
		}
	}
	fmt.Printf("done.\n")

	if !anyContainersMissing {
		return fmt.Errorf("Dotmesh is already running!")
	}

	logTiming("check dotmesh isn't running")

	if anyContainersRunning {
		fmt.Printf("Terminating old containers... ")

		for _, c := range containers {
			_, err := returnCode("docker", "rm", "-f", c)
			if err != nil {
				return err
			}
			// Ignore `docker rm -f` errors, as not ALL the containers might have been running.
		}
		fmt.Printf("done.\n")
		logTiming("stop existing containers")
	}

	if !offline {
		fmt.Printf("Pulling dotmesh-server docker image...\n")
		resp, err := exec.Command(
			"docker", "pull", dotmeshDockerImage,
		).CombinedOutput()
		if err != nil {
			fmt.Printf("response: %s\n", resp)
			return err
		}
		fmt.Printf("done.\n")
	}
	logTiming("pull dotmesh-server docker image")
	dumpTiming()
	return nil
}

func getHostFromEnv() string {
	// use DOCKER_HOST as a hint as to where the "local" dotmesh will be
	// running, from the PoV of the client
	// cases handled:
	// - DOCKER_HOST is unset: use localhost (e.g. docker on Linux, docker for
	// Mac)
	// - DOCKER_HOST=tcp://192.168.99.101:2376: parse out the bit between the
	// '://' and the second ':', because this may be a docker-machine
	// environment
	dockerHost := os.Getenv("DOCKER_HOST")
	if dockerHost == "" {
		return "127.0.0.1"
	} else {
		return strings.Split(strings.Split(dockerHost, "://")[1], ":")[0]
	}
}

func transportFromTLS(certFile, keyFile, caFile string) (*http.Transport, error) {
	// Load client cert
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}
	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	return transport, nil
}

func getEtcd() (client.KeysAPI, error) {
	// attempt to connect to etcd and set the admin password for the first time
	transport, err := transportFromTLS(
		getPkiPath()+"/apiserver.pem",
		getPkiPath()+"/apiserver-key.pem",
		getPkiPath()+"/ca.pem",
	)
	if err != nil {
		return nil, err
	}
	cfg := client.Config{
		Endpoints: []string{fmt.Sprintf("https://%s:42379", getHostFromEnv())},
		Transport: transport,
		// set timeout per request to fail fast when the target endpoint is
		// unavailable
		HeaderTimeoutPerRequest: time.Second * 30,
	}
	c, err := client.New(cfg)
	if err != nil {
		return nil, err
	}
	return client.NewKeysAPI(c), nil
}

func getToken() (string, error) {
	kapi, err := getEtcd()
	if err != nil {
		return "", err
	}
	encoded, err := kapi.Get(
		context.Background(),
		fmt.Sprintf("/dotmesh.io/users/%s", ADMIN_USER_UUID),
		nil,
	)
	if err != nil {
		return "", err
	}
	// just extract the field we need
	var s struct{ ApiKey string } // This MUST be a subset of the type User defined in cmd/dotmesh-server/pkg/main/types.go
	err = json.Unmarshal([]byte(encoded.Node.Value), &s)
	if err != nil {
		return "", err
	}
	return s.ApiKey, nil
}

func setAuthTokens(adminPassword, adminKey string) error {
	salt := make([]byte, SALT_BYTES)
	_, err := rand.Read(salt)

	if err != nil {
		return err
	}

	hashedPassword, err := scrypt.Key([]byte(adminPassword), salt, SCRYPT_N, SCRYPT_R, SCRYPT_P, HASH_BYTES)

	kapi, err := getEtcd()
	if err != nil {
		return err
	}
	// This MUST be a subset of the type User defined in cmd/dotmesh-server/pkg/main/types.go
	user := struct {
		Id       string
		Name     string
		Salt     []byte
		Password []byte
		ApiKey   string
	}{Id: ADMIN_USER_UUID, Name: "admin", Salt: salt, Password: hashedPassword, ApiKey: adminKey}
	encoded, err := json.Marshal(user)
	if err != nil {
		return err
	}
	_, err = kapi.Set(
		context.Background(),
		fmt.Sprintf("/dotmesh.io/users/%s", ADMIN_USER_UUID),
		string(encoded),
		&client.SetOptions{},
	)
	return err
}

func guessHostIPv4Addresses() ([]string, error) {
	// XXX this will break if the node's IP address changes
	ip, err := exec.Command(
		"docker", "run", "--rm", "--net=host",
		dotmeshDockerImage,
		"dotmesh-server", "--guess-ipv4-addresses",
	).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", ip)
		return []string{}, err
	}
	ipAddr := strings.TrimSpace(string(ip))
	return strings.Split(ipAddr, ","), nil
}

func guessHostname() (string, error) {
	hostname, err := exec.Command(
		"docker", "run", "--rm", "--net=host",
		dotmeshDockerImage,
		"hostname",
	).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", hostname)
		return "", err
	}
	hostnameString := strings.TrimSpace(string(hostname))
	return hostnameString, nil
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func startDotmeshContainer(pkiPath string) error {
	if traceAddr != "" {
		fmt.Printf("Trace address: %s\n", traceAddr)
	}
	if logAddr != "" {
		fmt.Printf("Log address: %s\n", logAddr)
	}

	args := []string{
		"run", "--restart=always",
		"--privileged", "--pid=host", "--net=host",
		"-d", "--name=dotmesh-server",
		"-v", "/lib:/system-lib/lib",
		"-v", "dotmesh-kernel-modules:/bundled-lib",
		// so that we can create /var/lib/dotmesh in require_zfs.sh and have
		// it manifest on the host so that ZFS in the kernel can find the path
		// that the zpool command passes it!
		"-v", "/var/lib:/var/lib",
		"-v", "/run/docker:/run/docker",
		"-v", "/var/run/docker.sock:/var/run/docker.sock",
		// Find bundled zfs bins and libs if exists
		"-e", "PATH=/bundled-lib/sbin:/usr/local/sbin:/usr/local/bin:" +
			"/usr/sbin:/usr/bin:/sbin:/bin",
		"-e", "LD_LIBRARY_PATH=/bundled-lib/lib:/bundled-lib/usr/lib/",
		// Allow tests to specify which pool to create and where.
		"-e", fmt.Sprintf("USE_POOL_NAME=%s", usePoolName),
		"-e", fmt.Sprintf("USE_POOL_DIR=%s", usePoolDir),
		// In case the docker daemon is older than the bundled docker client in
		// the dotmesh-server image, at least allow the user to instruct it to
		// fall back to an older API version.
		"-e", fmt.Sprintf("DOCKER_API_VERSION=%s", dockerApiVersion),
		// Allow centralized tracing and logging.
		"-e", fmt.Sprintf("TRACE_ADDR=%s", traceAddr),
		"-e", fmt.Sprintf("LOG_ADDR=%s", logAddr),
		// Set env var so that sub-container executor can bind-mount the right
		// certificates in.
		"-e", fmt.Sprintf("PKI_PATH=%s", maybeEscapeLinuxEmulatedPathOnWindows(pkiPath)),
		// And know their own identity, so they can respawn.
		"-e", fmt.Sprintf("DOTMESH_DOCKER_IMAGE=%s", dotmeshDockerImage),
		"-e", fmt.Sprintf("DOTMESH_UPGRADES_URL=%s", checkpointUrl),
		"-e", fmt.Sprintf("DOTMESH_UPGRADES_INTERVAL_SECONDS=%d", checkpointInterval),
	}

	// inject the inherited env variables from the context of the dm binary into require_zfs.sh
	for _, envName := range inheritedEnvironment {
		args = append(args, "-e", fmt.Sprintf("%s=%s", envName, os.Getenv(envName)))
	}

	if usePoolDir != "" {
		args = append(args, []string{"-v", fmt.Sprintf("%s:%s", usePoolDir, usePoolDir)}...)
	}
	args = append(args, []string{
		dotmeshDockerImage,
		// This attempts to download ZFS modules (if necc.) and modprobe them
		// on the Docker host before starting a second container which runs the
		// actual dotmesh-server with an rshared bind-mount of /var/pool.
		"/require_zfs.sh", "dotmesh-server",
	}...)
	fmt.Fprintf(logFile, "docker %s\n", strings.Join(args, " "))
	resp, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", resp)
		return err
	}
	return nil
}

// exists returns whether the given file or directory exists or not
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func clusterCommonSetup(clusterUrl, adminPassword, adminKey, pkiPath string) error {
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

	if adminPassword != "" && adminKey != "" {
		// we are to try and initialize the first admin passwords
		// try 10 times with exponentially increasing delay in between.
		// panic if the adminPassword already exists
		delay := 1
		var err error
		for i := 1; i <= 10; i++ {
			time.Sleep(time.Duration(delay) * 5 * time.Second)
			err = setAuthTokens(adminPassword, adminKey)
			if err == nil {
				path, _ := filepath.Split(configPath)
				passwordPath := filepath.Join(path, "admin-password.txt")
				fmt.Printf(
					"Succeeded setting initial admin password to '%s' - writing it to %s\n",
					adminPassword,
					passwordPath,
				)

				// Delete any previous admin password; ignore errors as we
				// don't care if it wasn't there, and if there's something
				// more exotic wrong with the filesystem (eg, permissions,
				// IO error) the attempt to write will report it:
				os.Remove(passwordPath)

				// Mode 0600 to make it owner-only
				err = ioutil.WriteFile(passwordPath, []byte(adminPassword), 0600)

				if err == nil {
					// Try to limit that to 0400 (read-only) now we've written it
					err2 := unix.Chmod(passwordPath, 0400)
					if err2 != nil {
						// Non-fatal error
						fmt.Printf("WARNING: Could not make admin password file %s read-only: %v\n", passwordPath, err2)
					}
				}
				break
			}
			delay *= 2
			fmt.Printf(
				"Can't set initial admin password yet (%s), retrying in %ds...\n",
				err, delay,
			)
		}
		if err != nil {
			return err
		}
	} else {
		// GET adminKey from our local etcd
		// TODO refactor tryUntil
		delay := 1
		var err error
		for i := 1; i <= 10; i++ {
			time.Sleep(time.Duration(delay) * 5 * time.Second)
			adminKey, err = getToken()
			if err == nil {
				fmt.Printf(
					"Succeeded getting initial admin API key '%s'\n",
					adminKey,
				)
				break
			}
			delay *= 2
			fmt.Printf(
				"Can't get initial admin API key yet (%s), retrying in %ds...\n",
				err, delay,
			)
		}
		if err != nil {
			return err
		}
	}
	// set the admin password in our Configuration
	fmt.Printf("Configuring dm CLI to authenticate to dotmesh server %s... ", configPath)
	config, err := remotes.NewConfiguration(configPath)
	if err != nil {
		return err
	}
	if config.RemoteExists("local") {
		fmt.Printf("Removing old 'local' remote... ")
		err = config.RemoveRemote("local")
		if err != nil {
			return err
		}
	}
	err = config.AddRemote("local", "admin", getHostFromEnv(), adminKey)
	if err != nil {
		return err
	}
	err = config.SetCurrentRemote("local")
	if err != nil {
		return err
	}
	fmt.Printf("done.\n")

	// - Start dotmesh-server.
	fmt.Printf("Starting dotmesh server... ")
	err = startDotmeshContainer(pkiPath)
	if err != nil {
		return err
	}
	fmt.Printf("done.\n")
	fmt.Printf("Waiting for dotmesh server to come up")
	connected := false
	try := 0
	for !connected {
		try++
		connected = func() bool {
			dm, err := remotes.NewDotmeshAPI(configPath)
			e := func() {
				if try == 4*120 { // 120 seconds (250ms per try)
					fmt.Printf(
						"\nUnable to connect to dotmesh server after 120s, " +
							"please run `docker logs dotmesh-server` " +
							"and paste the result into an issue at " +
							"https://github.com/dotmesh-io/dotmesh/issues/new\n")
				}
				fmt.Printf(".")
				time.Sleep(250 * time.Millisecond)
			}
			if err != nil {
				e()
				return false
			}
			var response bool
			response, err = dm.PingLocal()
			if err != nil {
				e()
				return false
			}
			if !response {
				e()
			}
			fmt.Printf("\n")
			return response
		}()
	}
	fmt.Printf("done.\n")
	return nil
}

func clusterReset(cmd *cobra.Command, args []string, out io.Writer) error {
	// TODO this should gather a _list_ of errors, not just at-most-one!
	var bailErr error

	fmt.Printf("Destroying all dotmesh data... ")
	resp, err := exec.Command(
		"docker", "exec", "dotmesh-server-inner",
		"zfs", "destroy", "-r", "pool",
	).CombinedOutput()
	if err != nil {
		if strings.Contains(string(resp), "dataset is busy") {
			return fmt.Errorf("unable to destroy zfs pool because it was busy, please ensure all containers using dotmesh volumes are deleted and then try again; use dm list to see them: %v", string(resp))
		} else {
			fmt.Printf("response: %s\n", resp)
			bailErr = err
		}
	}
	fmt.Printf("done.\n")

	// TODO consider wiping out /var/run/docker/dotmesh/* including mnt
	// TODO investigate why /var/run/docker/dotmesh/mnt can ever have non-zfs
	// mounted directories in it

	fmt.Printf("Deleting dotmesh-etcd container... ")
	resp, err = exec.Command(
		"docker", "rm", "-v", "-f", "dotmesh-etcd",
	).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", resp)
		bailErr = err
	}
	fmt.Printf("done.\n")
	fmt.Printf("Deleting dotmesh-server containers... ")
	resp, err = exec.Command(
		"docker", "rm", "-v", "-f", "dotmesh-server",
	).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", resp)
		bailErr = err
	}
	fmt.Printf("done.\n")
	fmt.Printf("Deleting dotmesh-server-inner containers... ")
	resp, err = exec.Command(
		"docker", "rm", "-v", "-f", "dotmesh-server-inner",
	).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", resp)
		bailErr = err
	}
	fmt.Printf("done.\n")

	// - Delete dotmesh socket
	fmt.Printf("Deleting dotmesh socket... ")
	resp, err = exec.Command(
		"rm", "-f", "/run/docker/plugins/dm.sock",
	).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", resp)
		bailErr = err
	}
	fmt.Printf("done.\n")

	fmt.Printf("Deleting dotmesh-etcd-data local volume... ")
	resp, err = exec.Command(
		"docker", "volume", "rm", "dotmesh-etcd-data",
	).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", resp)
		bailErr = err
	}
	fmt.Printf("done.\n")
	fmt.Printf("Deleting dotmesh-kernel-modules local volume... ")
	resp, err = exec.Command(
		"docker", "volume", "rm", "dotmesh-kernel-modules",
	).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", resp)
		bailErr = err
	}
	fmt.Printf("done.\n")
	fmt.Printf("Deleting 'local' remote... ")
	config, err := remotes.NewConfiguration(configPath)
	if err != nil {
		fmt.Printf("response: %s\n", resp)
		bailErr = err
	} else {
		err = config.RemoveRemote("local")
		if err != nil {
			fmt.Printf("response: %s\n", resp)
			bailErr = err
		}
	}
	fmt.Printf("done.\n")
	fmt.Printf("Deleting cached PKI assets... ")
	pkiPath := getPkiPath()
	clientVersion, err := exec.Command(
		"rm", "-rf", pkiPath,
	).CombinedOutput()
	if err != nil {
		fmt.Printf("response: %s\n", clientVersion)
		bailErr = err
	}
	fmt.Printf("done.\n")
	if bailErr != nil {
		return bailErr
	}
	return nil
}

func generatePkiJsonEncoded(pkiPath string) (string, error) {
	v := url.Values{}
	resultMap := map[string]string{}
	files, err := ioutil.ReadDir(pkiPath)
	if err != nil {
		return "", err
	}
	for _, file := range files {
		name := file.Name()
		c, err := ioutil.ReadFile(pkiPath + "/" + name)
		if err != nil {
			return "", err
		}
		resultMap[name] = string(c)
	}
	j, err := json.Marshal(resultMap)
	if err != nil {
		return "", err
	}
	v.Set("value", string(j))
	return v.Encode(), nil
}

func getPkiPath() string {
	dirPath := filepath.Dir(configPath)
	pkiPath := dirPath + "/pki"
	return pkiPath
}

func maybeEscapeLinuxEmulatedPathOnWindows(path string) string {
	// If the 'dm' client is running on Windows in WSL (Windows Subsystem for
	// Linux), and the Linux docker client is installed in the WSL environment,
	// and Docker for Windows is installed, we need to escape the WSL chroot
	// path, before being passed to Docker as a Windows path. E.g.
	//
	// /home/$USER/.dotmesh/pki
	//   ->
	// C:/Users/$USER/AppData/Local/lxss/home/$USER/.dotmesh/pki
	//
	// We can determine whether this is necessary by reading /proc/version
	// https://github.com/Microsoft/BashOnWindows/issues/423#issuecomment-221627364

	version, err := ioutil.ReadFile("/proc/version")
	if err != nil {
		if os.IsNotExist(err) {
			// normal on macOS
			return path
		} else {
			panic(err)
		}
	}
	user, err := exec.Command("whoami").CombinedOutput()
	if err != nil {
		panic(err)
	}
	if strings.Contains(string(version), "Microsoft") {
		// In test environment, user was 'User' and Linux user was 'user'.
		// Hopefully lowercasing is the only transformation.  Hopefully on the
		// Windows (docker server) side, the path is case insensitive!
		return "/c/Users/" + strings.TrimSpace(string(user)) + "/AppData/Local/lxss" + path
	}
	return path
}

func generatePKI(extantCA bool) error {
	// TODO add generatePKI(true) after getting PKI material from discovery
	// server
	ipAddrs, err := guessHostIPv4Addresses()
	if err != nil {
		return err
	}
	hostname, err := guessHostname()
	if err != nil {
		return err
	}
	advertise := []string{getHostFromEnv()}
	advertise = append(advertise, ipAddrs...)
	pkiPath := getPkiPath()
	_, _, err = pki.CreatePKIAssets(pkiPath, &pki.Configuration{
		AdvertiseAddresses: advertise,
		ExternalDNSNames:   []string{"dotmesh-etcd", "localhost", hostname}, // TODO allow arg
		ExtantCA:           extantCA,
	})
	if err != nil {
		return err
	}
	return nil
}

func clusterInit(cmd *cobra.Command, args []string, out io.Writer) error {
	// - Run clusterCommonPreflight.
	err := clusterCommonPreflight()
	if err != nil {
		return err
	}
	fmt.Printf("Registering new cluster... ")
	// - Get a unique cluster id by asking discovery.dotmesh.io.
	// support specifying size here (to avoid cliques/enable HA)
	resp, err := http.Get(fmt.Sprintf("%s/new?size=%d", discoveryUrl, serverCount))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Printf("got URL:\n%s\n", string(body))

	// - Generate PKI material, and upload it to etcd at hidden clusterSecret

	pkiPath := getPkiPath()
	_, err = os.Stat(pkiPath)
	switch {
	case err == nil:
		fmt.Printf(
			"PKI directory already exists at %s, using existing credentials.\n", pkiPath,
		)
		fmt.Printf(
			"If you want to completely recreate your cluster with fresh ones, run\n",
		)
		fmt.Printf(
			"`dm cluster reset` then re-run this command.\n",
		)

	case os.IsNotExist(err):
		fmt.Printf("Generating PKI assets... ")
		err = os.Mkdir(pkiPath, 0700)
		if err != nil {
			return err
		}
		err = generatePKI(false)
		fmt.Printf("done.\n")

		// - Upload all PKI assets to discovery.dotmesh.io under "secure" path
		pkiJsonEncoded, err := generatePkiJsonEncoded(pkiPath)
		if err != nil {
			return err
		}
		clusterSecret, err := RandToken(32)
		if err != nil {
			return err
		}
		putPath := fmt.Sprintf("%s/_secrets/_%s", string(body), clusterSecret)

		req, err := http.NewRequest("PUT", putPath, bytes.NewBufferString(pkiJsonEncoded))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		client := &http.Client{}
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()

		_, err = ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		//fmt.Printf("Response: %s\n", r)

		fmt.Printf(
			"If you want more than one node in your cluster, run this on other nodes:\n\n"+
				"    dm cluster join %s:%s\n\n"+
				"This is the last time this secret will be printed, so keep it safe!\n\n",
			string(body), clusterSecret,
		)
		if serverCount > 1 {
			fmt.Printf("=====================================================================\n" +
				"You specified --count > 1, you'll need to run this on the other nodes\n" +
				"immediately, before the following setup will complete.\n" +
				"=====================================================================\n",
			)
		}
	default:
		return err
	}

	// - Generate admin creds, and insert them into etcd
	adminPassword, err := RandToken(32)
	if err != nil {
		return err
	}

	adminKey, err := RandToken(32)
	if err != nil {
		return err
	}

	// - Run clusterCommonSetup.
	err = clusterCommonSetup(
		strings.TrimSpace(string(body)), adminPassword, adminKey, pkiPath,
	)
	if err != nil {
		return err
	}
	return nil
}

func clusterJoin(cmd *cobra.Command, args []string, out io.Writer) error {
	// - Run clusterCommonPreflight.
	err := clusterCommonPreflight()
	if err != nil {
		return err
	}
	// - Require unique cluster id and secret to be specified.
	if len(args) != 1 {
		return fmt.Errorf("Please specify <cluster-url>:<secret> as argument.")
	}
	// - Download PKI assets
	fmt.Printf("Downloading PKI assets... ")
	shrapnel := strings.Split(args[0], ":")
	clusterUrlPieces := []string{}
	// construct the 'https://host:port/path' (clusterUrl) from
	// 'https://host:port/path:secret' and save 'secret' to clusterSecret
	for i := 0; i < len(shrapnel)-1; i++ {
		clusterUrlPieces = append(clusterUrlPieces, shrapnel[i])
	}
	clusterUrl := strings.Join(clusterUrlPieces, ":")
	clusterSecret := shrapnel[len(shrapnel)-1]

	adminPassword := "" // aka "don't attempt to set it in etcd"
	adminKey := ""      // aka "don't attempt to set it in etcd"
	pkiPath := getPkiPath()
	// Now get PKI assets from discovery service.
	// TODO: discovery service should mint new credentials just for us, rather
	// than handing us the keys to the kingdom.
	// https://github.com/dotmesh-io/dotmesh/issues/21
	//fmt.Printf("clusterUrl: %s\n", clusterUrl)
	getPath := fmt.Sprintf("%s/_secrets/_%s", clusterUrl, clusterSecret)
	//fmt.Printf("getPath: %s\n", getPath)
	resp, err := http.Get(getPath)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	//fmt.Printf(string(body))
	var etcdNode map[string]interface{}
	err = json.Unmarshal(body, &etcdNode)
	if err != nil {
		return err
	}
	var filesContents map[string]string
	err = json.Unmarshal(
		[]byte(
			(etcdNode["node"].(map[string]interface{}))["value"].(string),
		), &filesContents,
	)
	//fmt.Printf("===\nfilesContents is %s\n===\n", filesContents)
	// first check whether the directory exists
	if _, err := os.Stat(pkiPath); err == nil {
		return fmt.Errorf(
			"PKI already exists at %s, refusing to proceed. Run 'sudo dm cluster reset' to clean up.",
			pkiPath,
		)
	}
	if _, err := os.Stat(pkiPath + ".tmp"); err == nil {
		return fmt.Errorf(
			"PKI already exists at %s, refusing to proceed. "+
				"Delete the stray temporary directory and run 'sudo dm cluster reset' to try again.",
			pkiPath+".tmp",
		)
	}

	err = os.MkdirAll(pkiPath+".tmp", 0700)
	if err != nil {
		return err
	}
	for filename, contents := range filesContents {
		err = ioutil.WriteFile(pkiPath+".tmp/"+filename, []byte(contents), 0600)
		if err != nil {
			return err
		}
	}
	err = os.Rename(pkiPath+".tmp", pkiPath)
	if err != nil {
		return err
	}
	err = generatePKI(true)
	if err != nil {
		return err
	}
	fmt.Printf("done!\n")
	// - Run clusterCommonSetup.
	err = clusterCommonSetup(clusterUrl, adminPassword, adminKey, pkiPath)
	if err != nil {
		return err
	}
	return nil
}

func returnCode(name string, arg ...string) (int, error) {
	// Run a command and either get the returncode or an error if the command
	// failed to execute, based on
	// http://stackoverflow.com/questions/10385551/get-exit-code-go
	cmd := exec.Command(name, arg...)
	if err := cmd.Start(); err != nil {
		return -1, err
	}
	if err := cmd.Wait(); err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			// The program has exited with an exit code != 0
			// This works on both Unix and Windows. Although package
			// syscall is generally platform dependent, WaitStatus is
			// defined for both Unix and Windows and in both cases has
			// an ExitStatus() method with the same signature.
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				return status.ExitStatus(), nil
			}
		} else {
			return -1, err
		}
	}
	// got here, so err == nil
	return 0, nil
}
