package citools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/rpc/v2/json2"
)

var DEBUG_ENV = map[string]string{"DEBUG_MODE": "1"}

// props to https://github.com/kubernetes/kubernetes/issues/49387
var KUBE_DEBUG_CMD = `(
echo 'SEARCHABLE HEADER: KUBE DEBUG'
echo ' _  ___   _ ____  _____   ____  _____ ____  _   _  ____ '
echo '| |/ / | | | __ )| ____| |  _ \| ____| __ )| | | |/ ___|'
echo "| ' /| | | |  _ \|  _|   | | | |  _| |  _ \| | | | |  _ "
echo '| . \| |_| | |_) | |___  | |_| | |___| |_) | |_| | |_| |'
echo '|_|\_\\___/|____/|_____| |____/|_____|____/ \___/ \____|'
echo '                                                        '

for INTERESTING_POD in $(kubectl get pods --all-namespaces --no-headers \
		|grep -v Running | tr -s ' ' |cut -d ' ' -f 1,2,4 |tr ' ' '/'); do
	NS=$(echo $INTERESTING_POD |cut -d "/" -f 1)
	NAME=$(echo $INTERESTING_POD |cut -d "/" -f 2)
	PHASE=$(echo $INTERESTING_POD |cut -d "/" -f 3)
	echo "--> status of $INTERESTING_POD"
	kubectl describe pod $NAME -n $NS
	if [ "$PHASE" != "ContainerCreating" ]; then
		for CONTAINER in $(kubectl get pods $NAME -n $NS -o \
				jsonpath={.spec.containers[*].name}); do
			echo "--> logs of $INTERESTING_POD/$CONTAINER"
			kubectl logs --tail 10 $NAME -n $NS $CONTAINER
		done
	fi

	if [ "$PHASE" == "ImagePullBackOff" ]; then
		echo "QUITTING, image could not be found for pod ${INTERESTING_POD}"
		echo "IMAGEPULLBACKOFF Describe output -->"
		kubectl describe $INTERESTING_POD -n $NS
		exit 1
	fi

	if [ "$PHASE" == "CrashLoopBackOff" ]; then
		echo "WARNING crashLoopBackOff, will try again then exit."
		kubectl describe $INTERESTING_POD -n $NS
	fi

	if [ "$PHASE" == "Evicted" ]; then
		echo "WARNING evicted, will try again then exit."
		kubectl describe $INTERESTING_POD -n $NS
	fi

	if [ "$PHASE" == "CreateContainerConfigError" ]; then
		echo "QUITTING, Create Container config error for pod ${INTERESTING_POD}"
		echo "CREATECONTAINERCONFIGERROR Describe output -->"
		kubectl describe $INTERESTING_POD -n $NS
		exit 1
	fi
done
kubectl get pods --all-namespaces
echo '    ___  ___   _ ____  _____   ____  _____ ____  _   _  ____ '
echo '   / / |/ / | | | __ )| ____| |  _ \| ____| __ )| | | |/ ___|'
echo "  / /| ' /| | | |  _ \|  _|   | | | |  _| |  _ \| | | | |  _ "
echo ' / / | . \| |_| | |_) | |___  | |_| | |___| |_) | |_| | |_| |'
echo '/_/  |_|\_\\___/|____/|_____| |____/|_____|____/ \___/ \____|'
echo '                                                             '

exit 0)` // never let the debug command failing cause us to fail the tests!

// return a list of names of pods which are crashing. If there are some, exit 1.
var checkCrashLoopCmd = `(
	kubectl get pod --all-namespaces -o json | jq '.items[] | if .status.phase == "CrashLoopBackOff" then .metadata.name else empty end'
	if [ ! -z $result ]; then
		echo $result
		exit 1
	fi
	exit 0)`

var timings map[string]float64
var lastTiming int64

// An arbitrary point in time that all test runs after this commit
// should be after, so we can use it as an epoch for our timestamps
// and therefore make them much shorter strings... Update this every
// decade or so to keep them short.
const DAWN_OF_DOTMESH_TIME = 1533034862684223659

var stamp int64

// DOTMESH_TEST_CLEANUP_ENV - cleanup policy for the tests
var DOTMESH_TEST_CLEANUP_ENV = "DOTMESH_TEST_CLEANUP"

type cleanupStrategy int

const (
	cleanupStrategyNone cleanupStrategy = iota
	cleanupStrategyAlways
	cleanupStrategyNever
	cleanupStrategyOnSuccess
	cleanupStrategyLater
)

var defaultCleanupStrategy = cleanupStrategyOnSuccess

func getCleanupStrategy() cleanupStrategy {
	c := strings.ToLower(os.Getenv(DOTMESH_TEST_CLEANUP_ENV))
	switch c {
	case "always":
		return cleanupStrategyAlways
	case "never":
		return cleanupStrategyNever
	case "onsuccess":
		return cleanupStrategyOnSuccess
	case "later":
		return cleanupStrategyLater
	}

	return defaultCleanupStrategy
}

var getFieldsByNewLine = func(c rune) bool {
	return c == '\n'
}

func Contains(arr []string, str string) bool {
	for _, a := range arr {
		if strings.Contains(a, str) {
			return true
		}
	}
	return false
}

func AddFuncToCleanups(f func()) {
	globalCleanupFuncs = append(globalCleanupFuncs, f)
}

var globalCleanupFuncs []func()

func StartTiming() {
	lastTiming = time.Now().UnixNano()
	timings = make(map[string]float64)
}

func LogTiming(tag string) {
	now := time.Now().UnixNano()
	timings[tag] = float64(now-lastTiming) / (1000 * 1000 * 1000)
	lastTiming = now
}

func DumpTiming() {
	fmt.Printf("=== TIMING ===\n")
	for tag, timing := range timings {
		fmt.Printf("%s => %.2f\n", tag, timing)
	}
	fmt.Printf("=== END TIMING ===\n")
	timings = map[string]float64{}
}

func System(cmd string, args ...string) error {
	log.Printf("[system] running %s %s", cmd, args)
	c := exec.Command(cmd, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Run()
}

func SilentSystem(cmd string, args ...string) error {
	log.Printf("[silentSystem] running %s %s", cmd, args)
	c := exec.Command(cmd, args...)
	return c.Run()
}

func TryUntilSucceeds(f func() error, desc string) error {
	attempt := 0
	for {
		err := f()
		if err != nil {
			if attempt > 20 {
				return err
			} else {
				fmt.Printf("Error %s: %v, pausing and trying again...\n", desc, err)
				time.Sleep(time.Duration(attempt) * time.Second)
			}
		} else {
			return nil
		}
		attempt++
	}
}

func TryForever(f func() error, desc string) error {
	attempt := 0
	for {
		err := f()
		if err != nil {
			fmt.Printf("Error %s: %v, pausing and trying again...\n", desc, err)
			time.Sleep(time.Duration(attempt) * time.Second)
		} else {
			return nil
		}
		attempt++
	}
}

func TestMarkForCleanup(f Federation) {
	log.Printf(`Entering TestMarkForCleanup:
  ____ _     _____    _    _   _ ___ _   _  ____   _   _ ____  
 / ___| |   | ____|  / \  | \ | |_ _| \ | |/ ___| | | | |  _ \ 
| |   | |   |  _|   / _ \ |  \| || ||  \| | |  _  | | | | |_) |
| |___| |___| |___ / ___ \| |\  || || |\  | |_| | | |_| |  __/ 
 \____|_____|_____/_/   \_\_| \_|___|_| \_|\____|  \___/|_|    
                                                               
`)
	for _, c := range f {
		for _, n := range c.GetNodes() {
			node := n.Container
			err := TryUntilSucceeds(func() error {
				return System("bash", "-c", fmt.Sprintf(
					`docker exec -t %s bash -c 'touch /CLEAN_ME_UP'`, node,
				))
			}, fmt.Sprintf("marking %s for cleanup", node))
			if err != nil {
				log.Printf("Error marking %s for cleanup: %s, giving up.\n", node, err)
			} else {
				log.Printf("Marked %s for cleanup.", node)
			}
		}
	}

	// Attempt log extraction only after we've safely touched all those CLEAN_ME_UP files, *phew*.
	for _, c := range f {
		if !c.isBlank() {
			for _, n := range c.GetNodes() {
				node := n.Container
				containers := []string{"dotmesh-server", "dotmesh-server-inner"}
				for _, container := range containers {
					logDir := "../extracted_logs"
					logFile := fmt.Sprintf(
						"%s/%s-%s.log",
						logDir, container, node,
					)
					err := SilentSystem(
						"bash", "-c",
						fmt.Sprintf(
							"mkdir -p %s && touch %s && chmod -R a+rwX %s && "+
								"docker exec -i %s "+
								"docker logs %s > %s",
							logDir, logFile, logDir, node, container, logFile,
						),
					)
					if err != nil {
						log.Printf("Unable to stream docker logs to artifacts directory for %s: %s", node, err)
					}
				}
			}
		}
	}
}

func testSetup(t *testing.T, f Federation) error {
	err := System("bash", "-c", fmt.Sprintf(`
		# Create a home for the test pools to live that can have the same path
		# both from ZFS's perspective and that of the inner container.
		# (Bind-mounts all the way down.)
		mkdir -p %s
		# tmpfs makes etcd not completely rinse your IOPS (which it can do
		# otherwise); create if doesn't exist
		if [ $(mount |grep "/tmpfs " |wc -l) -eq 0 ]; then
		        mkdir -p /tmpfs && mount -t tmpfs -o size=4g tmpfs /tmpfs
		fi
		# Attempt to mitigate https://github.com/kinvolk/kube-spawn/issues/14#issuecomment-293207134
		echo 131072 > /sys/module/nf_conntrack/parameters/hashsize || true
	`, testDirName(stamp)))
	if err != nil {
		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	dindClusterScriptName := fmt.Sprintf("%s/dind-cluster-%d.sh", cwd, os.Getpid())

	runScriptDir := os.Getenv("DIND_RUN_FROM_PATH")
	if runScriptDir == "" {
		runScriptDir = cwd
	}

	// we write the dind-script.sh file out from go because we need to distribute
	// that .sh script as a go package using dep
	err = ioutil.WriteFile(dindClusterScriptName, []byte(DIND_SCRIPT), 0755)
	if err != nil {
		return err
	}

	// XXX do we actually use this for anything at all???
	dindConfig := `
# Apiserver port
APISERVER_PORT=${APISERVER_PORT:-8080}

# Use prebuilt DIND image
DIND_IMAGE="${DIND_IMAGE:-quay.io/dotmesh/kubeadm-dind-cluster:v1.10}"

# Define which DNS service to run
# possible values are kube-dns (default) and coredns
DNS_SERVICE="${DNS_SERVICE:-kube-dns}"
`
	err = ioutil.WriteFile(fmt.Sprintf("%s/config.sh", runScriptDir), []byte(dindConfig), 0644)
	if err != nil {
		return err
	}

	// dind-script.sh needs config.sh

	// don't leave copies of the script around once we have used it
	defer func() {
		os.Remove(dindClusterScriptName)
	}()

	for i, c := range f {

		clusterIpPrefix := getUniqueIpPrefix()
		c.SetIpPrefix(clusterIpPrefix)

		for j := 0; j < c.GetDesiredNodeCount(); j++ {
			node := nodeName(stamp, i, j)
			fmt.Printf(">>> Using RunArgs %s\n", c.RunArgs(i, j))

			dockerAuthFile := os.Getenv("MOUNT_DOCKER_AUTH")

			mountDockerAuth := ""

			if dockerAuthFile != "" {
				if _, err := os.Stat(dockerAuthFile); err == nil {
					mountDockerAuth = fmt.Sprintf(" -v %s:/root/.dockercfg ", dockerAuthFile)
				}
			}

			hostname, err := os.Hostname()
			if err != nil {
				panic(err)
			}

			// XXX the following only works if overlay is working
			err = TryUntilSucceeds(
				func() error {
					return System("bash", "-c", fmt.Sprintf(`
					set -xe
					MOUNTPOINT=/dotmesh-test-pools
					DOTMESH_MOUNTS=/var/lib/dotmesh-mounts
					mkdir -p $MOUNTPOINT
					mkdir -p $DOTMESH_MOUNTS
					NODE=%s
					if [ $(mount |grep $MOUNTPOINT |wc -l) -eq 0 ]; then
						echo "Creating and bind-mounting shared $MOUNTPOINT"
						mount --bind $MOUNTPOINT $MOUNTPOINT && \
						mount --make-shared $MOUNTPOINT;
					fi
					if [ $(mount |grep $DOTMESH_MOUNTS |wc -l) -eq 0 ]; then
						echo "Creating and bind-mounting shared $DOTMESH_MOUNTS"
						mount --bind $DOTMESH_MOUNTS $DOTMESH_MOUNTS && \
						mount --make-shared $DOTMESH_MOUNTS;
					fi
					(cd %s
						EXTRA_DOCKER_ARGS="\
							-v /var/lib/dotmesh-mounts:/var/lib/dotmesh-mounts:rshared \
							-v /dotmesh-test-pools:/dotmesh-test-pools:rshared \
							-v /var/run/docker.sock:/hostdocker.sock %s " \
						DIND_SUBNET="10.200.0.0" \
						DIND_SUBNET_SIZE="16" \
						SERVICE_CIDR="%s" \
						POD_NETWORK_CIDR="%s" \
						CNI_PLUGIN=bridge %s run $NODE "%s" %d)
					sleep 1
					echo "About to run docker exec on $NODE"
					docker exec -t $NODE bash -c '
						set -xe
						# from dind::fix-mounts
						mount --make-shared /lib/modules/
						mount --make-shared /run
						echo "%s '$(hostname)'.local" >> /etc/hosts
						echo -n "10.%d." > /POD_IP_PREFIX
						mkdir -p /etc/docker
						echo "{\"insecure-registries\" : [\"%s.local:80\"]}" > /etc/docker/daemon.json
						systemctl daemon-reload
						systemctl restart docker
					'
					ret=$?
					echo "Return code for docker exec was $ret"
					if [[ $ret -ne 0 ]]; then
						# Do it again
						echo "Retrying after 5 seconds..."
						sleep 5
						docker exec -t $NODE bash -c '
							set -xe
							echo "%s '$(hostname)'.local" >> /etc/hosts
							echo -n "10.%d." > /POD_IP_PREFIX
							mkdir -p /etc/docker
							echo "{\"insecure-registries\" : [\"%s.local:80\"]}" > /etc/docker/daemon.json
							systemctl daemon-reload
							systemctl restart docker
						'
					fi
					`, node, runScriptDir, mountDockerAuth,
						// clusterIpPrefix is used here to generate a
						// SERVICE_CIDR...
						serviceCIDR(clusterIpPrefix),
						// ... and a POD_NETWORK_CIDR, which gets broken up
						// into per-node /24s below (j+11)
						podNetworkCIDR(clusterIpPrefix),
						dindClusterScriptName, c.RunArgs(i, j),
						// See also "k+11" elsewhere - this is the per-node pod
						// network subnet, passed as the third argument to
						// dind::run in dind-cluster-patched.sh.
						// This transforms the 10.x.0.0/16 POD_NETWORK_CIDR
						// above into a 10.x.j+11.0/24 pod network sub-range
						// per node.
						j+11,
						hostIpFromContainer(c.GetIpPrefix()), clusterIpPrefix, hostname,
						hostIpFromContainer(c.GetIpPrefix()), clusterIpPrefix, hostname),
					)
				},
				fmt.Sprintf("starting container %s", node),
			)
			if err != nil {
				return err
			}

			RegisterCleanupAction(10, fmt.Sprintf(
				"NODE=%s ; docker exec -i $NODE systemctl stop docker || true; "+
					"docker exec -i $NODE systemctl stop docker || true ; "+
					"docker exec -i $NODE systemctl stop kubelet || true ; "+
					"docker exec -i $NODE systemctl stop systemd-journald || true ; "+
					"docker rm -f $NODE || true", node))

			// as soon as this completes, add it to c.Nodes. more detail gets
			// filled in later (eg dotmesh secrets), but it's important that
			// the basics are in here so that the nodes get marked for cleanup
			// if the setup fails (common w/kubernetes)
			clusterName := fmt.Sprintf("cluster_%d", i)
			c.AppendNode(NodeFromNodeName(t, stamp, i, j, clusterName))

			// if we are testing dotmesh - then the binary under test will have
			// already been created - otherwise, download the latest master build
			// this is to be consistent with LocalImage()
			serviceBeingTested := os.Getenv("CI_SERVICE_BEING_TESTED")
			getDmCommand := fmt.Sprintf("NODE=%s\n", node)
			ciJobId := os.Getenv("CI_JOB_ID")

			if ciJobId == "" {
				ciJobId = "local"
			}

			// if the CI_SERVICE_BEING_TESTED is empty it means we are in local testing mode
			if serviceBeingTested == "dotmesh" || serviceBeingTested == "" {
				// use the dm binary we have as part of the CI build
				getDmCommand += "docker cp ../binaries/Linux/dm $NODE:/usr/local/bin/dm"
			} else {
				// otherwise download the dm binary from our release url
				getDmCommand += fmt.Sprintf(`
					CI_JOB_ID=%s
					curl -L -o /tmp/dm-$CI_JOB_ID https://get.dotmesh.io/unstable/master/Linux/dm
					chmod a+x /tmp/dm-$CI_JOB_ID
					docker cp /tmp/dm-$CI_JOB_ID $NODE:/usr/local/bin/dm
					rm -f /tmp/dm-$CI_JOB_ID
				`, ciJobId)
			}

			err = System("bash", "-c", getDmCommand)
			if err != nil {
				return err
			}

			fmt.Printf("=== Started up %s\n", node)
		}
	}
	return nil
}

type N struct {
	Timestamp  int64
	ClusterNum string
	NodeNum    string
}

// InitialCleanup is called before starting tests
func InitialCleanup() {
	// currently we only remove previous tests if we're using a cleanup strategy that leaves them lurking
	switch getCleanupStrategy() {
	case cleanupStrategyOnSuccess, cleanupStrategyNever, cleanupStrategyNone:
		TeardownFinishedTestRuns()
	case cleanupStrategyAlways:
		// nothing to do
	}
}

// FinalCleanup is called after running all the tests
func FinalCleanup(retcode int) {
	// final cleanup is done based on cleanup strategy. For CI runs
	// it should be set to 'always'
	switch getCleanupStrategy() {
	case cleanupStrategyAlways:
		TeardownThisTestRun()
	case cleanupStrategyOnSuccess:
		if retcode == 0 {
			TeardownThisTestRun()
		} else {
			fmt.Printf("[Final Cleanup] skipping cleanup as tests didn't pass, return code: %d", retcode)
		}
	case cleanupStrategyLater:
		fd, err := os.OpenFile(testDirName(stamp)+"/finished", os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			fmt.Printf("[Final Cleanup] we failed to write the cleanup later file: %s", err.Error())
			return
		}
		fd.Close()
	case cleanupStrategyNever, cleanupStrategyNone:
		// nothing to do
	}
}

func RegisterCleanupAction(phase int, command string) {
	err := System("mkdir",
		"-p",
		testDirName(stamp),
	)

	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile(fmt.Sprintf("%s/cleanup-actions.%02d", testDirName(stamp), phase),
		os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0700)

	if err != nil {
		panic(err)
	}

	defer f.Close()

	fmt.Fprintf(f, "%s\n", command)
}

func TeardownThisTestRun() {
	// run cleanup actions, in order
	err := System("bash", "-c", fmt.Sprintf(`for SCRIPT in %s/cleanup-actions.*; do set -x; . $SCRIPT; done`,
		testDirName(stamp),
	))
	if err != nil {
		fmt.Printf("err cleaning up test resources: %s\n", err)
	}
}

func TeardownFinishedTestRuns() {

	// Handle SIGQUIT and mark tests for cleanup in that case, then immediately
	// exit.

	go func() {
		// From https://golang.org/pkg/os/signal/#Notify
		// Set up channel on which to send signal notifications.
		// We must use a buffered channel or risk missing the signal
		// if we're not ready to receive when the signal is sent.
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGQUIT)
		signal.Notify(c, syscall.SIGINT)
		signal.Notify(c, syscall.SIGTERM)
		signal.Notify(c, syscall.SIGPIPE)

		// Block until a signal is received.
		s := <-c
		log.Printf("Got signal: %s", s)
		for _, f := range globalCleanupFuncs {
			f()
		}
		os.Exit(131)

	}()

	cs, err := exec.Command(
		"bash", "-c", "docker ps |grep cluster- || true",
	).Output()
	if err != nil {
		panic(err)
	}
	fmt.Printf("============\nContainers running before cleanup:\n%s\n============\n", cs)

	defer func() {
		cs, err = exec.Command(
			"bash", "-c", "docker ps |grep cluster- || true",
		).Output()
		if err != nil {
			panic(err)
		}
		fmt.Printf("============\nContainers running after cleanup:\n%s\n============\n", cs)
	}()

	// There maybe other teardown processes running in parallel with this one.
	// Check, and if there are, wait for it to complete and then return.
	lockfile := "/dotmesh-test-cleanup.lock"
	// if path exists, wait until it doesn't and then return.
	if _, err := os.Stat(lockfile); err == nil {
		for {
			log.Printf(
				"Waiting for %s to be deleted "+
					"by some other cleanup process finishing...", lockfile,
			)
			time.Sleep(1 * time.Second)
			if _, err := os.Stat(lockfile); os.IsNotExist(err) {
				return
			}
		}
	}
	// if path doesn't exist, create it and clean it up on return
	if _, err := os.Stat(lockfile); os.IsNotExist(err) {
		f, err := os.Create(lockfile)
		if err != nil {
			return
		}
		if os.Getenv("CI_DOCKER_TAG") != "" {
			// GitLab runner sets this env variable
			_, err = f.WriteString(fmt.Sprintf("Time: %s  Commit: %s\n", time.Now().String(), os.Getenv("CI_DOCKER_TAG")))
			if err != nil {
				log.Printf("Error writing timestamp and commit into the lockfile: %s", err)
			}
		} else {
			// local run
			_, err = f.WriteString(fmt.Sprintf("Time: %s", time.Now().String()))
			if err != nil {
				log.Printf("Error writing timestamp into the lockfile: %s\n", err)
			}
		}
		f.Close()
		defer os.RemoveAll(lockfile)
	}

	// Containers that weren't marked as CLEAN_ME_UP but which are older than
	// an hour, assume they should be cleaned up.
	err = System("../scripts/mark-old-cleanup.sh")
	if err != nil {
		log.Printf("Error running mark-old-cleanup.sh: %s", err)
	}

	cs, err = exec.Command(
		"bash", "-c", "docker ps --format {{.Names}} |grep cluster- || true",
	).Output()
	if err != nil {
		panic(err)
	}
	log.Printf("[TeardownFinishedTestRuns] cs = %s", cs)
	stamps := map[int64][]N{}
	for _, line := range strings.Split(string(cs), "\n") {
		shrap := strings.Split(line, "-")
		if len(shrap) > 4 {
			// cluster-<timestamp>-<clusterNum>-node-<nodeNum>
			stamp := shrap[1]
			clusterNum := shrap[2]
			nodeNum := shrap[4]

			i, err := strconv.ParseInt(stamp, 10, 64)
			if err != nil {
				panic(err)
			}
			_, ok := stamps[i]
			if !ok {
				stamps[i] = []N{}
			}
			stamps[i] = append(stamps[i], N{
				Timestamp:  i,
				ClusterNum: clusterNum,
				NodeNum:    nodeNum,
			})
		}
	}

	for stamp, ns := range stamps {
		func() {
			for _, n := range ns {
				cn, err := strconv.Atoi(n.ClusterNum)
				if err != nil {
					fmt.Printf("can't deduce clusterNum: %d \n", cn)
					return
				}

				nn, err := strconv.Atoi(n.NodeNum)
				if err != nil {
					fmt.Printf("can't deduce nodeNum: %d \n", nn)
					return
				}

				node := nodeName(stamp, cn, nn)

				existsErr := SilentSystem("docker", "inspect", node)
				notExists := false
				if existsErr != nil {
					// must have been a single-node test, don't return on our
					// behalf, we have zpool etc cleanup to do
					notExists = true
				}

				err = System("docker", "exec", "-i", node, "test", "-e", "/CLEAN_ME_UP")
				if err != nil {
					fmt.Printf("not cleaning up %s because /CLEAN_ME_UP not found\n", node)
					if !notExists {
						return
					}
				}
				err = System("docker", "rm", "-f", "-v", node)
				if err != nil {
					fmt.Printf("err during teardown %s\n", err)
				}

				// workaround https://github.com/docker/docker/issues/20398
				err = System("docker", "network", "disconnect", "-f", "bridge", node)
				if err != nil {
					fmt.Printf("err during network force-disconnect %s\n", err)
				}

				// cleanup after a previous test run; this is a pretty gross hack
				err = System("bash", "-c", fmt.Sprintf(`
					for X in $(findmnt -P -R /tmpfs |grep %s); do
						eval $X
						if [ "$TARGET" != "/tmpfs" ]; then
							umount $TARGET >/dev/null 2>&1 || true
						fi
					done
					rm -rf /tmpfs/%s`, node, node),
				)
				if err != nil {
					fmt.Printf("err during teardown %s\n", err)
				}

			}

			// clean up any leftover zpools
			out, err := exec.Command("zpool", "list", "-H").Output()
			if err != nil {
				fmt.Printf("unable to list zpools: %s\n", err)
			}
			shrap := strings.Split(string(out), "\n")
			for _, s := range shrap {
				shr := strings.Fields(string(s))
				if len(shr) > 0 {
					// Manually umount them and disregard failures
					if strings.HasPrefix(shr[0], fmt.Sprintf("testpool-%d", stamp)) {
						o, _ := exec.Command("bash", "-c",
							fmt.Sprintf(
								"for X in `cat /proc/self/mounts|grep testpool-%d"+
									"|grep -v '/mnt '|cut -d ' ' -f 2`; do "+
									"umount -f $X || true;"+
									"done", stamp),
						).CombinedOutput()
						fmt.Printf("Attempted pre-cleanup output: %s\n", o)
						o, err = exec.Command("zpool", "destroy", "-f", shr[0]).CombinedOutput()
						if err != nil {
							fmt.Printf("error running zpool destroy %s: %s %s\n", shr[0], o, err)
							time.Sleep(1 * time.Second)
							o, err := exec.Command("zpool", "destroy", "-f", shr[0]).CombinedOutput()
							if err != nil {
								fmt.Printf("Failed second try: %s %s", err, o)
							}
						}
						fmt.Printf("=== Cleaned up zpool %s\n", shr[0])
					}
				}
			}

			// we can only clean up zpool data dirs after we release the zpools.
			for _, n := range ns {
				nodeSuffix := fmt.Sprintf("%d-%s-node-%s", stamp, n.ClusterNum, n.NodeNum)
				// cleanup stray mounts, e.g. shm mounts
				err = System("bash", "-c", fmt.Sprintf(`
					for X in $(mount|cut -d ' ' -f 3 |grep %s); do
						umount $X || true
					done`, nodeSuffix),
				)
				if err != nil {
					fmt.Printf("err during cleanup mounts: %s\n", err)
				}
			}
		}()
		out, err := exec.Command("docker", "volume", "prune", "-f").Output()
		if err != nil {
			fmt.Printf("unable to prune docker volumes: %s, %s\n", err, out)
		}
	}
	err = System("docker", "container", "prune", "-f")
	if err != nil {
		fmt.Printf("Error from docker container prune -f: %v", err)
	}

}

func FindAHostIP() (string, error) {
	// Some terrible shell magic to find an interface that doesn't look
	// like a docker, bridge, or loopback one and find its IP

	// The intention is to find an IP that we can bind servers to on the
	// host, that things running in Docker containers can connect to.
	c := exec.Command("sh", "-c", "ip addr show `ifconfig -a | grep '^[a-z0-9]*:' | sed 's/:.*$//' | egrep -v 'docker|br-|lo|veth' | head -n 1` | grep -w inet | awk '{ print $2 }' | sed 's,/.*,,'")
	out, err := c.CombinedOutput()

	ip := strings.TrimRight(string(out), " \n")

	return ip, err
}

func docker(node string, cmd string, env map[string]string) (string, error) {
	args := []string{"exec"}
	if env != nil {
		for name, value := range env {
			args = append(args, []string{"-e", fmt.Sprintf("%s=%s", name, value)}...)
		}
	}
	args = append(args, []string{"-i", node, "bash", "-c", cmd}...)
	c := exec.Command("docker", args...)

	var b bytes.Buffer
	var o, e io.Writer
	if _, ok := env["DEBUG_MODE"]; ok {
		o = io.MultiWriter(&b, os.Stdout)
		e = io.MultiWriter(&b, os.Stderr)

	} else {
		o = io.MultiWriter(&b)
		e = io.MultiWriter(&b)
	}

	c.Stdout = o
	c.Stderr = e
	err := c.Run()
	return string(b.Bytes()), err

}

func dockerContext(ctx context.Context, node string, cmd string, env map[string]string) (string, error) {
	args := []string{"exec"}
	if env != nil {
		for name, value := range env {
			args = append(args, []string{"-e", fmt.Sprintf("%s=%s", name, value)}...)
		}
	}
	args = append(args, []string{"-i", node, "bash", "-c", cmd}...)
	c := exec.CommandContext(ctx, "docker", args...)

	var b bytes.Buffer
	var o, e io.Writer
	if _, ok := env["DEBUG_MODE"]; ok {
		o = io.MultiWriter(&b, os.Stdout)
		e = io.MultiWriter(&b, os.Stderr)

	} else {
		o = io.MultiWriter(&b)
		e = io.MultiWriter(&b)
	}

	c.Stdout = o
	c.Stderr = e
	err := c.Run()
	return string(b.Bytes()), err

}

func RunOnNodeErr(node string, cmd string) (string, error) {
	fmt.Printf("RUNNING on %s: %s\n", node, cmd)
	return docker(node, cmd, nil)
}

// the same as RunOnNodeErr but will stream the outout to os.Stdout & os.Stderr
func RunOnNodeErrDebug(node string, cmd string) (string, error) {
	fmt.Printf("RUNNING on %s: %s\n", node, cmd)
	return docker(node, cmd, DEBUG_ENV)
}

func dockerSystem(node string, cmd string) error {
	return System("docker", "exec", "-i", node, "sh", "-c", cmd)
}

func RunOnNode(t *testing.T, node string, cmd string) {
	t.Helper()
	fmt.Printf("RUNNING on %s: %s\n", node, cmd)
	debugEnv := map[string]string{}
	s, err := docker(node, cmd, debugEnv)
	if err != nil {
		t.Fatalf("%s while running %s on %s: %s", err, cmd, node, s)
	}
}

func RunOnNodeContext(ctx context.Context, t *testing.T, node string, cmd string) {
	t.Helper()
	fmt.Printf("RUNNING on %s: %s\n", node, cmd)
	debugEnv := map[string]string{}
	s, err := dockerContext(ctx, node, cmd, debugEnv)
	if err != nil {
		select {
		case <-ctx.Done():
			// nothing to do, ctx was cancelled
			return
		default:
			t.Fatalf("%s while running %s on %s: %s", err, cmd, node, s)
		}
	}
}

func RunOnNodeDebug(t *testing.T, node string, cmd string) {
	fmt.Printf("RUNNING on %s: %s\n", node, cmd)
	s, err := docker(node, cmd, DEBUG_ENV)
	if err != nil {
		t.Fatalf("%s while running %s on %s: %s", err, cmd, node, s)
	}
}

// e.g. KubectlApply(t, node1, `yaml...`)
func KubectlApply(t *testing.T, node string, input string) {
	c := exec.Command("docker", "exec", "-i", node, "kubectl", "apply", "-f", "-")

	var b bytes.Buffer

	o := io.MultiWriter(&b, os.Stdout)
	e := io.MultiWriter(&b, os.Stderr)

	c.Stdout = o
	c.Stderr = e

	stdin, err := c.StdinPipe()
	if err != nil {
		panic(err)
	}
	go func() {
		stdin.Write([]byte(input))
		stdin.Close()
	}()

	err = c.Run()
	if err != nil {
		t.Error(fmt.Errorf("%s while applying the following manifest on %s: %s\n%s", err, node, string(b.Bytes()), input))
	}
}

func OutputFromRunOnNode(t *testing.T, node string, cmd string) string {
	s, err := docker(node, cmd, nil)
	if err != nil {
		t.Error(fmt.Errorf("%s while running %s on %s: %s", err, cmd, node, s))
	}
	return s
}

func StreamOutputFromRunOnNode(t *testing.T, node string, cmd string) string {
	env := make(map[string]string)
	env["DEBUG_MODE"] = "1"
	s, err := docker(node, cmd, env)
	if err != nil {
		t.Error(fmt.Errorf("%s while running %s on %s: %s", err, cmd, node, s))
	}
	return s
}

func LocalImage(service string) string {
	var registry string
	// See .gitlab-ci.yml in the dotmesh repo for where these are set up
	if reg := os.Getenv("CI_REGISTRY"); reg != "" {
		repo := os.Getenv("CI_PROJECT_PATH")
		if repo == "" {
			repo = os.Getenv("CI_REPOSITORY")
		}
		registry = reg + "/" + repo
	}

	var tag string
	tagPreference := []string{"CI_DOCKER_TAG", "CI_COMMIT_TAG", "CI_COMMIT_SHA"}
	for _, tagVariable := range tagPreference {
		tag = os.Getenv(tagVariable)
		if tag != "" {
			break
		}
	}
	if tag == "" {
		tag = "latest"
	}
	// this means that if the X service is the one being tested - then
	// use the GIT_HASH from CI for that service and 'latest-passing-tests' for everything else
	// (which is the last build of that repo that passed the tests on master)
	serviceBeingTested := os.Getenv("CI_SERVICE_BEING_TESTED")
	if serviceBeingTested != "" && serviceBeingTested != "dotmesh" {
		tag = "test-latest"
	}
	img := fmt.Sprintf("%s:%s", service, tag)
	if registry == "" {
		return img + " --offline"
	}
	return fmt.Sprintf("%s/%s", registry, img)
}

func localEtcdImage() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s.local:80/dotmesh/etcd:v3.0.15", hostname)
}

func (c *Cluster) localImageArgs() string {
	logSuffix := ""
	if os.Getenv("DISABLE_LOG_AGGREGATION") == "" {
		logSuffix = fmt.Sprintf(" --log %s", hostIpFromContainer(c.GetIpPrefix()))
	}
	traceSuffix := ""
	if os.Getenv("DISABLE_TRACING") == "" {
		traceSuffix = fmt.Sprintf(" --trace %s", hostIpFromContainer(c.GetIpPrefix()))
	}
	regSuffix := ""
	return ("--image " + LocalImage("dotmesh-server") + " --etcd-image " + localEtcdImage() +
		" --docker-api-version 1.23 --discovery-url http://" + hostIpFromContainer(c.GetIpPrefix()) + ":8087" +
		logSuffix + traceSuffix + regSuffix)
}

// TODO a test which exercise `dm cluster init --count 3` or so

func DockerRun(v ...string) string {
	// supports either 1 or 2 args. in 1-arg case, just takes a volume name.
	// in 2-arg case, takes volume name and arguments to pass to docker run.
	// in 3-arg case, third arg is image in "$(hostname).local:80/$image".
	// in 4-arg case, fourth arg is volume target.
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	image := "busybox"
	if len(v) >= 3 {
		image = v[2]
	}
	path := "/foo"
	if len(v) == 4 {
		path = v[3]
	}
	if len(v) > 1 {
		return fmt.Sprintf(
			"docker run -i -v '%s:%s' --volume-driver dm %s %s.local:80/%s",
			v[0], path, v[1], hostname, image,
		)
	} else {
		return fmt.Sprintf(
			"docker run -i -v '%s:%s' --volume-driver dm %s.local:80/%s",
			v[0], path, hostname, image,
		)
	}
}

func DockerRunDetached(v ...string) string {
	// supports either 1 or 2 args. in 1-arg case, just takes a volume name.
	// in 2-arg case, takes volume name and arguments to pass to docker run.
	// in 3-arg case, third arg is image in "$(hostname).local:80/$image".
	// in 4-arg case, fourth arg is volume target.
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	image := "busybox"
	if len(v) == 3 {
		image = v[2]
	}
	path := "/foo"
	if len(v) == 4 {
		path = v[3]
	}
	if len(v) > 1 {
		return fmt.Sprintf(
			"docker run -d -v '%s:%s' --volume-driver dm %s %s.local:80/%s",
			v[0], path, v[1], hostname, image,
		)
	} else {
		return fmt.Sprintf(
			"docker run -d -v '%s:%s' --volume-driver dm %s.local:80/%s",
			v[0], path, hostname, image,
		)
	}
}

var uniqNumber int

func UniqName() string {
	uniqNumber++
	return fmt.Sprintf("volume_%d", uniqNumber)
}

type Node struct {
	ClusterName string
	Container   string
	IP          string
	ApiKey      string
	Password    string
	Port        int
	// paths & names in /dotmesh-test-pools
	PoolDir  string
	PoolName string
}

type Cluster struct {
	DesiredNodeCount int
	Port             int
	Env              map[string]string
	ClusterArgs      string
	Nodes            []Node
	IpPrefix         int
}

type BlankCluster struct {
	DesiredNodeCount int
	Nodes            []Node
	IpPrefix         int
}

type Kubernetes struct {
	DesiredNodeCount int
	Nodes            []Node
	StorageMode      string
	DindStorage      bool
	IpPrefix         int
}

type Pair struct {
	From       Node
	To         Node
	RemoteName string
}

func NewBlankCluster(desiredNodeCount int) *BlankCluster {
	return &BlankCluster{DesiredNodeCount: desiredNodeCount}
}

func NewClusterOnPort(port, desiredNodeCount int) *Cluster {
	emptyEnv := make(map[string]string)
	return NewClusterWithArgs(desiredNodeCount, port, emptyEnv, "")
}

func NewCluster(desiredNodeCount int) *Cluster {
	emptyEnv := make(map[string]string)
	return NewClusterWithArgs(desiredNodeCount, 0, emptyEnv, "")
}

func NewClusterWithEnv(desiredNodeCount int, env map[string]string) *Cluster {
	return NewClusterWithArgs(desiredNodeCount, 0, env, "")
}

// custom arguments that are passed through to `dm cluster {init,join}`
func NewClusterWithArgs(desiredNodeCount, port int, env map[string]string, args string) *Cluster {
	env["DOTMESH_UPGRADES_URL"] = "" //set default test env vars
	return &Cluster{DesiredNodeCount: desiredNodeCount, Port: port, Env: env, ClusterArgs: args}
}

func NewKubernetes(desiredNodeCount int, storageMode string, dindStorage bool) *Kubernetes {
	return &Kubernetes{
		DesiredNodeCount: desiredNodeCount,
		StorageMode:      storageMode,
		DindStorage:      dindStorage,
	}
}

type Federation []Startable

func nodeName(now int64, i, j int) string {
	return fmt.Sprintf("cluster-%d-%d-node-%d", now, i, j)
}

// testDirName
func testDirName(now int64) string {
	if os.Getenv("GO_TEST_ID") != "" {
		return fmt.Sprintf("/dotmesh-test-pools/%d-%s", now, os.Getenv("GO_TEST_ID"))
	} else {
		return fmt.Sprintf("/dotmesh-test-pools/%d", now)
	}
}

func NodeName(now int64, i, j int) string {
	return nodeName(now, i, j)
}

func poolId(now int64, i, j int) string {
	return fmt.Sprintf("testpool-%d-%d-node-%d", now, i, j)
}

func GetNodeIP(t *testing.T, now int64, i, j int) string {
	nodeIP := strings.TrimSpace(OutputFromRunOnNode(t,
		nodeName(now, i, j),
		`ifconfig eth0 | grep -v "inet6" | grep "inet" | cut -d " " -f 10`,
	))
	if strings.TrimSpace(nodeIP) == "" {
		// Try the way that works on newbuntu (18.04) :-S
		nodeIP = strings.TrimSpace(OutputFromRunOnNode(t,
			nodeName(now, i, j),
			`ifconfig eth0 | grep -v "inet6" | grep "inet" | cut -d " " -f 12 |cut -d ":" -f 2`,
		))
	}

	return nodeIP
}

func NodeFromNodeName(t *testing.T, now int64, i, j int, clusterName string) Node {

	nodeIP := GetNodeIP(t, now, i, j)

	dotmeshConfig, err := docker(
		nodeName(now, i, j),
		"cat /root/.dotmesh/config",
		nil,
	)
	var apiKey string
	var port int
	if err != nil {
		fmt.Printf("no dm config found, proceeding without recording apiKey\n")
	} else {
		fmt.Printf("dm config on %s: %s\n", nodeName(now, i, j), dotmeshConfig)
		m := struct {
			Remotes struct {
				Local struct {
					ApiKey string
					Port   int
				}
			}
		}{}
		json.Unmarshal([]byte(dotmeshConfig), &m)
		apiKey = m.Remotes.Local.ApiKey
		port = m.Remotes.Local.Port
	}

	// /root/.dotmesh/admin-password.txt is created on docker
	// clusters, but k8s clusters are configured from k8s secrets so
	// there's no automatic password generation; the value we show here
	// is what we hardcode as the password.
	password := OutputFromRunOnNode(t,
		nodeName(now, i, j),
		"sh -c 'if [ -f /root/.dotmesh/admin-password.txt ]; then "+
			"cat /root/.dotmesh/admin-password.txt; else echo -n FAKEAPIKEY; fi'",
	)

	fmt.Printf("dm password on %s: %s\n", nodeName(now, i, j), password)

	return Node{
		ClusterName: clusterName,
		Container:   nodeName(now, i, j),
		IP:          nodeIP,
		ApiKey:      apiKey,
		Password:    password,
		Port:        port,
		PoolDir:     filepath.Join(testDirName(now), fmt.Sprintf("wd-%d-%d", i, j)),
		PoolName:    fmt.Sprintf("testpool-%d-%d-node-%d", now, i, j),
	}
}

// Networking config helper functions

func hostIpFromContainer(prefix int) string {
	// This is definitely the default route from the dind containers to the
	// host.
	return "10.200.0.1"
}

func serviceCIDR(prefix int) string {
	return fmt.Sprintf("172.16.%d.0/24", prefix)
}

func podNetworkCIDR(prefix int) string {
	return fmt.Sprintf("10.%d.0.0/16", prefix)
}

// Federation

func (f Federation) Start(t *testing.T) error {
	fmt.Printf(`
SEARCHABLE HEADER: STARTING CLUSTER
 ____ _____  _    ____ _____ ___ _   _  ____ 
/ ___|_   _|/ \  |  _ \_   _|_ _| \ | |/ ___|
\___ \ | | / _ \ | |_) || |  | ||  \| | |  _ 
 ___) || |/ ___ \|  _ < | |  | || |\  | |_| |
|____/ |_/_/   \_\_| \_\|_| |___|_| \_|\____|
                                             
  ____ _    _   _ ____ _____ _____ ____  
 / ___| |  | | | / ___|_   _| ____|  _ \ 
| |   | |  | | | \___ \ | | |  _| | |_) |
| |___| |__| |_| |___) || | | |___|  _ < 
 \____|_____\___/|____/ |_| |_____|_| \_\
                                         
`)

	stamp = time.Now().UnixNano() - DAWN_OF_DOTMESH_TIME
	err := testSetup(t, f)
	if err != nil {
		return err
	}

	// Register to delete top-level directory, last of all
	RegisterCleanupAction(99, fmt.Sprintf(`rm -rf %s`, testDirName(stamp)))

	LogTiming("setup")

	for i, c := range f {
		fmt.Printf("==== GOING FOR %d, %+v ====\n", i, c)
		err = c.Start(t, stamp, i)
		if err != nil {
			return err
		}
	}
	// TODO refactor the following so that each node has one other node on the
	// other cluster as a remote named 'cluster0' or 'cluster1', etc.

	// for each node in each cluster, add remotes for all the other clusters
	// O(n^3)
	pairs := []Pair{}
	for _, c := range f {
		if !c.isBlank() {
			for _, node := range c.GetNodes() {
				for _, otherCluster := range f {
					first := otherCluster.GetNode(0)
					pairs = append(pairs, Pair{
						From:       node,
						To:         first,
						RemoteName: first.ClusterName,
					})
					for i, oNode := range otherCluster.GetNodes() {
						pairs = append(pairs, Pair{
							From:       node,
							To:         oNode,
							RemoteName: fmt.Sprintf("%s_node_%d", first.ClusterName, i),
						})
					}
				}
			}
		}

	}
	for _, pair := range pairs {
		found := false
		for _, remote := range strings.Split(OutputFromRunOnNode(t,
			pair.From.Container, "dm remote"), "\n") {
			if remote == pair.RemoteName {
				found = true
			}
		}
		if !found {
			err := TryUntilSucceeds(
				func() error {
					st, err := docker(
						pair.From.Container,
						fmt.Sprintf(
							"echo %s |dm remote add %s admin@%s:%d",
							pair.To.ApiKey,
							pair.RemoteName,
							pair.To.IP,
							pair.To.Port,
						),
						nil,
					)
					if err != nil {
						fmt.Printf("Error adding remote: %s", st)
					}
					return err
				},
				fmt.Sprintf("adding remote to %s", pair.From.Container),
			)
			if err != nil {
				t.Error(err)
			}
			res := OutputFromRunOnNode(t, pair.From.Container, "dm remote -v")
			if !strings.Contains(res, pair.RemoteName) {
				t.Errorf("can't find %s in %s's remote config", pair.RemoteName, pair.From.ClusterName)
			}
			// TODO remove duplicate runs of the following for minor speedup
			RunOnNode(t, pair.From.Container, "dm remote switch local")
		}
	}
	fmt.Printf(`
SEARCHABLE HEADER: STARTING TESTS
 ____ _____  _    ____ _____ ___ _   _  ____   _____ _____ ____ _____ ____  
/ ___|_   _|/ \  |  _ \_   _|_ _| \ | |/ ___| |_   _| ____/ ___|_   _/ ___| 
\___ \ | | / _ \ | |_) || |  | ||  \| | |  _    | | |  _| \___ \ | | \___ \ 
 ___) || |/ ___ \|  _ < | |  | || |\  | |_| |   | | | |___ ___) || |  ___) |
|____/ |_/_/   \_\_| \_\|_| |___|_| \_|\____|   |_| |_____|____/ |_| |____/ 
                                                                            
`)
	return nil
}

type Startable interface {
	GetNode(int) Node
	GetNodes() []Node
	AppendNode(Node)
	GetDesiredNodeCount() int
	Start(*testing.T, int64, int) error
	RunArgs(int, int) string
	GetIpPrefix() int
	SetIpPrefix(int)
	isBlank() bool
}

///////////// Kubernetes

func (c *Kubernetes) RunArgs(i, j int) string {
	// try starting Kube clusters without hardcoding any IP addresses
	return ""
}

func (c *Kubernetes) GetNode(i int) Node {
	return c.Nodes[i]
}

func (c *Kubernetes) GetNodes() []Node {
	return c.Nodes
}

func (c *Kubernetes) AppendNode(n Node) {
	c.Nodes = append(c.Nodes, n)
}

func (c *Kubernetes) GetDesiredNodeCount() int {
	return c.DesiredNodeCount
}

func (c *Kubernetes) SetIpPrefix(ipPrefix int) {
	c.IpPrefix = ipPrefix
}

func (c *Kubernetes) GetIpPrefix() int {
	return c.IpPrefix
}

func (c *Kubernetes) isBlank() bool {
	return false
}

func ChangeOperatorNodeSelector(masterNode, nodeSelector string) error {
	st, err := docker(
		masterNode,
		"kubectl get configmap -n dotmesh configuration -o yaml",
		nil,
	)
	if err != nil {
		return err
	}

	re := regexp.MustCompile("nodeSelector: .*\n")
	newYaml := re.ReplaceAllLiteralString(st, "nodeSelector: "+nodeSelector+"\n")

	st, err = docker(
		masterNode,
		"kubectl delete configmap -n dotmesh configuration ; "+
			"kubectl apply -f - -n dotmesh "+
			"<<DOTMESHEOF\n"+newYaml+"\nDOTMESHEOF",
		nil,
	)
	if err != nil {
		return err
	}

	return nil
}

func RestartOperator(t *testing.T, masterNode string) {
	output := OutputFromRunOnNode(t, masterNode, "kubectl get pods -n dotmesh | grep dotmesh-operator | grep Running | cut -f 1 -d ' '")
	podNames := strings.FieldsFunc(output, getFieldsByNewLine)

	if len(podNames) != 1 {
		t.Errorf("RunningOperatorPods = %v. Operator not running or more than one running operator instance detected.", podNames)
	}
	podName := podNames[0]
	RunOnNode(t, masterNode, "kubectl delete pod -n dotmesh "+podName)
	fmt.Printf("Counting operator pods:\n")
	for tries := 1; tries < 10; tries++ {
		output := OutputFromRunOnNode(t, masterNode, "kubectl get pods -n dotmesh | grep dotmesh-operator | grep -v "+podName)
		podsExceptOld := strings.FieldsFunc(output, getFieldsByNewLine)
		running := len(podsExceptOld)
		if running == 1 {
			break
		}

		if tries == 9 {
			t.Error("Couldn't seem to get back to a single operator pod running after restart :-(")
		} else {
			fmt.Printf("%d operator pods running: %#v\n", running, podsExceptOld)
			time.Sleep(5 * time.Second)
		}
	}
}

func getUniqueIpPrefix() int {
	ipPrefix := -1
	iteration := 0
	prefixFileName := ""
	for ; iteration < 20; iteration++ {
		ipPrefix = rand.Intn(60) + 20

		prefixFileName = fmt.Sprintf("/tmp/DOTMESH_KUBE_IP.%d", ipPrefix)

		// Attempt atomic creation of the lock file
		fp, err := os.OpenFile(prefixFileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if os.IsExist(err) {
			// Is it stale?
			stat, err := os.Stat(prefixFileName)
			if err != nil {
				if os.IsNotExist(err) {
					// Somebody might have deleted it from under us, no problem
				} else {
					panic(err)
				}
			} else {
				age := time.Now().Nanosecond() - stat.ModTime().Nanosecond()
				if age > 3600000000 { // 1 hour in nanoseconds
					os.Remove(prefixFileName) // Deliberately ignore errors, as somebody else might be doing the same thing at the same time
				}
			}
			// Sleep a random interval to avoid thundering herds, then try again, picking a new
			// random number
			time.Sleep(time.Duration(rand.Intn(1000)+500) * time.Millisecond)
			continue
		} else if err != nil {
			panic(err)
		}

		// Success! Write an identifying string (our test dir name) to
		// the file, just for audit reasons.
		fp.Write([]byte(testDirName(stamp) + "\n"))
		fp.Close()
		break
	}

	if ipPrefix == -1 {
		panic("Gave up looking for a free IP prefix")
	}

	// Make sure we clear up if the tests finish OK
	RegisterCleanupAction(30, fmt.Sprintf("rm %s || true", prefixFileName))

	return ipPrefix
}

func (c *Kubernetes) Start(t *testing.T, now int64, i int) error {
	if c.DesiredNodeCount == 0 {
		panic("no such thing as a zero-node cluster")
	}

	// TODO: start using injectImage function from github.com/dotmesh-io/e2e
	// instead of this - it uses the host docker cache, doesn't depend on a
	// local registry, and we won't get any speedup from doing it the following
	// way anyway because the dind are always fresh (empty) docker instances
	// anyway.
	images, err := ioutil.ReadFile("../kubernetes/images.txt")
	if err != nil {
		return err
	}
	cache := map[string]string{}
	for _, x := range strings.Split(string(images), "\n") {
		ys := strings.Split(x, " ")
		if len(ys) == 2 {
			cache[ys[0]] = ys[1]
		}
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	// pre-pull all the container images Kubernetes needs to use, tag them to
	// trick it into not downloading anything.
	for fqImage, localName := range cache {
		realLocalName := fmt.Sprintf("%s.local:80/%s", hostname, localName)
		err = SilentSystem("docker", "image", "inspect", fqImage)
		if err != nil {
			fmt.Printf("Image for caching %s not available in local docker, pulling...\n", fqImage)
			err = System("docker", "pull", fqImage)
			if err != nil {
				panic(fmt.Sprintf("unable to cache image %s", fqImage))
			}
		} else {
			fmt.Printf("Found cached image %s\n", fqImage)
		}
		fmt.Printf("Tagging %s -> %s\n", fqImage, realLocalName)
		err = System("docker", "tag", fqImage, realLocalName)
		if err != nil {
			panic(fmt.Sprintf("Error tagging %s -> %s\n", fqImage, realLocalName))
		}
		fmt.Printf("Pushing %s\n", realLocalName)
		err = System("docker", "push", realLocalName)
		if err != nil {
			panic(fmt.Sprintf("Error pushing %s\n", realLocalName))
		}
	}
	finishing := make(chan bool)
	for j := 0; j < c.DesiredNodeCount; j++ {
		go func(j int) {
			for fqImage, localName := range cache {
				realLocalName := fmt.Sprintf("%s.local:80/%s", hostname, localName)
				fmt.Printf("Pulling %s\n", realLocalName)
				st, err := docker(
					nodeName(now, i, j),
					/*
					   docker pull $local_name
					   docker tag $local_name $fq_image
					*/
					fmt.Sprintf(
						"docker pull %s && "+
							"docker tag %s %s",
						realLocalName, realLocalName, fqImage,
					),
					nil,
				)
				if err != nil {
					panic(st)
				}
			}
			finishing <- true
		}(j)
	}
	for j := 0; j < c.DesiredNodeCount; j++ {
		_ = <-finishing
	}

	logAddr := ""
	if os.Getenv("DISABLE_LOG_AGGREGATION") == "" {
		logAddr = hostIpFromContainer(c.GetIpPrefix())
	}

	// Move k8s root dir into /dotmesh-test-pools/<timestamp>/ on every node.

	// This is required for the tests of k8s using PV storage with the
	// DIND provisioner to work; the container mountpoints must be
	// consistent between the actual host and the dind kubelet node, or
	// ZFS will barf on them. Putting the k8s root dir in
	// /dotmesh-test-pools/<timestamp> means the paths are consistent across all
	// containers, as we keep the same filesystem mounted there
	// throughout.
	for j := 0; j < c.DesiredNodeCount; j++ {
		node := nodeName(now, i, j)
		path := fmt.Sprintf("%s/k8s-%d-%d", testDirName(now), i, j)
		cmd := fmt.Sprintf("sed -i 's!hyperkube kubelet !hyperkube kubelet --root-dir %s !' /lib/systemd/system/kubelet.service && mkdir -p %s && systemctl restart kubelet", path, path)
		_, err := docker(
			node,
			cmd,
			nil,
		)
		if err != nil {
			return err
		}

		// Also: Leave the config file for the dind-flexvolume driver to make it store files for this test run
		// in the /dotmesh-test-pools/<timestamp>/ file

		_, err = docker(
			node,
			fmt.Sprintf("echo %s/dind-flexvolume > /dind-flexvolume-prefix && mkdir -p `cat /dind-flexvolume-prefix`", testDirName(now)),
			nil,
		)
		if err != nil {
			return err
		}
	}

	// Regex the following yamels to refer to the newly pushed dotmesh
	// container image, rather than the latest stable

	err = System("bash", "-c",
		fmt.Sprintf(
			`MASTER=%s
			docker exec $MASTER mkdir /dotmesh-kube-yaml
			for X in ../kubernetes/*.yaml; do docker cp $X $MASTER:/dotmesh-kube-yaml/; done
			docker exec $MASTER sed -i 's/quay.io\/dotmesh\/dotmesh-operator:DOCKER_TAG/%s/' /dotmesh-kube-yaml/dotmesh.yaml
			docker exec $MASTER sed -i 's/quay.io\/dotmesh\/dotmesh-dynamic-provisioner:DOCKER_TAG/%s/' /dotmesh-kube-yaml/dotmesh.yaml
			docker exec $MASTER sed -i 's/size: 3/size: 1/' /dotmesh-kube-yaml/dotmesh-etcd-cluster.yaml
			`,
			nodeName(now, i, 0),
			strings.Replace(LocalImage("dotmesh-operator"), "/", "\\/", -1),
			strings.Replace(LocalImage("dotmesh-dynamic-provisioner"), "/", "\\/", -1),
		),
	)
	if err != nil {
		return err
	}

	// Set up routing between nodes, similar to how kubeadm-dind-cluster does it.

	fmt.Printf("Setting up bridge networking between nodes...\n")
	fmt.Printf("============================================================\n")

	for j := 0; j < c.DesiredNodeCount; j++ {
		for k := 0; k < c.DesiredNodeCount; k++ {
			if j == k {
				continue
			}
			gw := OutputFromRunOnNode(t, c.Nodes[k].Container,
				"ip addr show eth0 | grep -w inet | awk '{ print $2 }' | sed 's,/.*,,'",
			)

			// debuggering
			routes := OutputFromRunOnNode(t, c.Nodes[j].Container, "ip route")
			fmt.Printf("Routes on %s: %s\n", c.Nodes[j].Container, routes)
			podIpPrefix := OutputFromRunOnNode(t, c.Nodes[j].Container, "cat /POD_IP_PREFIX")
			fmt.Printf("POD_IP_PREFIX on %s: %s\n", c.Nodes[j].Container, podIpPrefix)

			cmd := fmt.Sprintf("ip route add $(cat /POD_IP_PREFIX)%d.0/24 via %s", k+11, gw)
			fmt.Printf("ON %s, RUN %s\n", c.Nodes[j].Container, cmd)
			RunOnNode(t, c.Nodes[j].Container, cmd)
		}
	}
	fmt.Printf("============================================================\n")

	fmt.Println("Yamls are wrangled, preparing to kubeadm init...")

	st, err := docker(
		nodeName(now, i, 0),
		"touch /dind/flexvolume_driver && "+
			"systemctl start kubelet && "+

			// write out an appropriate kubeadm.conf, especially wrt networking config
			fmt.Sprintf(`sed -e "s|{{API_VERSION}}|kubeadm.k8s.io/v1alpha2|" \
				-e "s|{{ADV_ADDR}}|%s|" \
				-e "s|{{POD_SUBNET_DISABLE}}||" \
				-e "s|{{POD_NETWORK_CIDR}}|%s|" \
				-e "s|{{SVC_SUBNET}}|%s|" \
				-e "s|{{BIND_ADDR}}|0.0.0.0|" \
				-e "s|{{BIND_PORT}}|8080|" \
				-e "s|{{FEATURE_GATES}}|{}|" \
				-e "s|{{KUBEADM_VERSION}}|1.10.1|" \
				-e "s|{{COMPONENT_FEATURE_GATES}}|feature-gates: MountPropagation=true|" \
				-e "s|{{APISERVER_EXTRA_ARGS}}||" \
				-e "s|{{CONTROLLER_MANAGER_EXTRA_ARGS}}||" \
				-e "s|{{SCHEDULER_EXTRA_ARGS}}||" \
				-e "s|{{KUBE_MASTER_NAME}}|%s|" \
				/etc/kubeadm.conf.tmpl > /etc/kubeadm.conf`,
				// master ip address
				c.Nodes[0].IP,
				// POD_NETWORK_CIDR
				podNetworkCIDR(c.GetIpPrefix()),
				// SERVICE_CIDR
				serviceCIDR(c.GetIpPrefix()),
				// master hostname
				nodeName(now, i, 0),
			),
		nil,
	)
	if err != nil {
		return err
	}

	st, err = docker(
		nodeName(now, i, 0),
		"wrapkubeadm init --ignore-preflight-errors=all --kubernetes-version=v1.10.6",
		map[string]string{
			"DEBUG_MODE": "WHY, YES PLEASE, IF THAT'S QUITE ALRIGHT",
		},
	)
	if err != nil {
		return err
	}

	_, err = docker(
		nodeName(now, i, 0),
		"mkdir /root/.kube && cp /etc/kubernetes/admin.conf /root/.kube/config && "+
			// Make kube-dns faster; trick copied from dind-cluster-v1.7.sh
			"kubectl get deployment kube-dns -n kube-system -o json | jq '.spec.template.spec.containers[0].readinessProbe.initialDelaySeconds = 3|.spec.template.spec.containers[0].readinessProbe.periodSeconds = 3' | kubectl apply --force -f -",
		nil,
	)
	if err != nil {
		return err
	}

	fmt.Println("kubeadm init is done, preparing to join...")

	lines := strings.Split(st, "\n")

	joinArgs := func(lines []string) string {
		for _, line := range lines {
			shrap := strings.Fields(line)
			if len(shrap) > 3 {
				// line will look like:
				//     kubeadm join --token c06d9b.57ef131db5c0e0e5 10.192.0.2:6443
				if shrap[0] == "kubeadm" && shrap[1] == "join" {
					return strings.Join(shrap[2:], " ")
				}
			}
		}
		return ""
	}(lines)

	fmt.Printf("JOIN URL: %s\n", joinArgs)

	clusterName := fmt.Sprintf("cluster_%d", i)

	for j := 1; j < c.DesiredNodeCount; j++ {
		// if c.Nodes is 3, this iterates over 1 and 2 (0 was the init'd
		// node).
		_, err = docker(nodeName(now, i, j), fmt.Sprintf(
			"touch /dind/flexvolume_driver && "+
				"systemctl start kubelet && "+
				"wrapkubeadm join --ignore-preflight-errors=all %s",
			joinArgs,
		), nil)
		if err != nil {
			return err
		}
		LogTiming("join_" + poolId(now, i, j))
	}

	// Wait until all nodes are Ready, or the next step will fail.
	for try := 0; try < 10; try++ {
		st, err = docker(nodeName(now, i, 0), fmt.Sprintf(
			"kubectl get no | grep ' Ready ' | wc -l",
		), nil)
		if err != nil {
			return err
		}
		if st == fmt.Sprintf("%d\n", c.DesiredNodeCount) {
			break
		} else {
			fmt.Printf("Nodes ready: %s", st)
			time.Sleep(10 * time.Second)
		}
	}

	// Set node labels, for testing the operator.
	// Node N should have labels "clusterSize-X=yes" for X in N..(max-1)
	// so we can limit a pod to "clusterSize-5=yes" to make it only run on 5 nodes.
	time.Sleep(5 * time.Second) // Sleep to let kubelets all get started properly
	for j := 0; j < c.DesiredNodeCount; j++ {
		for k := j; k < c.DesiredNodeCount; k++ {
			_, err = docker(nodeName(now, i, 0), fmt.Sprintf(
				"kubectl label nodes %s clusterSize-%d=yes",
				nodeName(now, i, j),
				k+1,
			), nil)
			if err != nil {
				return err
			}
		}
	}

	// now install dotmesh yaml (setting initial admin pw)

	st, err = docker(
		nodeName(now, i, 0),
		"echo '#### CREATING DOTMESH CONFIGURATION' && "+
			"kubectl create namespace dotmesh && "+
			"echo -n 'secret123' > dotmesh-admin-password.txt && "+
			"echo -n 'FAKEAPIKEY' > dotmesh-api-key.txt && "+
			"kubectl create secret generic dotmesh "+
			"    --from-file=./dotmesh-admin-password.txt --from-file=./dotmesh-api-key.txt -n dotmesh && "+
			"rm dotmesh-admin-password.txt && "+
			"rm dotmesh-api-key.txt",
		nil,
	)
	if err != nil {
		return err
	}

	zfsVersionOverride := ""

	if kzv := os.Getenv("KERNEL_ZFS_VERSION"); kzv != "" {
		zfsVersionOverride = "--from-literal=kernel.zfsVersion=" + kzv + " "
	}

	workDir := filepath.Join(testDirName(now), "wd-#HOSTNAME#")

	st, err = docker(
		nodeName(now, i, 0),
		fmt.Sprintf(
			"kubectl create configmap -n dotmesh configuration "+
				"--from-literal=upgradesUrl= "+
				"'--from-literal=poolNamePrefix=#HOSTNAME#-' "+
				"'--from-literal=local.poolLocation=%s' "+
				"--from-literal=logAddress=%s "+
				"--from-literal=storageMode=%s "+
				"--from-literal=pvcPerNode.storageClass=dind-pv "+
				zfsVersionOverride+
				"--from-literal=nodeSelector=clusterSize-%d=yes", // This needs to be in here so it can be replaced with sed
			workDir,
			logAddr,
			c.StorageMode,
			c.DesiredNodeCount,
		),
		nil,
	)

	RegisterCleanupAction(50, fmt.Sprintf(
		`
		for POOL in $(zpool list -H | cut -f 1 | grep %d); do
			zpool destroy -f $POOL || (zumount $POOL && zpool destroy -f $POOL)
		done &&
		for DIR in %s/wd-*
		do
			umount $DIR || true
		done`,
		stamp,
		testDirName(now),
	))

	if err != nil {
		return err
	}

	if c.DindStorage { // Release the DIND provisioner!!!

		// Install the dind-flexvolume driver on all nodes (test tooling to
		// simulate cloud PVs).
		for j := 0; j < c.DesiredNodeCount; j++ {
			nodeName := nodeName(now, i, j)
			getFlexCommand := fmt.Sprintf(`
			export NODE=%s
			docker exec -i $NODE mkdir -p \
				/usr/libexec/kubernetes/kubelet-plugins/volume/exec/dotmesh.io~dind &&
			docker cp ../target/dind-flexvolume \
				$NODE:/usr/libexec/kubernetes/kubelet-plugins/volume/exec/dotmesh.io~dind/dind &&
			docker exec -i $NODE systemctl restart kubelet
			`,
				// Restarting the kubelet (line above) shouldn't be
				// necessary, but in this case for some reason it seems to be
				// necessary to make the flexvolume plugin be seen on all
				// nodes :-(
				nodeName,
			)
			err = System("bash", "-c", getFlexCommand)
			if err != nil {
				return err
			}
		}

		st, err = docker(
			nodeName(now, i, 0),
			fmt.Sprintf(
				"cat > /dotmesh-kube-yaml/dind-provisioner.yaml <<END\n"+
					"apiVersion: apps/v1\n"+
					"kind: Deployment\n"+
					"metadata:\n"+
					"  name: dind-dynamic-provisioner\n"+
					"  namespace: dotmesh\n"+
					"  labels:\n"+
					"    app: dind-dynamic-provisioner\n"+
					"spec:\n"+
					"  replicas: 1\n"+
					"  selector:\n"+
					"    matchLabels:\n"+
					"      app: dind-dynamic-provisioner\n"+
					"  template:\n"+
					"    metadata:\n"+
					"      labels:\n"+
					"        app: dind-dynamic-provisioner\n"+
					"    spec:\n"+
					"      containers:\n"+
					"      - name: dind-dynamic-provisioner\n"+
					"        image: %s\n"+
					"        imagePullPolicy: \"IfNotPresent\"\n"+
					"END\n",
				LocalImage("dind-dynamic-provisioner")),
			nil)
		if err != nil {
			return err
		}

		st, err = docker(
			nodeName(now, i, 0),
			fmt.Sprintf(
				"cat > /dotmesh-kube-yaml/dind-storageclass.yaml <<END\n"+
					"apiVersion: storage.k8s.io/v1\n"+
					"kind: StorageClass\n"+
					"metadata:\n"+
					"  name: dind-pv\n"+
					"provisioner: dotmesh/dind-dynamic-provisioner\n"+
					"END"),
			nil)
		if err != nil {
			return err
		}

		st, err = docker(
			nodeName(now, i, 0),
			fmt.Sprintf("kubectl apply -f /dotmesh-kube-yaml/dind-provisioner.yaml && kubectl apply -f /dotmesh-kube-yaml/dind-storageclass.yaml"),
			nil)
		if err != nil {
			return err
		}
	}

	crashloopMax := 2
	st, err = docker(
		nodeName(now, i, 0),
		// install etcd operator on the cluster
		"echo '#### STARTING ETCD OPERATOR' && "+
			"kubectl apply -f /dotmesh-kube-yaml/etcd-operator-clusterrole.yaml && "+
			"kubectl apply -f /dotmesh-kube-yaml/etcd-operator-dep.yaml && "+
			// install dotmesh once on the master (retry because etcd operator
			// needs to initialize)
			"sleep 1 && "+
			"echo '#### STARTING ETCD' && "+
			"while ! kubectl apply -f /dotmesh-kube-yaml/dotmesh-etcd-cluster.yaml; do sleep 2; "+KUBE_DEBUG_CMD+"; done",
		DEBUG_ENV,
	)
	if err != nil {
		return err
	}

	fmt.Printf("Waiting for etcd...\n")
	etcdIteration := 0
	crashlooping := 0
	for ; ; etcdIteration++ {
		resp := OutputFromRunOnNode(t, c.Nodes[0].Container, "kubectl describe etcd dotmesh-etcd-cluster -n dotmesh | grep Type:")
		if err != nil {
			return err
		}
		if strings.Contains(resp, "Available") {
			fmt.Printf("etcd is up!\n")
			break
		}
		if etcdIteration > 20 {
			log.Printf("Gave up waiting for etcd after %d retries, giving up.\n", etcdIteration)
			return fmt.Errorf("Gave up waiting for etcd cluster to be ready after %d retries", etcdIteration)
		}

		fmt.Printf("etcd is not up... %#v\n", resp)
		time.Sleep(time.Second * 2)
		st, err = docker(
			nodeName(now, i, 0),
			KUBE_DEBUG_CMD,
			nil,
		)
		if err != nil {
			return err
		}
		st, debugErr := docker(
			nodeName(now, i, 0),
			checkCrashLoopCmd,
			DEBUG_ENV,
		)
		if debugErr != nil {
			if crashlooping < crashloopMax {
				log.Printf("Some pods - %s - are crashlooping. Will retry %d times then quit.", st, crashloopMax)
				crashlooping++
			} else {
				return debugErr
			}
		}
	}
	fmt.Printf("RETRIES: etcd started after %d tries\n", etcdIteration)

	st, err = docker(
		nodeName(now, i, 0),
		"echo '#### STARTING DOTMESH' && "+
			"kubectl apply -f /dotmesh-kube-yaml/dotmesh.yaml",
		DEBUG_ENV,
	)
	if err != nil {
		return err
	}

	// For each node, wait until we can talk to dm from that node before
	// proceeding.
	for j := 0; j < c.DesiredNodeCount; j++ {
		nn := nodeName(now, i, j)
		crashlooping := 0
		err := TryUntilSucceeds(func() error {
			// Check that the docker volume plugin socket works
			st, err = RunOnNodeErr(
				nn,
				`curl -X POST --unix-socket /usr/libexec/kubernetes/kubelet-plugins/volume/exec/dotmesh.io~dm/dm.sock -H "Content-Type: application/json" http://socket/rpc --data-binary "{\"jsonrpc\":\"2.0\",\"method\":\"DotmeshRPC.Ping\",\"params\":{},\"id\":1}"`,
			)

			if err == nil && strings.Contains(st, `{"jsonrpc":"2.0","result":true,"id":1}`) {
				log.Printf("Probed API socket on cluster %d node %d: %s", i, j, st)
			} else {
				log.Printf("Failed to probe API socket on cluster %d node %d: %s / %#v", i, j, st, err)
				st, debugErr := docker(
					nodeName(now, i, 0),
					KUBE_DEBUG_CMD,
					DEBUG_ENV,
				)
				if debugErr != nil {
					log.Printf("Error debugging kubectl status:  %v, %s", debugErr, st)
					return debugErr
				}
				st, debugErr = docker(
					nodeName(now, i, 0),
					checkCrashLoopCmd,
					DEBUG_ENV,
				)
				if debugErr != nil {
					if crashlooping < crashloopMax {
						log.Printf("Some pods - %s - are crashlooping. Will retry %d times then quit.", st, crashloopMax)
						crashlooping++
					} else {
						return debugErr
					}
				}
				if c.DindStorage {
					// Is the DIND provisioner not working?

					// If we see "list of unmounted volumes=[backend-pv]"
					// appearing in the dotmesh server pod status, this is
					// often the problem.

					st, err = docker(
						nn,
						fmt.Sprintf(
							"echo DIND FLEXVOLUME STATUS ON %s:\nls -l /usr/libexec/kubernetes/kubelet-plugins/volume/exec/dotmesh.io~dind/dind\ntail -n 50 /var/log/dotmesh-dind-flexvolume.log\n",
							nn,
						),
						nil,
					)

					if err == nil {
						log.Printf("DIND status:\n%s\n", st)
					} else {
						log.Printf("Error getting DIND status: %#v\n", err)
					}
				}

				time.Sleep(time.Second * 2)
				return fmt.Errorf("Failed to API socket plugin on cluster %d node %d: %s", i, j, st)
			}

			return nil
		}, fmt.Sprintf("checking dotmesh is running on %s", nn))
		if err != nil {
			return err
		}
	}

	// Add the nodes at the end, because NodeFromNodeName expects dotmesh
	// config to be set up.
	for j := 0; j < c.DesiredNodeCount; j++ {
		dotmeshIteration := 0
		for ; ; dotmeshIteration++ {
			// This will succeed as soon as ANY node is ready, as k8s will route
			// access to 127.0.0.1:32607 to any "ready" dotmesh server pod.
			log.Printf("Attempting to add dm remote on cluster %d node %d", i, j)
			st, err = docker(
				nodeName(now, i, j),
				"echo FAKEAPIKEY | dm remote add local admin@127.0.0.1",
				nil,
			)

			if err != nil {
				if dotmeshIteration > 20 {
					fmt.Printf("Gave up adding remotes after %d retries, giving up: %v (%s)\n", dotmeshIteration, err, st)
					return err
				}
				fmt.Printf("got err %v (%s), waiting 2 seconds and trying again...\n", err, st)
				time.Sleep(time.Second * 2)
			} else {
				break
			}
		}
		log.Printf("RETRIES: dotmesh started on cluster %d node %d after %d tries\n", i, j, dotmeshIteration)
		c.Nodes[j] = NodeFromNodeName(t, now, i, j, clusterName)
	}

	return nil
}

///////////// Blank Cluster (plain DIND cluster, no dotmesh)

func (c *BlankCluster) RunArgs(i, j int) string {
	// No special args required for dind with plain Dotmesh.
	return ""
}

func (c *BlankCluster) GetNode(i int) Node {
	return c.Nodes[i]
}

func (c *BlankCluster) GetNodes() []Node {
	return c.Nodes
}

func (c *BlankCluster) AppendNode(n Node) {
	c.Nodes = append(c.Nodes, n)
}

func (c *BlankCluster) GetDesiredNodeCount() int {
	return c.DesiredNodeCount
}

func (c *BlankCluster) SetIpPrefix(ipPrefix int) {
	c.IpPrefix = ipPrefix
}

func (c *BlankCluster) GetIpPrefix() int {
	return c.IpPrefix
}

func (c *BlankCluster) isBlank() bool {
	return true
}

func (c *BlankCluster) Start(t *testing.T, now int64, i int) error {
	if c.DesiredNodeCount == 0 {
		panic("no such thing as a zero-node cluster")
	}

	clusterName := fmt.Sprintf("cluster_%d", i)
	LogTiming("init_" + poolId(now, i, 0))
	for j := 0; j < c.DesiredNodeCount; j++ {
		c.Nodes[j] = NodeFromNodeName(t, now, i, j, clusterName)

		LogTiming("init_" + poolId(now, i, j))
	}
	RegisterCleanupAction(50, fmt.Sprintf(
		`
		for POOL in $(zpool list -H | cut -f 1 | grep %d); do
			zpool destroy -f $POOL || (zumount $POOL && zpool destroy -f $POOL)
		done`,
		now,
	))

	return nil
}

///////////// Cluster (plain Dotmesh cluster, no orchestrator)

func (c *Cluster) RunArgs(i, j int) string {
	// No special args required for dind with plain Dotmesh.
	return ""
}

func (c *Cluster) GetNode(i int) Node {
	return c.Nodes[i]
}

func (c *Cluster) GetNodes() []Node {
	return c.Nodes
}

func (c *Cluster) AppendNode(n Node) {
	c.Nodes = append(c.Nodes, n)
}

func (c *Cluster) GetDesiredNodeCount() int {
	return c.DesiredNodeCount
}

func (c *Cluster) SetIpPrefix(ipPrefix int) {
	c.IpPrefix = ipPrefix
}

func (c *Cluster) GetIpPrefix() int {
	return c.IpPrefix
}

func (c *Cluster) isBlank() bool {
	return false
}

func (c *Cluster) Start(t *testing.T, now int64, i int) error {
	// init the first node in the cluster, join the rest
	if c.DesiredNodeCount == 0 {
		panic("no such thing as a zero-node cluster")
	}

	workDir := filepath.Join(testDirName(now), fmt.Sprintf("wd-%d-0", i))
	dmInitCommand := "EXTRA_HOST_COMMANDS='echo Testing EXTRA_HOST_COMMANDS' dm cluster init " + c.localImageArgs() +
		" --use-pool-dir " + workDir +
		" --use-pool-name " + poolId(now, i, 0) +
		" --dotmesh-upgrades-url ''" +
		" --port " + strconv.Itoa(c.Port)

	if kzv := os.Getenv("KERNEL_ZFS_VERSION"); kzv != "" {
		dmInitCommand = dmInitCommand + " --zfs " + kzv
	}

	if eum, ok := c.Env["EXTERNAL_USER_MANAGER_URL"]; ok {
		dmInitCommand = dmInitCommand + " --external-user-manager-url " + eum
	}

	dmInitCommand = dmInitCommand + c.ClusterArgs

	RegisterCleanupAction(50, fmt.Sprintf(
		`
		zpool destroy -f %s || (zumount %s && zpool destroy -f %s) &&
		(umount %s || true)
		`,
		poolId(now, i, 0),
		poolId(now, i, 0),
		poolId(now, i, 0),
		workDir,
	))

	fmt.Printf("running dm cluster init with following command: %s\n", dmInitCommand)

	env := c.Env
	env["DEBUG_MODE"] = "1"

	st, err := docker(
		nodeName(now, i, 0), dmInitCommand, env)

	if err != nil {
		return err
	}
	clusterName := fmt.Sprintf("cluster_%d", i)
	c.Nodes[0] = NodeFromNodeName(t, now, i, 0, clusterName)
	fmt.Printf("(just added) Here are my nodes: %+v\n", c.Nodes)

	lines := strings.Split(st, "\n")
	joinUrl := func(lines []string) string {
		for _, line := range lines {
			shrap := strings.Fields(line)
			if len(shrap) > 3 {
				if shrap[0] == "dm" && shrap[1] == "cluster" && shrap[2] == "join" {
					return shrap[3]
				}
			}
		}
		return ""
	}(lines)
	if joinUrl == "" {
		return fmt.Errorf("unable to find join url in 'dm cluster init' output")
	}
	LogTiming("init_" + poolId(now, i, 0))
	for j := 1; j < c.DesiredNodeCount; j++ {
		// if c.Nodes is 3, this iterates over 1 and 2 (0 was the init'd
		// node).

		workDir := filepath.Join(testDirName(now), fmt.Sprintf("wd-%d-%d", i, j))

		dmJoinCommand := fmt.Sprintf(
			"dm cluster join %s %s %s",
			c.localImageArgs()+" --use-pool-dir "+workDir+" ",
			joinUrl,
			" --use-pool-name "+poolId(now, i, j),
		)

		RegisterCleanupAction(50, fmt.Sprintf(
			`
		zpool destroy -f %s || (zumount %s && zpool destroy -f %s) &&
		(umount %s || true)`,
			poolId(now, i, j),
			poolId(now, i, j),
			poolId(now, i, j),
			workDir,
		))

		if kzv := os.Getenv("KERNEL_ZFS_VERSION"); kzv != "" {
			dmJoinCommand = dmJoinCommand + " --zfs " + kzv
		}

		dmJoinCommand = dmJoinCommand + c.ClusterArgs

		_, err = docker(nodeName(now, i, j), dmJoinCommand, env)
		if err != nil {
			return err
		}
		c.Nodes[j] = NodeFromNodeName(t, now, i, j, clusterName)

		LogTiming("join_" + poolId(now, i, j))
	}
	return nil
}

func CreateDockerNetwork(t *testing.T, node string) {
	fmt.Printf("Creating Docker network on %s", node)
	RunOnNode(t, node, fmt.Sprintf(`
		docker network create dotmesh-dev  &>/dev/null || true
	`))
	RunOnNode(t, node, fmt.Sprintf(`
		docker network connect dotmesh-dev dotmesh-server-inner
	`))
}

type UserLogin struct {
	Email    string
	Username string
	Password string
}

var uniqUserNumber int

func UniqLogin() UserLogin {
	uniqUserNumber++
	return UserLogin{
		Email:    fmt.Sprintf("test%d@test.com", uniqUserNumber),
		Username: fmt.Sprintf("test%d", uniqUserNumber),
		Password: "test",
	}
}

func RegisterUser(node Node, username, email, password string) error {
	fmt.Printf("Registering test user %s on node %s\n", username, node.IP)

	var safeUser struct {
		Id          string
		Name        string
		Email       string
		EmailHash   string
		CustomerId  string
		CurrentPlan string
	}

	err := DoRPC(node.IP, "admin", node.ApiKey,
		"DotmeshRPC.RegisterNewUser",
		struct {
			Name, Email, Password string
		}{
			Name:     username,
			Email:    email,
			Password: password,
		},
		&safeUser)
	if err != nil {
		return err
	}
	return nil
}

func DoRPC(hostname, user, apiKey, method string, args interface{}, result interface{}) error {
	url := fmt.Sprintf("http://%s:32607/rpc", hostname)
	message, err := json2.EncodeClientRequest(method, args)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(message))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(user, apiKey)
	client := new(http.Client)

	resp, err := client.Do(req)

	if err != nil {
		fmt.Printf("Test RPC FAIL: %+v -> %s -> %+v\n", args, method, err)
		return err
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Test RPC FAIL: %+v -> %s -> %+v\n", args, method, err)
		return fmt.Errorf("Error reading body: %s", err)
	}
	err = json2.DecodeClientResponse(bytes.NewBuffer(b), &result)
	if err != nil {
		fmt.Printf("Test RPC FAIL: %+v -> %s -> %+v / %+v\n", args, method, string(b), err)
		return fmt.Errorf("Couldn't decode response '%s': %s", string(b), err)
	}
	fmt.Printf("Test RPC: %+v -> %s -> %+v\n", args, method, result)
	return nil
}

func DoSetDebugFlag(hostname, user, apikey, flag, value string) (string, error) {
	var result string

	fmt.Printf("Attempting to DoSetDebugFlag: %v %v %v %v %v\n", hostname, user, apikey, flag, value)

	err := DoRPC(hostname, user, apikey,
		"DotmeshRPC.SetDebugFlag",
		struct {
			FlagName  string
			FlagValue string
		}{
			FlagName:  flag,
			FlagValue: value,
		},
		&result)

	if err != nil {
		return "", err
	}

	return result, nil
}

func DoAddCollaborator(hostname, user, apikey, namespace, volume, collaborator string) error {
	// FIXME: Duplicated types, see issue #44
	type VolumeName struct {
		Namespace string
		Name      string
	}

	var volumes map[string]map[string]struct {
		Id             string
		Name           VolumeName
		Clone          string
		Master         string
		SizeBytes      int64
		DirtyBytes     int64
		CommitCount    int64
		ServerStatuses map[string]string // serverId => status
	}

	err := DoRPC(hostname, user, apikey,
		"DotmeshRPC.List",
		struct {
		}{},
		&volumes)
	if err != nil {
		return err
	}

	volumeID := volumes[namespace][volume].Id

	var result bool
	err = DoRPC(hostname, user, apikey,
		"DotmeshRPC.AddCollaborator",
		struct {
			MasterBranchID, Collaborator string
		}{
			MasterBranchID: volumeID,
			Collaborator:   collaborator,
		},
		&result)
	if err != nil {
		return err
	}
	if !result {
		return fmt.Errorf("AddCollaborator failed without an error")
	}
	return nil
}
