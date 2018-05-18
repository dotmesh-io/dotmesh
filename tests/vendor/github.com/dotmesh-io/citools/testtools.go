package citools

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/rpc/v2/json2"
)

// props to https://github.com/kubernetes/kubernetes/issues/49387
var KUBE_DEBUG_CMD = `(
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
done
kubectl get pods --all-namespaces
exit 0)` // never let the debug command failing cause us to fail the tests!

var timings map[string]float64
var lastTiming int64

const HOST_IP_FROM_CONTAINER = "10.192.0.1"

func Contains(arr []string, str string) bool {
	for _, a := range arr {
		if strings.Contains(a, str) {
			return true
		}
	}
	return false
}

func AddFuncToCleanups(f func()) {
	log.Printf("ADDING CLEANUP FUNC %+v", f)
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

func TestMarkForCleanup(f Federation) {
	log.Printf("Entering TestMarkForCleanup")
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
		for _, n := range c.GetNodes() {
			node := n.Container
			containers := []string{"dotmesh-server", "dotmesh-server-inner"}
			for _, container := range containers {
				logDir := "../extracted_logs"
				logFile := fmt.Sprintf(
					"%s/%s-%s.log",
					logDir, container, node,
				)
				err := System(
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

func testSetup(t *testing.T, f Federation, stamp int64) error {
	err := System("bash", "-c", `
		# Create a home for the test pools to live that can have the same path
		# both from ZFS's perspective and that of the inner container.
		# (Bind-mounts all the way down.)
		mkdir -p /dotmesh-test-pools
		# tmpfs makes etcd not completely rinse your IOPS (which it can do
		# otherwise); create if doesn't exist
		if [ $(mount |grep "/tmpfs " |wc -l) -eq 0 ]; then
		        mkdir -p /tmpfs && mount -t tmpfs -o size=4g tmpfs /tmpfs
		fi
	`)
	if err != nil {
		return err
	}

	dindClusterScriptName := fmt.Sprintf("./dind-cluster-%d.sh", os.Getpid())

	// we write the dind-script.sh file out from go because we need to distribute
	// that .sh script as a go package using dep
	err = ioutil.WriteFile(dindClusterScriptName, []byte(DIND_SCRIPT), 0755)
	if err != nil {
		return err
	}

	// don't leave copies of the script around once we have used it
	defer func() {
		os.Remove(dindClusterScriptName)
	}()

	for i, c := range f {
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

			// XXX the following only works if overlay is working
			err := System("bash", "-c", fmt.Sprintf(`
			set -xe
			mkdir -p /dotmesh-test-pools
			MOUNTPOINT=/dotmesh-test-pools
			NODE=%s
			if [ $(mount |grep $MOUNTPOINT |wc -l) -eq 0 ]; then
				echo "Creating and bind-mounting shared $MOUNTPOINT"
				mkdir -p $MOUNTPOINT && \
				mount --bind $MOUNTPOINT $MOUNTPOINT && \
				mount --make-shared $MOUNTPOINT;
			fi
			EXTRA_DOCKER_ARGS="-v /dotmesh-test-pools:/dotmesh-test-pools:rshared -v /var/run/docker.sock:/hostdocker.sock %s " \
			CNI_PLUGIN=weave \
				%s bare $NODE %s
			sleep 1
			echo "About to run docker exec on $NODE"
			docker exec -t $NODE bash -c '
				set -xe
				# from dind::fix-mounts
				mount --make-shared /lib/modules/
				mount --make-shared /run
			    echo "%s '$(hostname)'.local" >> /etc/hosts
				sed -i "s/rundocker/rundocker \
					--insecure-registry '$(hostname)'.local:80/" \
					/etc/systemd/system/docker.service.d/20-fs.conf
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
					sed -i "s/rundocker/rundocker \
						--insecure-registry '$(hostname)'.local:80/" \
						/etc/systemd/system/docker.service.d/20-fs.conf
					systemctl daemon-reload
					systemctl restart docker
				'
			fi
			`, node, mountDockerAuth, dindClusterScriptName, c.RunArgs(i, j), HOST_IP_FROM_CONTAINER, HOST_IP_FROM_CONTAINER))
			if err != nil {
				return err
			}
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
		log.Printf("WAITING FOR THE SIGNAL")
		s := <-c
		log.Printf("Got signal:", s)
		for _, f := range globalCleanupFuncs {
			log.Printf("RUNNING CLEANUP FUNC")
			f()
		}
		log.Printf("DONE CLEANUP")
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
				"Waiting for /dotmesh-test-cleanup.lock to be deleted " +
					"by some other cleanup process finishing...",
			)
			time.Sleep(1 * time.Second)
			if _, err := os.Stat(lockfile); os.IsNotExist(err) {
				return
			}
		}
	}
	// if path doesn't exist, create it and clean it up on return
	if _, err := os.Stat(lockfile); os.IsNotExist(err) {
		os.OpenFile(lockfile, os.O_RDONLY|os.O_CREATE, 0666)
		defer os.Remove(lockfile)
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
					fmt.Printf("can't deduce clusterNum: %s", cn)
					return
				}

				nn, err := strconv.Atoi(n.NodeNum)
				if err != nil {
					fmt.Printf("can't deduce nodeNum: %s", nn)
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
					fmt.Printf("erk during teardown %s\n", err)
				}

				// workaround https://github.com/docker/docker/issues/20398
				err = System("docker", "network", "disconnect", "-f", "bridge", node)
				if err != nil {
					fmt.Printf("erk during network force-disconnect %s\n", err)
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
				// cleanup zpool data directories
				err = System("bash", "-c", fmt.Sprintf(`rm -rf /dotmesh-test-pools/testpool-%s*`, nodeSuffix))
				if err != nil {
					fmt.Printf("err cleaning up test pools dirs: %s\n", err)
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

func docker(node string, cmd string, env map[string]string) (string, error) {
	envString := ""
	if env != nil {
		for name, value := range env {
			envString += name + "=" + value + " "
		}
	}

	c := exec.Command("docker", "exec", "-i", node, "bash", "-c", envString+cmd)

	var b bytes.Buffer

	o := io.MultiWriter(&b, os.Stdout)
	e := io.MultiWriter(&b, os.Stderr)

	c.Stdout = o
	c.Stderr = e
	err := c.Run()
	return string(b.Bytes()), err

}

func RunOnNodeErr(node string, cmd string) (string, error) {
	return docker(node, cmd, nil)
}

func dockerSystem(node string, cmd string) error {
	return System("docker", "exec", "-i", node, "sh", "-c", cmd)
}

func RunOnNode(t *testing.T, node string, cmd string) {
	fmt.Printf("RUNNING on %s: %s\n", node, cmd)
	debugEnv := map[string]string{"DEBUG_MODE": "1"}
	s, err := docker(node, cmd, debugEnv)
	if err != nil {
		t.Error(fmt.Errorf("%s while running %s on %s: %s", err, cmd, node, s))
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

func LocalImage(service string) string {
	var registry string
	// expected format: quay.io/dotmesh for example
	if reg := os.Getenv("CI_DOCKER_REGISTRY"); reg != "" {
		registry = reg
	} else {
		hostname, err := os.Hostname()
		if err != nil {
			panic(err)
		}
		registry = fmt.Sprintf("%s.local:80/dotmesh", hostname)
	}

	tag := os.Getenv("CI_DOCKER_TAG")
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
	return fmt.Sprintf("%s/%s:%s", registry, service, tag)
}

func localEtcdImage() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s.local:80/dotmesh/etcd:v3.0.15", hostname)
}

func localImageArgs() string {
	logSuffix := ""
	if os.Getenv("DISABLE_LOG_AGGREGATION") == "" {
		logSuffix = fmt.Sprintf(" --log %s", HOST_IP_FROM_CONTAINER)
	}
	traceSuffix := ""
	if os.Getenv("DISABLE_TRACING") == "" {
		traceSuffix = fmt.Sprintf(" --trace %s", HOST_IP_FROM_CONTAINER)
	}
	regSuffix := ""
	return ("--image " + LocalImage("dotmesh-server") + " --etcd-image " + localEtcdImage() +
		" --docker-api-version 1.23 --discovery-url http://" + HOST_IP_FROM_CONTAINER + ":8087" +
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
}

type Cluster struct {
	DesiredNodeCount int
	Env              map[string]string
	ClusterArgs      string
	Nodes            []Node
}

type Kubernetes struct {
	DesiredNodeCount int
	Nodes            []Node
	StorageMode      string
	DindStorage      bool
}

type Pair struct {
	From       Node
	To         Node
	RemoteName string
}

func NewCluster(desiredNodeCount int) *Cluster {
	emptyEnv := make(map[string]string)
	return NewClusterWithArgs(desiredNodeCount, emptyEnv, "")
}

func NewClusterWithEnv(desiredNodeCount int, env map[string]string) *Cluster {
	return NewClusterWithArgs(desiredNodeCount, env, "")
}

// custom arguments that are passed through to `dm cluster {init,join}`
func NewClusterWithArgs(desiredNodeCount int, env map[string]string, args string) *Cluster {
	env["DOTMESH_UPGRADES_URL"] = "" //set default test env vars
	return &Cluster{DesiredNodeCount: desiredNodeCount, Env: env, ClusterArgs: args}
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

func NodeName(now int64, i, j int) string {
	return nodeName(now, i, j)
}

func poolId(now int64, i, j int) string {
	return fmt.Sprintf("testpool-%d-%d-node-%d", now, i, j)
}

func NodeFromNodeName(t *testing.T, now int64, i, j int, clusterName string) Node {
	nodeIP := strings.TrimSpace(OutputFromRunOnNode(t,
		nodeName(now, i, j),
		`ifconfig eth0 | grep "inet addr" | cut -d ':' -f 2 | cut -d ' ' -f 1`,
	))
	dotmeshConfig, err := docker(
		nodeName(now, i, j),
		"cat /root/.dotmesh/config",
		nil,
	)
	var apiKey string
	if err != nil {
		fmt.Printf("no dm config found, proceeding without recording apiKey\n")
	} else {
		fmt.Printf("dm config on %s: %s\n", nodeName(now, i, j), dotmeshConfig)
		m := struct {
			Remotes struct{ Local struct{ ApiKey string } }
		}{}
		json.Unmarshal([]byte(dotmeshConfig), &m)
		apiKey = m.Remotes.Local.ApiKey
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
	}
}

func (f Federation) Start(t *testing.T) error {
	now := time.Now().UnixNano()
	err := testSetup(t, f, now)
	if err != nil {
		return err
	}
	LogTiming("setup")

	for i, c := range f {
		fmt.Printf("==== GOING FOR %d, %+v ====\n", i, c)
		err = c.Start(t, now, i)
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
					_, err := docker(
						pair.From.Container,
						fmt.Sprintf(
							"echo %s |dm remote add %s admin@%s",
							pair.To.ApiKey,
							pair.RemoteName,
							pair.To.IP,
						),
						nil,
					)
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
			RunOnNode(t, pair.From.Container, "dm remote switch local")
		}
	}
	return nil
}

type Startable interface {
	GetNode(int) Node
	GetNodes() []Node
	AppendNode(Node)
	GetDesiredNodeCount() int
	Start(*testing.T, int64, int) error
	RunArgs(int, int) string
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

func (c *Kubernetes) Start(t *testing.T, now int64, i int) error {
	if c.DesiredNodeCount == 0 {
		panic("no such thing as a zero-node cluster")
	}

	/*
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
	*/

	/*

		hostname, err := os.Hostname()
		if err != nil {
			return err
		}

		// pre-pull all the container images Kubernetes needs to use, tag them to
		// trick it into not downloading anything.
			finishing := make(chan bool)
			for j := 0; j < c.DesiredNodeCount; j++ {
				go func(j int) {
					// Use the locally build dotmesh server image as the "latest" image in
					// the test containers.
					for _, imageName := range []string{"dotmesh-server", "dotmesh-dynamic-provisioner", "dotmesh-operator", "dind-dynamic-provisioner"} {
						st, err := docker(
							nodeName(now, i, j),
							fmt.Sprintf(
								"docker pull %s.local:80/dotmesh/%s:latest && "+
									"docker tag %s.local:80/dotmesh/%s:latest "+
									"quay.io/dotmesh/%s:latest",
								hostname, imageName,
								hostname, imageName,
								imageName,
							),
							nil)
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
	*/

	logAddr := ""
	if os.Getenv("DISABLE_LOG_AGGREGATION") == "" {
		logAddr = HOST_IP_FROM_CONTAINER
	}

	// Move k8s root dir into /dotmesh-test-pools on every node.

	// This is required for the tests of k8s using PV storage with the
	// DIND provisioner to work; the container mountpoints must be
	// consistent between the actual host and the dind kubelet node, or
	// ZFS will barf on them. Putting the k8s root dir in
	// /dotmesh-test-pools means the paths are consistent across all
	// containers, as we keep the same filesystem mounted there
	// throughput.
	for j := 0; j < c.DesiredNodeCount; j++ {
		node := nodeName(now, i, j)
		path := fmt.Sprintf("/dotmesh-test-pools/k8s-%s", node)
		cmd := fmt.Sprintf("sed -i 's!hyperkube kubelet !hyperkube kubelet --root-dir %s !' /lib/systemd/system/kubelet.service && mkdir -p %s && systemctl restart kubelet", path, path)
		st, err := docker(
			node,
			cmd,
			nil,
		)
		fmt.Printf("FUDGING KUBELET DIR: %s\n%s\n", cmd, st)
		if err != nil {
			return err
		}
	}

	// TODO regex the following yamels to refer to the newly pushed
	// dotmesh container image, rather than the latest stable

	err := System("bash", "-c",
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

	st, err := docker(
		nodeName(now, i, 0),
		"touch /dind/flexvolume_driver && "+
			"systemctl start kubelet && "+
			"wrapkubeadm init --ignore-preflight-errors=all && "+
			"mkdir /root/.kube && cp /etc/kubernetes/admin.conf /root/.kube/config && "+
			// Make kube-dns faster; trick copied from dind-cluster-v1.7.sh
			"kubectl get deployment kube-dns -n kube-system -o json | jq '.spec.template.spec.containers[0].readinessProbe.initialDelaySeconds = 3|.spec.template.spec.containers[0].readinessProbe.periodSeconds = 3' | kubectl apply --force -f -",
		nil,
	)
	if err != nil {
		return err
	}

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
	// now install dotmesh yaml (setting initial admin pw)

	configMapCmd := fmt.Sprintf(
		"kubectl create configmap -n dotmesh configuration --from-literal=upgradesUrl= '--from-literal=poolNamePrefix=%s-#HOSTNAME#-' '--from-literal=local.poolLocation=/dotmesh-test-pools/%s-#HOSTNAME#' --from-literal=logAddress=%s --from-literal=storageMode=%s --from-literal=pvcPerNode.storageClass=dind-pv",
		poolId(now, i, 0),
		poolId(now, i, 0),
		logAddr,
		c.StorageMode,
	)

	st, err = docker(
		nodeName(now, i, 0),
		"echo '#### STARTING WEAVE-NET' && "+
			"kubectl apply -f /dotmesh-kube-yaml/weave-net.yaml && "+
			"echo '#### CREATING DOTMESH CONFIGURATION' && "+
			"kubectl create namespace dotmesh && "+
			"echo -n 'secret123' > dotmesh-admin-password.txt && "+
			"echo -n 'FAKEAPIKEY' > dotmesh-api-key.txt && "+
			"kubectl create secret generic dotmesh "+
			"    --from-file=./dotmesh-admin-password.txt --from-file=./dotmesh-api-key.txt -n dotmesh && "+
			"rm dotmesh-admin-password.txt && "+
			"rm dotmesh-api-key.txt && "+
			// create configmap
			configMapCmd+" && "+
			// install etcd operator on the cluster
			"echo '#### STARTING ETCD OPERATOR' && "+
			"kubectl apply -f /dotmesh-kube-yaml/etcd-operator-clusterrole.yaml && "+
			"kubectl apply -f /dotmesh-kube-yaml/etcd-operator-dep.yaml && "+
			// install dotmesh once on the master (retry because etcd operator
			// needs to initialize)
			"sleep 1 && "+
			"echo '#### STARTING ETCD' && "+
			"while ! kubectl apply -f /dotmesh-kube-yaml/dotmesh-etcd-cluster.yaml; do sleep 2; "+KUBE_DEBUG_CMD+"; done && "+
			"echo '#### STARTING DOTMESH' && "+
			"kubectl apply -f /dotmesh-kube-yaml/dotmesh.yaml",
		nil,
	)
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
				/usr/libexec/kubernetes/kubelet-plugins/volume/exec/dotmesh.io~dind
			docker cp ../cmd/dotmesh-server/target/dind-flexvolume \
				$NODE:/usr/libexec/kubernetes/kubelet-plugins/volume/exec/dotmesh.io~dind/dind
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

	// Add the nodes at the end, because NodeFromNodeName expects dotmesh
	// config to be set up.
	for j := 0; j < c.DesiredNodeCount; j++ {
		for {
			st, err = docker(
				nodeName(now, i, j),
				"echo FAKEAPIKEY | dm remote add local admin@127.0.0.1",
				nil,
			)

			if err != nil {
				time.Sleep(time.Second * 2)
				st, debugErr := docker(
					nodeName(now, i, 0),
					KUBE_DEBUG_CMD,
					nil,
				)
				if debugErr != nil {
					log.Printf("Error debugging kubctl status:  %v, %s", debugErr, st)
				}

				log.Printf("Error adding remote:  %v, retrying..", err)
			} else {
				break
			}
		}
		c.Nodes[j] = NodeFromNodeName(t, now, i, j, clusterName)
	}

	// Wait for etcd to settle before firing up volumes. This works
	// around https://github.com/dotmesh-io/dotmesh/issues/62 so
	// removing this will be a good test of that issue :-)
	fmt.Printf("Waiting for etcd...\n")
	for {
		resp := OutputFromRunOnNode(t, c.Nodes[0].Container, "kubectl describe etcd dotmesh-etcd-cluster -n dotmesh | grep Type:")
		if err != nil {
			return err
		}
		if strings.Contains(resp, "Available") {
			fmt.Printf("etcd is up!\n")
			break
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
	}

	// For each node, wait until we can talk to dm from that node before
	// proceeding.
	for j := 0; j < c.DesiredNodeCount; j++ {
		nodeName := nodeName(now, i, j)
		err := TryUntilSucceeds(func() error {
			// Check that the dm API works
			_, err := RunOnNodeErr(nodeName, "dm list")
			if err != nil {
				return err
			}

			// Check that the docker volume plugin socket works
			_, err = RunOnNodeErr(
				nodeName,
				"echo 'GET / HTTP/1.0' | socat /run/docker/plugins/dm.sock -",
			)
			return err
		}, fmt.Sprintf("running dm list on %s", nodeName))
		if err != nil {
			return err
		}
	}

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

func (c *Cluster) Start(t *testing.T, now int64, i int) error {
	// init the first node in the cluster, join the rest
	if c.DesiredNodeCount == 0 {
		panic("no such thing as a zero-node cluster")
	}

	dmInitCommand := "EXTRA_HOST_COMMANDS='echo Testing EXTRA_HOST_COMMANDS' dm cluster init " + localImageArgs() +
		" --use-pool-dir /dotmesh-test-pools/" + poolId(now, i, 0) +
		" --use-pool-name " + poolId(now, i, 0) +
		" --dotmesh-upgrades-url ''" +
		c.ClusterArgs

	fmt.Printf("running dm cluster init with following command: %s\n", dmInitCommand)

	st, err := docker(
		nodeName(now, i, 0), dmInitCommand, c.Env)

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
		_, err = docker(nodeName(now, i, j), fmt.Sprintf(
			"dm cluster join %s %s %s",
			localImageArgs()+" --use-pool-dir /dotmesh-test-pools/"+poolId(now, i, j),
			joinUrl,
			" --use-pool-name "+poolId(now, i, j)+c.ClusterArgs,
		), c.Env)
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
