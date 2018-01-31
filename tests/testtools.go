package main

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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/rpc/v2/json2"
)

var timings map[string]float64
var lastTiming int64

const HOST_IP_FROM_CONTAINER = "10.192.0.1"

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if strings.Contains(a, str) {
			return true
		}
	}
	return false
}

func startTiming() {
	lastTiming = time.Now().UnixNano()
	timings = make(map[string]float64)
}

func logTiming(tag string) {
	now := time.Now().UnixNano()
	timings[tag] = float64(now-lastTiming) / (1000 * 1000 * 1000)
	lastTiming = now
}

func dumpTiming() {
	fmt.Printf("=== TIMING ===\n")
	for tag, timing := range timings {
		fmt.Printf("%s => %.2f\n", tag, timing)
	}
	fmt.Printf("=== END TIMING ===\n")
	timings = map[string]float64{}
}

func system(cmd string, args ...string) error {
	log.Printf("[system] running %s %s", cmd, args)
	c := exec.Command(cmd, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Run()
}

func silentSystem(cmd string, args ...string) error {
	log.Printf("[silentSystem] running %s %s", cmd, args)
	c := exec.Command(cmd, args...)
	return c.Run()
}

func tryUntilSucceeds(f func() error, desc string) error {
	attempt := 0
	for {
		err := f()
		if err != nil {
			if attempt > 10 {
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

func testMarkForCleanup(f Federation) {
	for _, c := range f {
		for _, n := range c.GetNodes() {
			node := n.Container
			err := tryUntilSucceeds(func() error {
				return system("bash", "-c", fmt.Sprintf(
					`docker exec -t %s bash -c 'touch /CLEAN_ME_UP'`, node,
				))
			}, fmt.Sprintf("marking %s for cleanup", node))
			if err != nil {
				fmt.Printf("Error marking %s for cleanup: %s, giving up.\n", node, err)
				panic("This is bad. Stop everything and clean up manually!")
			}
		}
	}
}

func testSetup(f Federation, stamp int64) error {
	err := system("bash", "-c", `
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

	for i, c := range f {
		for j := 0; j < c.GetDesiredNodeCount(); j++ {
			node := nodeName(stamp, i, j)
			fmt.Printf(">>> Using RunArgs %s\n", c.RunArgs(i, j))

			// XXX the following only works if overlay is working
			err := system("bash", "-c", fmt.Sprintf(`
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
			EXTRA_DOCKER_ARGS="-v /dotmesh-test-pools:/dotmesh-test-pools:rshared" \
			DIND_IMAGE="quay.io/lukemarsden/kubeadm-dind-cluster:v1.7-hostport" \
			CNI_PLUGIN=weave \
				../kubernetes/dind-cluster-v1.7.sh bare $NODE %s
			sleep 1
			echo "About to run docker exec on $NODE"
			docker exec -t $NODE bash -c '
				set -xe
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
			docker cp ../binaries/Linux/dm $NODE:/usr/local/bin/dm
			`, node, c.RunArgs(i, j), HOST_IP_FROM_CONTAINER))
			if err != nil {
				return err
			}

			// a trick to speed up CI builds where we slurp a tarball
			// from the host docker into the dind container after pulling
			// it first to make sure we have the latest based on CI_HASH
			//
			// we get the speed boost of the existing layers that way
			// and sometimes the builder will have just built the image
			// NOTE: we pull on the host to make sure that we pull latest CI
			// images from quay even if we did not build it - that is done before
			// sending the tarball into the dind container
			//
			// TODO: test if streaming the tarball like this is actualy quicker
			// than downloading from the remote registry

			injectImages := []string{}
			// allow the injected images to be overriden by a comma-delimeted string
			// useful for frontend test where there are lots of images
			INJECT_HOST_IMAGES := os.Getenv("INJECT_HOST_IMAGES")
			if INJECT_HOST_IMAGES != "" {
				injectImages = strings.Split(INJECT_HOST_IMAGES, ",")
			}

			// make sure the host has the latest image in case it was not the builder
			// the tests are read-only in terms of images so we mount the host
			// docker images /var/lib folder on the strict assumption we are not
			// building anything from inside the tests
			for _, image := range injectImages {
				if image == "" {
					continue
				}
				if !strings.Contains(image, "/") {
					image = localImage(image)
				}
				err := system("bash", "-c", fmt.Sprintf(`
					set -xe
					docker pull %s
					docker save %s | docker exec -i %s docker load
				`, image, image, node))
				if err != nil {
					return err
				}
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

func teardownFinishedTestRuns() {
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
	err := system("../scripts/mark-old-cleanup.sh")
	if err != nil {
		log.Printf("Error running mark-old-cleanup.sh: %s", err)
	}

	cs, err := exec.Command(
		"bash", "-c", "docker ps --format {{.Names}} |grep cluster- || true",
	).Output()
	if err != nil {
		panic(err)
	}
	log.Printf("[teardownFinishedTestRuns] cs = %s", cs)
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
				existsErr := silentSystem("docker", "inspect", node)
				notExists := false
				if existsErr != nil {
					// must have been a single-node test, don't return on our
					// behalf, we have zpool etc cleanup to do
					notExists = true
				}

				err = system("docker", "exec", "-i", node, "test", "-e", "/CLEAN_ME_UP")
				if err != nil {
					fmt.Printf("not cleaning up %s because /CLEAN_ME_UP not found\n", node)
					if !notExists {
						return
					}
				}

				err = system("docker", "rm", "-f", "-v", node)
				if err != nil {
					fmt.Printf("erk during teardown %s\n", err)
				}

				// workaround https://github.com/docker/docker/issues/20398
				err = system("docker", "network", "disconnect", "-f", "bridge", node)
				if err != nil {
					fmt.Printf("erk during network force-disconnect %s\n", err)
				}

				// cleanup after a previous test run; this is a pretty gross hack
				err = system("bash", "-c", fmt.Sprintf(`
					for X in $(findmnt -P -R /tmpfs |grep %s); do
						eval $X
						if [ "$TARGET" != "/tmpfs" ]; then
							umount $TARGET >/dev/null 2>&1 || true
						fi
					done
					rm -rf /tmpfs/%s`, node, node),
				)
				if err != nil {
					fmt.Printf("erk during teardown %s\n", err)
				}

				fmt.Printf("=== Cleaned up node %s\n", node)
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
		}()
		out, err := exec.Command("docker", "volume", "prune", "-f").Output()
		if err != nil {
			fmt.Printf("unable to prune docker volumes: %s, %s\n", err, out)
		}
	}
	err = system("docker", "container", "prune", "-f")
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

	c := exec.Command("docker", "exec", "-i", node, "sh", "-c", envString+cmd)

	var b bytes.Buffer

	o := io.MultiWriter(&b, os.Stdout)
	e := io.MultiWriter(&b, os.Stderr)

	c.Stdout = o
	c.Stderr = e
	err := c.Run()
	return string(b.Bytes()), err

}

func dockerSystem(node string, cmd string) error {
	return system("docker", "exec", "-i", node, "sh", "-c", cmd)
}

func d(t *testing.T, node string, cmd string) {
	fmt.Printf("RUNNING on %s: %s\n", node, cmd)
	s, err := docker(node, cmd, nil)
	if err != nil {
		t.Error(fmt.Errorf("%s while running %s on %s: %s", err, cmd, node, s))
	}
}

// e.g. kubectlApply(t, node1, `yaml...`)
func kubectlApply(t *testing.T, node string, input string) {
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

func s(t *testing.T, node string, cmd string) string {
	s, err := docker(node, cmd, nil)
	if err != nil {
		t.Error(fmt.Errorf("%s while running %s on %s: %s", err, cmd, node, s))
	}
	return s
}

func helperImage(service string) string {
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
	tag := "latest"
	return fmt.Sprintf("%s/%s:%s", registry, service, tag)
}

func localImage(service string) string {
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
	return ("--image " + localImage("dotmesh-server") + " --etcd-image " + localEtcdImage() +
		" --docker-api-version 1.23 --discovery-url http://" + HOST_IP_FROM_CONTAINER + ":8087" +
		logSuffix + traceSuffix + regSuffix)
}

// TODO a test which exercise `dm cluster init --count 3` or so

func dockerRun(v ...string) string {
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

func dockerRunDetached(v ...string) string {
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

func uniqName() string {
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
}

type Pair struct {
	From Node
	To   Node
}

func NewCluster(desiredNodeCount int) *Cluster {
	emptyEnv := make(map[string]string)
	return &Cluster{DesiredNodeCount: desiredNodeCount, Env: emptyEnv, ClusterArgs: ""}
}

func NewClusterWithEnv(desiredNodeCount int, env map[string]string) *Cluster {
	return &Cluster{DesiredNodeCount: desiredNodeCount, Env: env, ClusterArgs: ""}
}

// custom arguments that are passed through to `dm cluster {init,join}`
func NewClusterWithArgs(desiredNodeCount int, env map[string]string, args string) *Cluster {
	return &Cluster{DesiredNodeCount: desiredNodeCount, Env: env, ClusterArgs: args}
}

func NewKubernetes(desiredNodeCount int) *Kubernetes {
	return &Kubernetes{DesiredNodeCount: desiredNodeCount}
}

type Federation []Startable

func nodeName(now int64, i, j int) string {
	return fmt.Sprintf("cluster-%d-%d-node-%d", now, i, j)
}

func poolId(now int64, i, j int) string {
	return fmt.Sprintf("testpool-%d-%d-node-%d", now, i, j)
}

func NodeFromNodeName(t *testing.T, now int64, i, j int, clusterName string) Node {
	nodeIP := strings.TrimSpace(s(t,
		nodeName(now, i, j),
		`ifconfig eth0 | grep "inet addr" | cut -d ':' -f 2 | cut -d ' ' -f 1`,
	))
	dotmeshConfig := s(t,
		nodeName(now, i, j),
		"cat /root/.dotmesh/config",
	)
	fmt.Printf("dm config on %s: %s\n", nodeName(now, i, j), dotmeshConfig)

	// /root/.dotmesh/admin-password.txt is created on docker
	// clusters, but k8s clusters are configured from k8s secrets so
	// there's no automatic password generation; the value we show here
	// is what we hardcode as the password.
	password := s(t,
		nodeName(now, i, j),
		"sh -c 'if [ -f /root/.dotmesh/admin-password.txt ]; then cat /root/.dotmesh/admin-password.txt; else echo -n FAKEAPIKEY; fi'",
	)

	fmt.Printf("dm password on %s: %s\n", nodeName(now, i, j), password)

	m := struct {
		Remotes struct{ Local struct{ ApiKey string } }
	}{}
	json.Unmarshal([]byte(dotmeshConfig), &m)

	return Node{
		ClusterName: clusterName,
		Container:   nodeName(now, i, j),
		IP:          nodeIP,
		ApiKey:      m.Remotes.Local.ApiKey,
		Password:    password,
	}
}

func (f Federation) Start(t *testing.T) error {
	now := time.Now().UnixNano()
	err := testSetup(f, now)
	if err != nil {
		return err
	}
	logTiming("setup")

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
					From: node,
					To:   first,
				})
			}
		}
	}
	for _, pair := range pairs {
		found := false
		for _, remote := range strings.Split(s(t, pair.From.Container, "dm remote"), "\n") {
			if remote == pair.To.ClusterName {
				found = true
			}
		}
		if !found {
			d(t, pair.From.Container, fmt.Sprintf(
				"echo %s |dm remote add %s admin@%s",
				pair.To.ApiKey,
				pair.To.ClusterName,
				pair.To.IP,
			))
			res := s(t, pair.From.Container, "dm remote -v")
			if !strings.Contains(res, pair.To.ClusterName) {
				t.Errorf("can't find %s in %s's remote config", pair.To.ClusterName, pair.From.ClusterName)
			}
			d(t, pair.From.Container, "dm remote switch local")
		}
	}
	return nil
}

type Startable interface {
	GetNode(int) Node
	GetNodes() []Node
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

func (c *Kubernetes) GetDesiredNodeCount() int {
	return c.DesiredNodeCount
}

func (c *Kubernetes) Start(t *testing.T, now int64, i int) error {
	if c.DesiredNodeCount == 0 {
		panic("no such thing as a zero-node cluster")
	}

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
	finishing := make(chan bool)
	for j := 0; j < c.DesiredNodeCount; j++ {
		go func(j int) {
			// Use the locally build dotmesh server image as the "latest" image in
			// the test containers.
			st, err := docker(
				nodeName(now, i, j),
				fmt.Sprintf(
					"docker pull %s.local:80/dotmesh/dotmesh-server:latest && "+
						"docker tag %s.local:80/dotmesh/dotmesh-server:latest "+
						"quay.io/dotmesh/dotmesh-server:latest",
					hostname, hostname,
				),
				nil,
			)
			if err != nil {
				panic(st)
			}
			for fqImage, localName := range cache {
				st, err := docker(
					nodeName(now, i, j),
					/*
					   docker pull $local_name
					   docker tag $local_name $fq_image
					*/
					fmt.Sprintf(
						"docker pull %s.local:80/%s && "+
							"docker tag %s.local:80/%s %s",
						hostname, localName, hostname, localName, fqImage,
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

	// TODO regex the following yamels to refer to the newly pushed
	// dotmesh container image, rather than the latest stable
	err = system("bash", "-c",
		fmt.Sprintf(
			`MASTER=%s
			docker exec $MASTER mkdir /dotmesh-kube-yaml
			for X in ../kubernetes/*.yaml; do docker cp $X $MASTER:/dotmesh-kube-yaml/; done
			docker exec $MASTER sed -i 's/quay.io\/dotmesh\/dotmesh-server:latest/%s/' /dotmesh-kube-yaml/dotmesh.yaml
			docker exec $MASTER sed -i 's/quay.io\/dotmesh\/dotmesh-dynamic-provisioner:latest/%s/' /dotmesh-kube-yaml/dotmesh.yaml
			docker exec $MASTER sed -i 's/value: pool/value: %s-\#HOSTNAME\#/' /dotmesh-kube-yaml/dotmesh.yaml
			docker exec $MASTER sed -i 's/value: \/var\/lib\/docker\/dotmesh/value: %s-\#HOSTNAME\#/' /dotmesh-kube-yaml/dotmesh.yaml
			docker exec $MASTER sed -i 's/"" \# LOG_ADDR/%s/' /dotmesh-kube-yaml/dotmesh.yaml
			docker exec $MASTER sed -i 's/size: 3/size: 1/' /dotmesh-kube-yaml/dotmesh.yaml
			`,
			nodeName(now, i, 0),
			strings.Replace(localImage("dotmesh-server"), "/", "\\/", -1),
			strings.Replace(localImage("dotmesh-dynamic-provisioner"), "/", "\\/", -1),
			// need to somehow number the instances, did this by modifying
			// require_zfs.sh to include the hostname in the pool name to make
			// them unique... TODO: make sure we clear these up
			poolId(now, i, 0),
			"\\/dotmesh-test-pools\\/"+poolId(now, i, 0),
			HOST_IP_FROM_CONTAINER,
		),
	)
	if err != nil {
		return err
	}
	st, err := docker(
		nodeName(now, i, 0),
		"rm /etc/machine-id && systemd-machine-id-setup && touch /dind/flexvolume_driver && "+
			"systemctl start kubelet && "+
			"kubeadm init --kubernetes-version=v1.7.6 --pod-network-cidr=10.244.0.0/16 --skip-preflight-checks && "+
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
			"rm /etc/machine-id && systemd-machine-id-setup && touch /dind/flexvolume_driver && "+
				"systemctl start kubelet && "+
				"kubeadm join --skip-preflight-checks %s",
			joinArgs,
		), nil)
		if err != nil {
			return err
		}
		logTiming("join_" + poolId(now, i, j))
	}
	// now install dotmesh yaml (setting initial admin pw)
	st, err = docker(
		nodeName(now, i, 0),
		"kubectl apply -f /dotmesh-kube-yaml/weave-net.yaml && "+
			"kubectl create namespace dotmesh && "+
			"echo -n 'secret123' > dotmesh-admin-password.txt && "+
			"echo -n 'FAKEAPIKEY' > dotmesh-api-key.txt && "+
			"kubectl create secret generic dotmesh "+
			"    --from-file=./dotmesh-admin-password.txt --from-file=./dotmesh-api-key.txt -n dotmesh && "+
			"rm dotmesh-admin-password.txt && "+
			"rm dotmesh-api-key.txt && "+
			// install etcd operator on the cluster
			"kubectl apply -f /dotmesh-kube-yaml/etcd-operator-clusterrole.yaml && "+
			"kubectl apply -f /dotmesh-kube-yaml/etcd-operator-dep.yaml && "+
			// install dotmesh once on the master (retry because etcd operator
			// needs to initialize)
			"sleep 1 && "+
			"while ! kubectl apply -f /dotmesh-kube-yaml/dotmesh.yaml; do sleep 1; done",
		nil,
	)
	if err != nil {
		return err
	}
	// Add the nodes at the end, because NodeFromNodeName expects dotmesh
	// config to be set up.
	for j := 0; j < c.DesiredNodeCount; j++ {
		st, err = docker(
			nodeName(now, i, j),
			// Restart kubelet so that dotmesh-installed flexvolume driver
			// gets activated.  This won't be necessary after Kubernetes 1.8.
			// https://github.com/Mirantis/kubeadm-dind-cluster/issues/40
			`while ! (
					echo secret123 | dm remote add local admin@127.0.0.1 &&
					systemctl restart kubelet
				); do
				echo 'retrying...' && sleep 1
			done`,
			nil,
		)
		if err != nil {
			return err
		}
		c.Nodes = append(c.Nodes, NodeFromNodeName(t, now, i, j, clusterName))
	}

	// Wait for etcd to settle before firing up volumes. This works
	// around https://github.com/dotmesh-io/dotmesh/issues/62 so
	// removing this will be a good test of that issue :-)
	fmt.Printf("Waiting for etcd...\n")
	for {
		resp := s(t, c.Nodes[0].Container, "kubectl describe etcd dotmesh-etcd-cluster -n dotmesh | grep Type:")
		if err != nil {
			return err
		}
		if strings.Contains(resp, "Ready") {
			fmt.Printf("etcd is up!\n")
			break
		}
		fmt.Printf("etcd is not up... %#v\n", resp)
		time.Sleep(time.Second)
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
		c.ClusterArgs

	fmt.Printf("running dm cluster init with following command: %s\n", dmInitCommand)

	st, err := docker(
		nodeName(now, i, 0), dmInitCommand, c.Env)

	if err != nil {
		return err
	}
	clusterName := fmt.Sprintf("cluster_%d", i)
	c.Nodes = append(c.Nodes, NodeFromNodeName(t, now, i, 0, clusterName))
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
	logTiming("init_" + poolId(now, i, 0))
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
		c.Nodes = append(c.Nodes, NodeFromNodeName(t, now, i, j, clusterName))

		logTiming("join_" + poolId(now, i, j))
	}
	return nil
}

func createDockerNetwork(t *testing.T, node string) {
	fmt.Printf("Creating Docker network on %s", node)
	d(t, node, fmt.Sprintf(`
		docker network create dotmesh-dev  &>/dev/null || true
	`))
	d(t, node, fmt.Sprintf(`
		docker network connect dotmesh-dev dotmesh-server-inner
	`))
}

func startBilling(t *testing.T, node string, apiKey string) {
	fmt.Printf("Starting Billing server on %s", node)
	billingImage := localImage("dotmesh-billing")
	d(t, node, fmt.Sprintf(`
		docker run -d \
			--name dotmesh-billing \
			--net dotmesh-dev \
			-e DOTMESH_SERVER_HOSTNAME=dotmesh-server-inner \
			-e DOTMESH_SERVER_PORT=6969 \
			-e DOTMESH_SERVER_API_KEY=%s \
			-e COMMUNICATIONS_SERVER_HOSTNAME=dotmesh-communications \
			-e COMMUNICATIONS_SERVER_PORT=80 \
			-e COMMUNICATIONS_ACTIVE=1 \
			-e STRIPE_PUBLIC_KEY=%s \
			-e STRIPE_SECRET_KEY=%s \
			-e STRIPE_TEST_WEBHOOKS_ENABLED=1 \
			-e CORS_DOMAINS=http://dotmesh-server-inner:6969 \
			-e NODE_ENV=production \
			-e BLACKHOLE_DATA_PATH=/tmp \
			%s
	`,
		apiKey,
		os.Getenv("STRIPE_PUBLIC_KEY"),
		os.Getenv("STRIPE_SECRET_KEY"),
		billingImage,
	))
}

func startCommunications(t *testing.T, node string) {
	fmt.Printf("Starting Communications server on %s", node)
	communicationsImage := localImage("dotmesh-communications")
	d(t, node, fmt.Sprintf(`
		docker run -d \
			--name dotmesh-communications \
			--net dotmesh-dev \
			-e NODE_ENV=production \
			-e BLACKHOLE_DATA_PATH=/tmp \
			%s
	`,
		communicationsImage,
	))
}

func startFrontend(t *testing.T, node string) {
	fmt.Printf("Starting Frontend server on %s", node)
	frontendImage := localImage("dotmesh-frontend")
	d(t, node, fmt.Sprintf(`
		docker run -d \
			--name dotmesh-frontend \
			--net dotmesh-dev \
			%s
	`, frontendImage))
}

func startRouter(t *testing.T, node string) {
	fmt.Printf("Starting Router server on %s", node)
	d(t, node, fmt.Sprintf(`
		docker run -d \
			--name dotmesh-router \
			--net dotmesh-dev \
			-p 8080:80 \
			-e NOXY_DEFAULT_HOST=dotmesh-frontend \
	    -e NOXY_BILLING_FRONT=/api/v1 \
	    -e NOXY_BILLING_HOST=dotmesh-billing \
	    -e NOXY_RPC_FRONT=/rpc \
	    -e NOXY_RPC_HOST=dotmesh-server-inner \
	    -e NOXY_RPC_PORT=6969 \
	    -e NOXY_STATUS_FRONT=/status \
	    -e NOXY_STATUS_HOST=dotmesh-server-inner \
	    -e NOXY_STATUS_PORT=6969 \
			%s
	`, "binocarlos/noxy"))
}

func startChromeDriver(t *testing.T, node string) {
	fmt.Printf("Starting Chromedriver server on %s", node)
	chromeDriverImage := helperImage("dotmesh-chromedriver")
	d(t, node, fmt.Sprintf(`
		docker run -d \
			--name dotmesh-chromedriver \
			--net dotmesh-dev \
			-e VNC_ENABLED=true \
			-e EXPOSE_X11=true \
			%s
	`, chromeDriverImage))
}

func startGotty(t *testing.T, node string) {
	fmt.Printf("Starting Gotty server on %s", node)
	gottyImage := helperImage("dotmesh-gotty")
	d(t, node, fmt.Sprintf(`
		docker run -d \
			--name dotmesh-gotty \
			--net dotmesh-dev \
			-e TERM=linux \
			-v /usr/local/bin/dm:/usr/local/bin/dm \
			-v /root/.dotmesh/config:/root/.dotmesh/host_config \
			-v /root/.dotmesh/config:/root/.dotmesh/host_config \
			-v /var/run/docker.sock:/var/run/docker.sock \
			%s
	`, gottyImage))
}

func stopBilling(t *testing.T, node string) {
	billingLogs := s(t, node, "docker logs dotmesh-billing")
	log.Printf("[billing] logs: \n%s", billingLogs)
	d(t, node, "docker rm -f dotmesh-billing || true")
}

func stopCommunications(t *testing.T, node string) {
	communicationsLogs := s(t, node, "docker logs dotmesh-communications")
	log.Printf("[communications] logs: \n%s", communicationsLogs)
	d(t, node, "docker rm -f dotmesh-communications || true")
}

func stopChromeDriver(t *testing.T, node string) {
	d(t, node, "docker rm -f dotmesh-chromedriver || true")
}

func stopGotty(t *testing.T, node string) {
	d(t, node, "docker rm -f dotmesh-gotty || true")
}

func stopFrontend(t *testing.T, node string) {
	d(t, node, "docker rm -f dotmesh-frontend || true")
}

func stopRouter(t *testing.T, node string) {
	d(t, node, "docker rm -f dotmesh-router || true")
}

type UserLogin struct {
	Email    string
	Username string
	Password string
}

var uniqUserNumber int

func uniqLogin() UserLogin {
	uniqUserNumber++
	return UserLogin{
		Email:    fmt.Sprintf("test%d@test.com", uniqUserNumber),
		Username: fmt.Sprintf("test%d", uniqUserNumber),
		Password: "test",
	}
}

// run the frontend tests - then copy the media out onto the dind host
func runFrontendTest(t *testing.T, node string, testName string, login UserLogin) {
	runnerImage := localImage("dotmesh-frontend-test-runner")

	d(t, node, fmt.Sprintf(`
		docker run --rm \
	    --name dotmesh-frontend-test-runner \
	    --net dotmesh-dev \
	    -e "LAUNCH_URL=dotmesh-router/ui" \
	    -e "SELENIUM_HOST=dotmesh-chromedriver" \
	    -e "WAIT_FOR_HOSTS=dotmesh-server-inner:6969 dotmesh-frontend:80 dotmesh-router:80 dotmesh-billing:80 dotmesh-communications:80 dotmesh-chromedriver:4444 dotmesh-chromedriver:6060 dotmesh-gotty:8081" \
	    -e "GOTTY_HOST=dotmesh-gotty:8081" \
	    -e "QUICK=1" \
	    -v /test_media/screenshots:/home/node/screenshots \
	    -v /test_media/videos:/home/node/videos \
	    %s %s
	`,
		runnerImage,
		testName,
	))

}

func copyMedia(node string) error {
	err := system("bash", "-c", fmt.Sprintf(`
		docker exec %s bash -c "tar -C /test_media -c ." > ../frontend_artifacts.tar
	`, node))

	return err
}

func registerUser(node Node, username, email, password string) error {
	fmt.Printf("Registering test user %s on node %s\n", username, node.IP)

	var safeUser struct {
		Id          string
		Name        string
		Email       string
		EmailHash   string
		CustomerId  string
		CurrentPlan string
	}

	err := doRPC(node.IP, "admin", node.ApiKey,
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

func doRPC(hostname, user, apiKey, method string, args interface{}, result interface{}) error {
	url := fmt.Sprintf("http://%s:6969/rpc", hostname)
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

func doSetDebugFlag(hostname, user, apikey, flag, value string) (string, error) {
	var result string

	err := doRPC(hostname, user, apikey,
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

func doAddCollaborator(hostname, user, apikey, namespace, volume, collaborator string) error {
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

	err := doRPC(hostname, user, apikey,
		"DotmeshRPC.List",
		struct {
		}{},
		&volumes)
	if err != nil {
		return err
	}

	volumeID := volumes[namespace][volume].Id

	var result bool
	err = doRPC(hostname, user, apikey,
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
