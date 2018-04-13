package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	lister_v1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const DOTMESH_NAMESPACE = "dotmesh"
const DOTMESH_NODE_LABEL = "dotmesh.io/node"

func GetClientConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

var Version string = "none" // Set at compile time

func main() {
	// When running as a pod in-cluster, a kubeconfig is not needed. Instead
	// this will make use of the service account injected into the pod.
	// However, allow the use of a local kubeconfig as this can make local
	// development & testing easier.
	kubeconfig := flag.String("kubeconfig", "", "Path to a kubeconfig file")

	// We log to stderr because glog will default to logging to a file.
	// By setting this debugging is easier via `kubectl logs`
	flag.Set("logtostderr", "true")
	flag.Parse()

	// Build the client config - optionally using a provided kubeconfig file.
	config, err := GetClientConfig(*kubeconfig)
	if err != nil {
		glog.Fatalf("Failed to load client config: %v", err)
	}
	// Construct the Kubernetes client
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Failed to create kubernetes client: %v", err)
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	newDotmeshController(client).Run(stopCh)
}

type dotmeshController struct {
	client       kubernetes.Interface
	nodeLister   lister_v1.NodeLister
	podLister    lister_v1.PodLister
	nodeInformer cache.Controller
	podInformer  cache.Controller

	updatesNeeded     bool
	updatesNeededLock *sync.Mutex
}

func newDotmeshController(client kubernetes.Interface) *dotmeshController {
	rc := &dotmeshController{
		client:            client,
		updatesNeeded:     false,
		updatesNeededLock: &sync.Mutex{},
	}

	// TODO: listen for node_list_changed, dotmesh_pod_list_changed,
	// config_changed or pv_list_changed

	// TRACK NODES

	nodeIndexer, nodeInformer := cache.NewIndexerInformer(
		&cache.ListWatch{
			ListFunc: func(lo meta_v1.ListOptions) (runtime.Object, error) {
				// FIXME: Add user-configurable selector to only care about certain nodes
				return client.Core().Nodes().List(lo)
			},
			WatchFunc: func(lo meta_v1.ListOptions) (watch.Interface, error) {
				// FIXME: Add user-configurable selector to only care about certain nodes
				return client.Core().Nodes().Watch(lo)
			},
		},
		// The types of objects this informer will return
		&v1.Node{},
		// The resync period of this object. This will force a re-update of all
		// cached objects at this interval.  Every object will trigger the
		// `Updatefunc` even if there have been no actual updates triggered.
		// In some cases you can set this to a very high interval - as you can
		// assume you will see periodic updates in normal operation.  The
		// interval is set low here for demo purposes.
		10*time.Second,
		// Callback Functions to trigger on add/update/delete
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				//				glog.Info("NODE ADD %+v", obj)
				rc.scheduleUpdate()
			},
			UpdateFunc: func(old, new interface{}) {
				//				glog.Info("NODE UPDATE %+v -> %+v", old, new)
				rc.scheduleUpdate()
			},
			DeleteFunc: func(obj interface{}) {
				//				glog.Info("NODE DELETE %+v", obj)
				rc.scheduleUpdate()
			},
		},
		cache.Indexers{},
	)

	rc.nodeInformer = nodeInformer
	// NodeLister avoids some boilerplate code (e.g. convert runtime.Object to
	// *v1.node)
	rc.nodeLister = lister_v1.NewNodeLister(nodeIndexer)

	// TRACK DOTMESH PODS

	podIndexer, podInformer := cache.NewIndexerInformer(
		&cache.ListWatch{
			ListFunc: func(lo meta_v1.ListOptions) (runtime.Object, error) {
				// Add selectors to only list Dotmesh pods
				dmLo := lo.DeepCopy()
				dmLo.LabelSelector = "app=dotmesh"
				return client.Core().Pods(DOTMESH_NAMESPACE).List(*dmLo)
			},
			WatchFunc: func(lo meta_v1.ListOptions) (watch.Interface, error) {
				// Add selectors to only list Dotmesh pods
				dmLo := lo.DeepCopy()
				dmLo.LabelSelector = "app=dotmesh"
				return client.Core().Pods(DOTMESH_NAMESPACE).Watch(*dmLo)
			},
		},
		// The types of objects this informer will return
		&v1.Pod{},
		// The resync period of this object. This will force a re-update of all
		// cached objects at this interval.  Every object will trigger the
		// `Updatefunc` even if there have been no actual updates triggered.
		// In some cases you can set this to a very high interval - as you can
		// assume you will see periodic updates in normal operation.  The
		// interval is set low here for demo purposes.
		10*time.Second,
		// Callback Functions to trigger on add/update/delete
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				//				glog.Info("POD ADD %+v", obj)
				rc.scheduleUpdate()
			},
			UpdateFunc: func(old, new interface{}) {
				//				glog.Info("POD UPDATE %+v -> %+v", old, new)
				rc.scheduleUpdate()
			},
			DeleteFunc: func(obj interface{}) {
				//				glog.Info("POD DELETE %+v", obj)
				rc.scheduleUpdate()
			},
		},
		cache.Indexers{},
	)

	rc.podInformer = podInformer
	// PodLister avoids some boilerplate code (e.g. convert runtime.Object to
	// *v1.pod)
	rc.podLister = lister_v1.NewPodLister(podIndexer)

	return rc
}

func (c *dotmeshController) scheduleUpdate() {
	c.updatesNeededLock.Lock()
	defer c.updatesNeededLock.Unlock()

	c.updatesNeeded = true
}

func (c *dotmeshController) Run(stopCh chan struct{}) {
	glog.Info("Starting RebootController")

	go c.nodeInformer.Run(stopCh)
	go c.podInformer.Run(stopCh)

	// Wait for all caches to be synced, before processing is started
	if !cache.WaitForCacheSync(stopCh, c.nodeInformer.HasSynced) {
		glog.Error(fmt.Errorf("Timed out waiting for node cache to sync"))
		return
	}

	if !cache.WaitForCacheSync(stopCh, c.podInformer.HasSynced) {
		glog.Error(fmt.Errorf("Timed out waiting for pod cache to sync"))
		return
	}

	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
	glog.Info("Stopping Reboot Controller")
}

func (c *dotmeshController) runWorker() {
	needed :=
		func() bool {
			c.updatesNeededLock.Lock()
			defer c.updatesNeededLock.Unlock()
			return c.updatesNeeded
		}()

	if needed {
		err := c.process()
		if err != nil {
			glog.Error(err)
		}
	} else {
		time.Sleep(time.Second)
	}
}

func (c *dotmeshController) process() error {
	// nodes is a []*v1.Node
	// v1.Node is documented at https://godoc.org/k8s.io/api/core/v1#Node
	nodes, err := c.nodeLister.List(labels.Everything())
	if err != nil {
		return err
	}

	// Ensure nodes are labelled, so we can bind Dotmesh instances to them
	for _, node := range nodes {
		nodeName := node.ObjectMeta.Name
		_, ok := node.ObjectMeta.Labels[DOTMESH_NODE_LABEL]
		if !ok {
			n2 := node.DeepCopy()
			n2.ObjectMeta.Labels[DOTMESH_NODE_LABEL] = nodeName
			glog.Info(fmt.Sprintf("Labelling unfamiliar node %s so we can bind a Dotmesh to it", n2.ObjectMeta.Name))
			_, err := c.client.Core().Nodes().Update(n2)
			if err != nil {
				return err
			}
		}
	}
	/* TODO
	for node in nodes:
		if not labelled(node):
			label(node)
	*/

	// dotmeshes is a []*v1.Pod
	// v1.Pod is documented at https://godoc.org/k8s.io/api/core/v1#Pod
	dotmeshes, err := c.podLister.List(labels.Everything())
	if err != nil {
		return err
	}

	undotted_nodes := map[string]struct{}{}

	for _, node := range nodes {
		glog.Info(fmt.Sprintf("NODE DETAILS: %s ", node.ObjectMeta.Name))
		undotted_nodes[node.ObjectMeta.Name] = struct{}{}
	}
	for _, dotmesh := range dotmeshes {
		boundNode, ok := dotmesh.Spec.NodeSelector[DOTMESH_NODE_LABEL]
		if ok {
			glog.Info(fmt.Sprintf("POD DETAILS: %s on %s", dotmesh.ObjectMeta.Name, boundNode))
			delete(undotted_nodes, boundNode)
		} else {
			glog.Info(fmt.Sprintf("POD DETAILS: %s - UNBOUND", dotmesh.ObjectMeta.Name))
		}
	}

	/* TODO
	pvs = list_of_dotmesh_pvs_in_az()
	unused_pvs = pvs - map(pv_of_dotmesh, dotmesh_nodes)
	*/

	for node, _ := range undotted_nodes {
		glog.Info(fmt.Sprintf("Undotted node: %s", node))

		/* TODO
		if config.use_pv_storage:
			if len(unused_pvs) == 0:
				unused_pvs.push(make_new_pv(config.new_pv_size))
			storage = unused_pvs.pop()
		else:
		*/

		// FIXME: Create a dotmesh pod (with local storage for now) assigned to "node"
		privileged := true
		newDotmesh := v1.Pod{
			ObjectMeta: meta_v1.ObjectMeta{
				Name:      fmt.Sprintf("dotmesh-%s", node),
				Namespace: "dotmesh",
				Labels: map[string]string{
					"app": "dotmesh",
				},
				Annotations: map[string]string{},
			},
			Spec: v1.PodSpec{
				HostPID: true,
				NodeSelector: map[string]string{
					DOTMESH_NODE_LABEL: node,
				},
				InitContainers: []v1.Container{},
				Containers: []v1.Container{
					v1.Container{
						Name:  "dotmesh-outer",
						Image: "quay.io/dotmesh/dotmesh-server:latest", // FIXME: Include actual hash (compiled in via version)
						Command: []string{
							"/require_zfs.sh",
							"dotmesh-server",
						},
						SecurityContext: &v1.SecurityContext{
							Privileged: &privileged,
						},
						Ports: []v1.ContainerPort{
							{
								Name:          "dotmesh-api",
								ContainerPort: int32(32607),
								Protocol:      v1.ProtocolTCP,
							},
						},
						VolumeMounts: []v1.VolumeMount{
							{Name: "docker-sock", MountPath: "/var/run/docker.sock"},
							{Name: "run-docker", MountPath: "/run/docker"},
							{Name: "var-lib", MountPath: "/var/lib"},
							{Name: "system-lib", MountPath: "/system-lib/lib"},
							{Name: "dotmesh-kernel-modules", MountPath: "/bundled-lib"},
							{Name: "dotmesh-secret", MountPath: "/secret"},
							{Name: "test-pools-dir", MountPath: "/dotmesh-test-pools"},
						},
						Env: []v1.EnvVar{
							{Name: "HOSTNAME", ValueFrom: &v1.EnvVarSource{FieldRef: &v1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "spec.nodeName"}}},
							{Name: "DOTMESH_ETCD_ENDPOINT", Value: "http://dotmesh-etcd-cluster-client.dotmesh.svc.cluster.local:2379"},
							{Name: "DOTMESH_DOCKER_IMAGE", Value: "quay.io/dotmesh/dotmesh-server:latest"},
							{Name: "PATH", Value: "/bundled-lib/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
							{Name: "LD_LIBRARY_PATH", Value: "/bundled-lib/lib:/bundled-lib/usr/lib/"},
							{Name: "ALLOW_PUBLIC_REGISTRATION", Value: "1"},
							{Name: "INITIAL_ADMIN_PASSWORD_FILE", Value: "/secret/dotmesh-admin-password.txt"},
							{Name: "INITIAL_ADMIN_API_KEY_FILE", Value: "/secret/dotmesh-api-key.txt"},
							{Name: "USE_POOL_NAME", Value: "pool"},
							{Name: "USE_POOL_DIR", Value: "/var/lib/dotmesh"},
							{Name: "LOG_ADDR"},
							{Name: "DOTMESH_UPGRADES_URL", Value: "https://checkpoint.dotmesh.com/"},
							{Name: "DOTMESH_UPGRADES_INTERVAL_SECONDS", Value: "14400"},
							{Name: "FLEXVOLUME_DRIVER_DIR", Value: "/usr/libexec/kubernetes/kubelet-plugins/volume/exec nil"},
						},
						ImagePullPolicy: v1.PullAlways,
						LivenessProbe: &v1.Probe{
							Handler: v1.Handler{
								HTTPGet: &v1.HTTPGetAction{
									Path: "/status",
									Port: intstr.FromInt(32607),
								},
								// FIXME								InitialDelaySeconds: 30,
							},
						},
					},
				},
				RestartPolicy: v1.RestartPolicyNever,
				Volumes: []v1.Volume{
					{Name: "test-pools-dir", VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/dotmesh-test-pools"}}},
					{Name: "run-docker", VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/var/run/docker"}}},
					{Name: "docker-sock", VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/var/run/docker.sock"}}},
					{Name: "var-lib", VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/var/lib"}}},
					{Name: "system-lib", VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/lib"}}},
					{Name: "dotmesh-kernel-modules", VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
					{Name: "dotmesh-secret", VolumeSource: v1.VolumeSource{Secret: &v1.SecretVolumeSource{SecretName: "dotmesh"}}},
				},
			},
		}
		glog.Info(fmt.Sprintf("Creating pod %s on node %s", newDotmesh.ObjectMeta.Name, node))
		_, err = c.client.Core().Pods(DOTMESH_NAMESPACE).Create(&newDotmesh)
		if err != nil {
			return err
		}
	}

	/*
		TODO: delete pods without nodes, or with nodes that are not (no longer?) supposed to be running them.
		FIXME: Think about nodes with >1 pod on. Legit thing to do if a cluster shrank? Make sure we do the right thing.

			orphan_dotmeshes = filter(node_of_dotmesh not in nodes, dotmeshes)
			unnoded_dotmeshes = filter(node_of_dotmesh == null, dotmeshes)

			for each dotmesh in unnoded_dotmeshes + orphan_dotmeshes:
				delete_dotmesh(dotmesh)

	*/

	// OLD CODE TO CRIB STUFF FROM:

	//	node, err := c.nodeLister.Get(key)
	//	if err != nil {
	//		return fmt.Errorf("failed to retrieve node by key %q: %v", key, err)
	//	}

	// glog.V(4).Infof("Received update of node: %s", node.GetName())
	// if node.Annotations == nil {
	// 	return nil // If node has no annotations, then it doesn't need a reboot
	// }

	// if _, ok := node.Annotations[RebootNeededAnnotation]; !ok {
	// 	return nil // Node does not need reboot
	// }

	// // Determine if we should reboot based on maximum number of unavailable nodes
	// unavailable := 0
	// if err != nil {
	// 	return fmt.Errorf("Failed to determine number of unavailable nodes: %v", err)
	// }

	// if unavailable >= maxUnavailable {
	// 	// TODO(aaron): We might want this case to retry indefinitely. Could
	// 	// create a specific error an check in handleErr()
	// 	return fmt.Errorf(
	// 		"Too many nodes unvailable (%d/%d). Skipping reboot of %s",
	// 		unavailable, maxUnavailable, node.Name,
	// 	)
	// }

	// // We should not modify the cache object directly, so we make a copy first
	// nodeCopy := node.DeepCopy()

	// glog.Infof("Marking node %s for reboot", node.Name)
	// nodeCopy.Annotations[RebootAnnotation] = ""
	// if _, err := c.client.Core().Nodes().Update(nodeCopy); err != nil {
	// 	return fmt.Errorf("Failed to set %s annotation: %v", RebootAnnotation, err)
	// }
	return nil
}
