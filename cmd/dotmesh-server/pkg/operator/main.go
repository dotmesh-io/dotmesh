package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	lister_v1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func GetClientConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

var Version string = "none" // Set at compile time
const (
	RebootAnnotation           = "reboot-agent.v1.demo.local/reboot"
	RebootNeededAnnotation     = "reboot-agent.v1.demo.local/reboot-needed"
	RebootInProgressAnnotation = "reboot-agent.v1.demo.local/reboot-in-progress"
)

// TODO(aaron): make configurable and add MinAvailable
const maxUnavailable = 1

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

	newRebootController(client).Run(stopCh)
}

type rebootController struct {
	client     kubernetes.Interface
	nodeLister lister_v1.NodeLister
	informer   cache.Controller
	queue      workqueue.RateLimitingInterface
}

func newRebootController(client kubernetes.Interface) *rebootController {
	rc := &rebootController{
		client: client,
		queue:  workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
	}

	indexer, informer := cache.NewIndexerInformer(
		&cache.ListWatch{
			ListFunc: func(lo meta_v1.ListOptions) (runtime.Object, error) {
				// We do not add any selectors because we want to watch all
				// nodes.  This is so we can determine the total count of
				// "unavailable" nodes.  However, this could also be
				// implemented using multiple informers (or better,
				// shared-informers)
				return client.Core().Nodes().List(lo)
			},
			WatchFunc: func(lo meta_v1.ListOptions) (watch.Interface, error) {
				return client.Core().Nodes().Watch(lo)
			},
		},
		// The types of objects this informer will return
		&v1.Node{},
		// The resync period of this object. This will force a re-queue of all
		// cached objects at this interval.  Every object will trigger the
		// `Updatefunc` even if there have been no actual updates triggered.
		// In some cases you can set this to a very high interval - as you can
		// assume you will see periodic updates in normal operation.  The
		// interval is set low here for demo purposes.
		10*time.Second,
		// Callback Functions to trigger on add/update/delete
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				if key, err := cache.MetaNamespaceKeyFunc(obj); err == nil {
					rc.queue.Add(key)
				}
			},
			UpdateFunc: func(old, new interface{}) {
				if key, err := cache.MetaNamespaceKeyFunc(new); err == nil {
					rc.queue.Add(key)
				}
			},
			DeleteFunc: func(obj interface{}) {
				if key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj); err == nil {
					rc.queue.Add(key)
				}
			},
		},
		cache.Indexers{},
	)

	rc.informer = informer
	// NodeLister avoids some boilerplate code (e.g. convert runtime.Object to
	// *v1.node)
	rc.nodeLister = lister_v1.NewNodeLister(indexer)

	return rc
}

func (c *rebootController) Run(stopCh chan struct{}) {
	defer c.queue.ShutDown()
	glog.Info("Starting RebootController")

	go c.informer.Run(stopCh)

	// Wait for all caches to be synced, before processing items from the queue
	// is started
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		glog.Error(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	// Launching additional goroutines would parallelize workers consuming from
	// the queue (but we don't really need this)
	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
	glog.Info("Stopping Reboot Controller")
}

func (c *rebootController) runWorker() {
	for c.processNext() {
	}
}

func (c *rebootController) processNext() bool {
	// Wait until there is a new item in the working queue
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	// Tell the queue that we are done with processing this key. This unblocks
	// the key for other workers. This allows safe parallel processing because
	// two pods with the same key are never processed in parallel.
	defer c.queue.Done(key)
	// Invoke the method containing the business logic
	err := c.process(key.(string))
	// Handle the error if something went wrong during the execution of the
	// business logic
	c.handleErr(err, key)
	return true
}

func (c *rebootController) process(key string) error {
	node, err := c.nodeLister.Get(key)
	if err != nil {
		return fmt.Errorf("failed to retrieve node by key %q: %v", key, err)
	}

	glog.V(4).Infof("Received update of node: %s", node.GetName())
	if node.Annotations == nil {
		return nil // If node has no annotations, then it doesn't need a reboot
	}

	if _, ok := node.Annotations[RebootNeededAnnotation]; !ok {
		return nil // Node does not need reboot
	}

	// Determine if we should reboot based on maximum number of unavailable nodes
	unavailable, err := c.unavailableNodeCount()
	if err != nil {
		return fmt.Errorf("Failed to determine number of unavailable nodes: %v", err)
	}

	if unavailable >= maxUnavailable {
		// TODO(aaron): We might want this case to retry indefinitely. Could
		// create a specific error an check in handleErr()
		return fmt.Errorf(
			"Too many nodes unvailable (%d/%d). Skipping reboot of %s",
			unavailable, maxUnavailable, node.Name,
		)
	}

	// We should not modify the cache object directly, so we make a copy first
	nodeCopy := node.DeepCopy()

	glog.Infof("Marking node %s for reboot", node.Name)
	nodeCopy.Annotations[RebootAnnotation] = ""
	if _, err := c.client.Core().Nodes().Update(nodeCopy); err != nil {
		return fmt.Errorf("Failed to set %s annotation: %v", RebootAnnotation, err)
	}
	return nil
}

func (c *rebootController) handleErr(err error, key interface{}) {
	if err == nil {
		// Forget about the #AddRateLimited history of the key on every
		// successful synchronization.  This ensures that future processing of
		// updates for this key is not delayed because of an outdated error
		// history.
		c.queue.Forget(key)
		return
	}

	// This controller retries 5 times if something goes wrong. After that, it
	// stops trying.
	if c.queue.NumRequeues(key) < 5 {
		glog.Infof("Error processing node %v: %v", key, err)

		// Re-enqueue the key rate limited. Based on the rate limiter on the
		// queue and the re-enqueue history, the key will be processed later
		// again.
		c.queue.AddRateLimited(key)
		return
	}

	c.queue.Forget(key)
	glog.Errorf("Dropping node %q out of the queue: %v", key, err)
}

func (c *rebootController) unavailableNodeCount() (int, error) {
	nodes, err := c.nodeLister.List(labels.Everything())
	if err != nil {
		return 0, err
	}
	var unavailable int
	for _, n := range nodes {
		if nodeIsRebooting(n) {
			unavailable++
			continue
		}
		for _, c := range n.Status.Conditions {
			if c.Type == v1.NodeReady && c.Status == v1.ConditionFalse {
				unavailable++
			}
		}
	}
	return unavailable, nil
}

func nodeIsRebooting(n *v1.Node) bool {
	// Check if node is marked for reeboot-in-progress
	if n.Annotations == nil {
		return false // No annotations - not marked as needing reboot
	}
	if _, ok := n.Annotations[RebootInProgressAnnotation]; ok {
		return true
	}
	// Check if node is already marked for immediate reboot
	_, ok := n.Annotations[RebootAnnotation]
	return ok
}
