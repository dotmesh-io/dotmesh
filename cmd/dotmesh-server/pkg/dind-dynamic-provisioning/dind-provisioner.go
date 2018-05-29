/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/external-storage/lib/controller"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/rest"
	"syscall"
)

const (
	resyncPeriod              = 15 * time.Second
	provisionerName           = "dotmesh/dind-dynamic-provisioner"
	exponentialBackOffOnError = false
	failedRetryThreshold      = 5
	leasePeriod               = controller.DefaultLeaseDuration
	retryPeriod               = controller.DefaultRetryPeriod
	renewDeadline             = controller.DefaultRenewDeadline
	termLimit                 = controller.DefaultTermLimit
)

type dindProvisioner struct {
}

// NewDotmeshProvisioner creates a new dotmesh provisioner
func NewDindProvisioner() controller.Provisioner {
	return &dindProvisioner{}
}

var _ controller.Provisioner = &dindProvisioner{}

// Provision creates a storage asset and returns a PV object representing it.
// The options.PVName
// PV such that the flexvolume can use the PVC id as the folder name
func (p *dindProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	// PV name: options.PVName
	// options is a https://godoc.org/github.com/kubernetes-incubator/external-storage/lib/controller#VolumeOptions
	// options.PVC is a https://godoc.org/k8s.io/kubernetes/pkg/api#PersistentVolumeClaim
	// options.Parameters = the storage class parameters

	// options.PVC.ObjectMeta.Annotations = the PVC annotations

	// we will use the

	glog.Info(fmt.Sprintf("Creating PV %s in response to PVC %s", options.PVName, options.PVName))

	size := options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	sizeBytes, ok := size.AsInt64()
	if !ok {
		return nil, fmt.Errorf("Cannot handle storage size %s", size.String())
	}

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:        options.PVName,
			Annotations: map[string]string{},
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: options.PersistentVolumeReclaimPolicy,
			AccessModes:                   options.PVC.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			// This big struct is documented here:
			// https://godoc.org/k8s.io/kubernetes/pkg/api#PersistentVolumeSource
			PersistentVolumeSource: v1.PersistentVolumeSource{
				FlexVolume: &v1.FlexVolumeSource{
					Driver: "dotmesh.io/dind",
					FSType: "ext4",
					Options: map[string]string{
						"id":   options.PVName,
						"size": fmt.Sprintf("%d", sizeBytes),
					},
				},
			},
		},
	}

	return pv, nil
}

// Delete removes the storage asset that was created by Provision represented
// by the given PV.
// We are only dealing with folders in test mode so let's not worry too much
// about deleting
func (p *dindProvisioner) Delete(volume *v1.PersistentVolume) error {
	return nil
}

func main() {
	syscall.Umask(0)

	flag.Parse()
	flag.Set("logtostderr", "true")

	glog.Info("Starting")

	// Create an InClusterConfig and use it to create a client for the controller
	// to use to communicate with Kubernetes
	config, err := rest.InClusterConfig()
	if err != nil {
		glog.Fatalf("Failed to create config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Failed to create client: %v", err)
	}

	// The controller needs to know what the server version is because out-of-tree
	// provisioners aren't officially supported until 1.5
	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		glog.Fatalf("Error getting server version: %v", err)
	}

	// Create the provisioner: it implements the Provisioner interface expected by
	// the controller
	dindProvisioner := NewDindProvisioner()

	// Start the provision controller which will dynamically provision dotmesh
	// PVs
	pc := controller.NewProvisionController(clientset, resyncPeriod, provisionerName, dindProvisioner, serverVersion.GitVersion, exponentialBackOffOnError, failedRetryThreshold, leasePeriod, renewDeadline, retryPeriod, termLimit)
	pc.Run(wait.NeverStop)
}
