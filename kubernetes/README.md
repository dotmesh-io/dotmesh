# Dotmesh on Kubernetes

Dotmesh supports:

* being deployed on Kubernetes
* providing persistent volumes to Kubernetes pods

## Prerequisites

You need a Kubernetes 1.6.0+ cluster.

## Getting started

Get started by creating an initial admin API key, then deploy Dotmesh with:

```
kubectl create namespace dotmesh
echo "secret123" > dotmesh-admin-password.txt
kubectl create secret generic dotmesh --from-file=dotmesh-admin-password.txt -n dotmesh
rm dotmesh-admin-password.txt
kubectl apply -f manifests/
```

Now you can use your Kubernetes cluster as a Dotmesh remote!

```
sudo curl -o /usr/local/bin/dm https://get.dotmesh.io/$(uname -s)/dm
sudo chmod +x /usr/local/bin/dm
dm remote add kube admin@<address-of-cluster-nodes>
dm list
```

Enter the admin password you specified (`secret123` in the example above), then you should be able to list Kubernetes-provisioned volumes with `dm list` and push/pull volumes between clusters with `dm push`, etc.

TODO: StorageClass example using Dotmesh for dynamic provisioning (how to get a volume in the first place).

TODO: a TPR for dotmesh volumes to experiment with fancy stuff?
Examples of declarative config for e.g. regular backups?
Federation API server volume implementation?


## Notes from installing on GKE / AWS

gke instances don't let you create a `/dotmesh-test-pools` folder on the root because `/` is read-only
TODO: allow the path of the `/dotmesh-test-pools` to be configurable.

gke cluster must be run in alpha to enable the `rbac.authorization.k8s.io/v1alpha1` apiVersion

aws cluster uses `4.4.102-k8s` kernel version and we got a 404 on `https://get.dotmesh.io/zfs/zfs-4.4.102-k8s.tar.gz`

## Installing on gcloud


NOTE: on each node you need to:

```bash
sudo mkdir -p /dotmesh-test-pools
sudo mount --bind /dotmesh-test-pools /dotmesh-test-pools
sudo mount --make-shared /dotmesh-test-pools
```

```bash
gcloud alpha container clusters create testdm --enable-kubernetes-alpha
gcloud container clusters get-credentials testdm
kubectl create namespace dotmesh
echo "secret123" > dotmesh-admin-password.txt
kubectl create secret generic dotmesh --from-file=dotmesh-admin-password.txt -n dotmesh
rm dotmesh-admin-password.txt
cat etcd-operator-dep.yaml | kubectl apply -f -
cat dotmesh.yaml | kubectl apply -f -
```
