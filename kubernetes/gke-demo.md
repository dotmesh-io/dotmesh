# gke demo


## setup

Things you will need:

 * [google cloud account](https://cloud.google.com/)
 * [gcloud cli](https://cloud.google.com/sdk/gcloud/)
 * [helm cli](https://github.com/kubernetes/helm)
 * [helm cli](https://github.com/kubernetes/helm)

We will need a google cloud project name - login to the google cloud console and either create a new project or get the name of an existing project.

```bash
export GCP_PROJECT_ID=myprojectname
```

NOTE: replace `myprojectname` with the name of the project from gcloud

```bash
gcloud config set project $GCP_PROJECT_ID
```

To test we have our gcloud CLI setup:

```bash
gcloud container clusters list
```

## service account

We need a [gcloud service account](https://cloud.google.com/kubernetes-engine/docs/tutorials/authenticating-to-cloud-platform) that we will use to deploy to k8s from gitlab.

From the google console - [create a new service account](https://console.cloud.google.com/iam-admin/serviceaccounts/project?project=webkit-servers)

The required roles:

 * Compute Storage Admin
 * Kubernetes Engine Admin
 * Storage Admin

Download the .json key for the service account.

## local dm cluster

Now we install datamesh locally:

```bash
sudo curl -o /usr/local/bin/dm \
    https://get.datamesh.io/$(uname -s)/dm
sudo chmod +x /usr/local/bin/dm
dm cluster init
```

## gke cluster

Then we create our cluster - first clone the datamesh repo then:

```bash
cd datamesh/kubernetes
bash gke.sh create
bash gke.sh prepare
bash gke.sh deploy
```

This will print out a `dm remote` command - run that and we will now have a `gke` datamesh remote:

```bash
dm remote -v
dm remote switch gke
```

To check that datamesh is up and running:

```bash
kubectl get po -n datamesh
```

## gitlab helm

Now - install gitlab using helm (these commands are all run from inside the `datamesh/kubernetes` folder):

```bash
helm init
helm repo add gitlab https://charts.gitlab.io
helm install --namespace gitlab --name gitlab -f gitlab/helm/values.yaml gitlab/gitlab
```

Now we wait until the load balancer has a publicIP:

```bash
kubectl get svc -n gitlab -w
```

Once the public IP shows up - we tell the gitlab helm chart about it.  The gitlab container itself will not start until we have done this:

```bash
export SERVICE_IP=$(kubectl get svc --namespace gitlab gitlab-gitlab -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
helm upgrade gitlab --set externalUrl=http://$SERVICE_IP gitlab/gitlab
```

Check the status of the gitlab containers:

```bash
kubectl get po -n gitlab
```

Next - we open gitlab in a browser and set the root password using the GUI:

```bash
open http://$SERVICE_IP
```

## gitlab runner

Open the `/admin/runners` page on the gitlab GUI - this will tell you the runner token - copy this and export it:

```bash
export GITLAB_RUNNER_TOKEN=xxx
```

Then we export the URL for our gitlab instance:

```bash
export GITLAB_URL=http://$SERVICE_IP
```

Make the namespace and secrets:

```
kubectl create ns gitlab-runner
echo "$GITLAB_RUNNER_TOKEN" > runnertoken.txt
echo "$GITLAB_URL" > gitlaburl.txt
kubectl create secret generic runnertoken --from-file=runnertoken.txt -n gitlab-runner
kubectl create secret generic gitlaburl --from-file=gitlaburl.txt -n gitlab-runner
rm -f runnertoken.txt gitlaburl.txt
```

Now deploy the runner:

```bash
kubectl apply -f gitlab-runner/deployment.yaml
```

Check the deployment status:

```bash
kubectl get po -n gitlab-runner
```

## create gitlab project

Now we login to gitlab and create a new project.  Then head to the CI/CD -> settings tab - we need to enter the following 2 secret variables:

 * `GCLOUD_SERVICE_KEY` - paste the contents of the service account .json as the value
 * `GCP_PROJECT_ID` - set this to the value of your `GCP_PROJECT_ID` variable (that we set earlier)

## push code

Now we copy the example app out of the datamesh folder (so we can `git init` without having git submodules):

```bash
cp -r example_app ~/dm_example_app
cd ~/dm_example_app
git init
git add -A
git commit -m "initial commit"
```

We can login to our gitlab server and find the git remote and add that:

```bash
git remote add origin http://...
```

Then we push our code:

```
git push origin master
```

We can now login to the gitlab server and see the pipelines running.

Once the jobs are done - we should see a datamesh volume in our list:

```bash
dm list
```

#### misc

NOTE: you don't need this section - it's for rebuilding the docker images that get this setup.

We need to build and push the installer image:

```bash
make k8s.installer
```

Then build the gitlab-runner image:

```bash
cd kubernetes/gitlab-runner
docker build -t binocarlos/dm-gitlab-runner:v10.2.0 .
docker push binocarlos/dm-gitlab-runner:v10.2.0
```
