# Releasing dotmesh

Our release process is automated through Gitlab pipelines. The file
`.gitlab-ci.yml` in the root directory of this repo contains all the
mechanisms that enable this.

## Branches and versions

Releases are made from branches matching the pattern `release-X.Y`, eg
`release-0.1`. Every commit on such a branch is a release version, and
the version string will be `release-X.Y` for the first commit on the
branch (which is the commit it shares with the parent branch, which
should be `master`), or `release-X.Y.Z`, where `Z` is the number of
commits since the first commit.

You can check the generated version string from any given git state by doing this:

```
$ (cd cmd/versioner/; go run versioner.go)
release-0.1.1
```

To do a new version release, just create a branch called
`release-X.Y`; to do a patch release, just merge into that branch,
like so:

```
$ git checkout release-0.1
$ git merge --no-ff master
$ (cd cmd/versioner/; go run versioner.go)
release-0.1.1
```

## The build artefacts

The products of a build fall into three camps.

### Docker images

The backend components of Dotmesh are all distributed as Docker
images. The `build` stage in the Gitlab CI pipelines push them
directory to `quay.io`, with the full Git hash of the commit being
built as the tag. There is no `:latest`, nor use of version numbers as
Docker image tags, because we don't publish the docker image names -
they (with their tags) are hardcoded into the binaries and YAML we
distribute.

As such, there is no distinction between release and non-release
builds; they all go into `quay.io` identified by their Git hash. It
keeps it simple, but we'll have fun one day when we decide to start
garbage collecting old master builds and we need to distinguish them
from release builds we want to keep!

### Client Binaries

We build `dm` client binaries for various platforms. They are uploaded
to `get.dotmesh.io`, with relative paths of the form `PLATFORM/dm`
(`PLATFORM`=`Linux` or `Darwin` at the time of writing). This binary
has the git hash of the Docker images it needs to pull down embedded
inside it, so will automatically refer to the correct server images.

### Kubernetes YAML

We also build YAML files to install Dotmesh into Kubernetes, which is
also uploaded to `get.dotmesh.io`, with a relative path of
`yaml/*.yaml` as they are platform-independent.

## Automated deployment

The `deploy` stage drives automatic continuous deployment, and which
jobs activate depend on the name of the branch.

### `master`

Master builds trigger the `deploy_master_build` job. This copies the
build artefacts to `get.dotmesh.io/unstable/master`, overwriting
whatever was there previously. We don't keep old master builds around.

The job is written such that, if we change it to trigger on any
non-release branch rather than just `master`, it would also do the
same for other branches - putting their latest builds in
`get.dotmesh.io/unstable/BRANCH`.

### `release-.*`

Release branch builds get a version computed by running the versioner,
found in-tree at `cmd/versioner`; this generates a human-readable
version string based on the branch name and the commit number in the
branch, eg `release-0.1.0`.

The artefacts are deployed to `get.dotmesh.io/VERSION`. As the version
changes with every commit, this creates a continuous record of every
release build.

## Making it live

However, we do not publish the above `get.dotmesh.io` URLs, or run
their builds in production. Additional steps are required to mark a
build as the "latest stable build" that appears at the root of
`get.dotmesh.io`, which is linked to from our documentation, or to
deploy a new version to the Hub. These are manual jobs in the
`manual_deploy` stage.

### `mark_release_as_stable`

This is only available for builds from `release-*` branches. If
triggered, it moves the symlinks from the root of `get.dotmesh.io/...`
to point to `get.dotmesh.io/VERSION/...`, thereby making the published
URLs now point to this version.

### Updating the releases on Github

We direct people here to see the release history:

https://github.com/dotmesh-io/dotmesh/releases

This currently needs to be manually updated.

 * Create a new release tag in the github UI. This opens up a window to enter details.
 * Call the tag `release-X.Y.Z` and pick the correct release branch
 * Write a description and release notes, by copying the pattern from an existing tag.
 * Upload tarballs of the binaries from `get.dotmesh.io`.
 * Press the button to create the release

I created the binary tarballs like so:

```
mybox$ ssh releases@get.dotmesh.io
get$ cd /pool/releases/release-0.2.0
get$ tar -czvf ~/Darwin.tar.gz Darwin/
get$ tar -czvf ~/Linux.tar.gz Linux/
get$ tar -czvf ~/kubernetes-cluster-yamls.tar.gz yaml/
get$ ^D
mybox$ scp releases@get.dotmesh.io:*.tar.gz .
```

Try the latest binary on https://dotmesh.com/try-dotmesh/ with a dm
version to check that it's all deployed correctly.

## Pushing live version of docs

There might be docs issues that talk about as yet unreleased features.  These
issues should be in the `blocked` column of the kanban board.

Once the release is complete - open the pipeline for the docs repo and click
the `deploy to production` job.

Do this once the release is complete - now the docs and the released software
should be lining up!

### Re-releasing
In the event of a failure during the release process, for example, CI failures, or an embarassing bug fix on the same tag, it is possible to re-release by triggering the pipeline on http://gitlab.dotmesh.io:9999/dotmesh/dotmesh-sync/pipelines on the commit on the release branch. If the commit is on a pre-exisiting release tag, it will preserve the version of the tag.

In rare cases where there is a requirement to re-do a release with different code, this can be done by resetting the branch and preserving the relese-x.y.z tag on the commit and force-pushing. GitLab doesn't handle syncing force pushes, so you will need to temporarily disable branch protection for your user and force push to dotmesh-sync after doing this.
