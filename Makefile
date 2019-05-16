# create the context dir before running any of the commands below - ensures the smallest docker context to make builds faster
create_context:
	mkdir context && cp -r ./pkg ./context/pkg && cp -r ./vendor ./context/vendor && cp -r ./cmd ./context/cmd && cp -r ./scripts ./context/scripts

clear_context:
	rm -rf context

build_server:
    docker build -t /$CI_PROJECT_PATH/dotmesh-server:${CI_COMMIT_TAG:-$CI_COMMIT_SHA} ./context --build-arg VERSION=${CI_COMMIT_TAG:-CI_COMMIT_SHORT_SHA} --build-arg STABLE_DOCKER_TAG=${CI_COMMIT_TAG:-$CI_COMMIT_SHA} -f dockerfiles/dotmesh.Dockerfile; \
    docker build -t ${REPOSITORY}/dind-dynamic-provisioner:${CI_COMMIT_TAG:-$CI_COMMIT_SHA} ./context -f dockerfiles/dind-provisioner.Dockerfile

push_server:
	docker push ${REPOSITORY}/dotmesh-server:${CI_COMMIT_TAG:-$CI_COMMIT_SHA}; \
    docker push ${REPOSITORY}/dind-dynamic-provisioner:${CI_COMMIT_TAG:-$CI_COMMIT_SHA}

build_operator:
    docker build -t ${REPOSITORY}/dotmesh-operator:${CI_COMMIT_TAG:-$CI_COMMIT_SHA} --build-arg VERSION=${CI_COMMIT_TAG:-CI_COMMIT_SHORT_SHA} --build-arg STABLE_DOTMESH_SERVER_IMAGE=quay.io/dotmesh/dotmesh-server:${CI_COMMIT_TAG:-$CI_COMMIT_SHA} ./context -f dockerfiles/operator.Dockerfile

push_operator:
	docker push ${REPOSITORY}/dotmesh-operator:${CI_COMMIT_TAG:-$CI_COMMIT_SHA}

build_provisioner:
    docker build -t ${REPOSITORY}/dotmesh-dynamic-provisioner:${CI_COMMIT_TAG:-$CI_COMMIT_SHA} ./context -f dockerfiles/provisioner.Dockerfile

push_provisioner:
	docker push ${REPOSITORY}/dotmesh-dynamic-provisioner:${CI_COMMIT_TAG:-$CI_COMMIT_SHA}

gitlab_registry_login:
	docker login -u gitlab-ci-token -p $CI_BUILD_TOKEN $CI_REGISTRY