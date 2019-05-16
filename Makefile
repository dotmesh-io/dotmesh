# create the context dir before running any of the commands below - ensures the smallest docker context to make builds faster
.PHONY: create_context
create_context:
	mkdir context && cp -r ./pkg ./context/pkg && cp -r ./vendor ./context/vendor && cp -r ./cmd ./context/cmd && cp -r ./scripts ./context/scripts

.PHONY: clear_context
clear_context:
	rm -rf context

.PHONY: build_server
build_server: ; docker build -t ${REPOSITORY}/dotmesh-server:${DOCKER_TAG} ./context --build-arg VERSION=${VERSION} --build-arg STABLE_DOCKER_TAG=${DOCKER_TAG} -f dockerfiles/dotmesh.Dockerfile

.PHONY: build_dind_prov
build_dind_prov: ; docker build -t ${REPOSITORY}/dind-dynamic-provisioner:${DOCKER_TAG} ./context -f dockerfiles/dind-provisioner.Dockerfile

.PHONY: push_server
push_server: ; docker push ${REPOSITORY}/dotmesh-server:${DOCKER_TAG}

.PHONY: push_dind_prov
push_dind_prov: ; docker push ${REPOSITORY}/dind-dynamic-provisioner:${DOCKER_TAG}

.PHONY: build_operator
build_operator: ; docker build -t ${REPOSITORY}/dotmesh-operator:${DOCKER_TAG} --build-arg VERSION=${VERSION} --build-arg STABLE_DOTMESH_SERVER_IMAGE=${REPOSITORY}/dotmesh-server:${DOCKER_TAG} ./context -f dockerfiles/operator.Dockerfile

.PHONY: push_operator
push_operator: ; docker push ${REPOSITORY}/dotmesh-operator:${DOCKER_TAG}

.PHONY: build_provisioner
build_provisioner: ; docker build -t ${REPOSITORY}/dotmesh-dynamic-provisioner:${DOCKER_TAG} ./context -f dockerfiles/provisioner.Dockerfile

.PHONY: push_provisioner
push_provisioner: ; docker push ${REPOSITORY}/dotmesh-dynamic-provisioner:${DOCKER_TAG}

.PHONY: gitlab_registry_login
gitlab_registry_login: ; docker login -u gitlab-ci-token -p ${CI_BUILD_TOKEN} ${CI_REGISTRY}

.PHONY: release_all
release_all: ; make build_server && make build_operator && make build_provisioner && make push_provisioner && make push_server && make push_operator