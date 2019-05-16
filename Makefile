# create the context dir before running any of the commands below - ensures the smallest docker context to make builds faster
create_context:
	mkdir context && cp -r ./pkg ./context/pkg && cp -r ./vendor ./context/vendor && cp -r ./cmd ./context/cmd && cp -r ./scripts ./context/scripts

clear_context:
	rm -rf context

build_server: 
	docker build -t ${REPOSITORY}/dotmesh-server:${DOCKER_TAG} ./context --build-arg VERSION=${VERSION} --build-arg STABLE_DOCKER_TAG=${DOCKER_TAG} -f dockerfiles/dotmesh.Dockerfile

build_dind_prov: 
	docker build -t ${REPOSITORY}/dind-dynamic-provisioner:${DOCKER_TAG} ./context -f dockerfiles/dind-provisioner.Dockerfile

push_server: 
	docker push ${REPOSITORY}/dotmesh-server:${DOCKER_TAG}


push_dind_prov:
	docker push ${REPOSITORY}/dind-dynamic-provisioner:${DOCKER_TAG}

build_operator:
	docker build -t ${REPOSITORY}/dotmesh-operator:${DOCKER_TAG} --build-arg VERSION=${VERSION} --build-arg STABLE_DOTMESH_SERVER_IMAGE=${REPOSITORY}/dotmesh-server:${DOCKER_TAG} ./context -f dockerfiles/operator.Dockerfile

push_operator:
	docker push ${REPOSITORY}/dotmesh-operator:${DOCKER_TAG}

build_provisioner: 
	docker build -t ${REPOSITORY}/dotmesh-dynamic-provisioner:${DOCKER_TAG} ./context -f dockerfiles/provisioner.Dockerfile

push_provisioner: 
	docker push ${REPOSITORY}/dotmesh-dynamic-provisioner:${DOCKER_TAG}

gitlab_registry_login: 
	docker login -u gitlab-ci-token -p ${CI_BUILD_TOKEN} ${CI_REGISTRY}

release_all: 
	make build_server && make build_operator && make build_provisioner && make push_provisioner && make push_server && make push_operator

build_push_server:
	make create_context && make gitlab_registry_login && make build_server && make build_dind_prov && make push_server && make push_dind_prov

build_push_operator:
	make create_context && make gitlab_registry_login && make build_operator && make push_operator

build_push_provisioner:
	make create_context && make gitlab_registry_login && make build_provisioner && make push_provisioner
