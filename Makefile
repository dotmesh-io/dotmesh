.PHONY: build
build: ; bash dev.sh build

.PHONY: cluster.build
cluster.build: ; bash dev.sh cluster-build

.PHONY: cluster.start
cluster.start: ; bash dev.sh cluster-start

.PHONY: cluster.stop
cluster.stop: ; bash dev.sh cluster-stop

.PHONY: cluster.upgrade
cluster.upgrade: ; bash dev.sh cluster-upgrade

.PHONY: cli.build
cli.build:
	bash dev.sh cli-build

.PHONY: reset
reset: ; bash dev.sh reset

.PHONY: vagrant.sync
vagrant.sync: ; bash dev.sh vagrant-sync

.PHONY: vagrant.prepare
vagrant.prepare: ; bash dev.sh vagrant-prepare

.PHONY: vagrant.test
vagrant.test: ; bash dev.sh vagrant-test