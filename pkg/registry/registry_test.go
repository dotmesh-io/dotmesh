package registry

import (
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/kv"
	"github.com/dotmesh-io/dotmesh/pkg/testutil"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
)

func TestUpdateFilesystemFromEtcdA(t *testing.T) {
	etcdClient, teardown, err := testutil.GetEtcdClient()
	if err != nil {
		t.Fatalf("failed to get etcd client: %s", err)
	}
	defer teardown()
	kvClient := kv.New(etcdClient, "usertests")
	um := user.New(kvClient)
	registry := NewRegistry(um, etcdClient, "registrytests")

	// 1. Creating user
	userA, err := um.New("foo", "foo@bar.pub", "verysecret")
	if err != nil {
		t.Fatalf("failed to create new user: %s", err)
	}

	// 2. Updating filesystem
	err = registry.UpdateFilesystemFromEtcd(types.VolumeName{
		Namespace: "def",
		Name:      "n",
	}, types.RegistryFilesystem{
		Id:      "id-1",
		OwnerId: userA.Id,
	})
	if err != nil {
		t.Fatalf("failed to update filesystem from etcd: %s", err)
	}

	// 3. Getting it by name

	tlf, err := registry.GetByName(types.VolumeName{
		"def",
		"n",
	})
	if err != nil {
		t.Fatalf("failed to get tlf by name: %s", err)
	}

	if tlf.Owner.Id != userA.Id {
		t.Errorf("tlf owner ID doesn't match, expected: %s, got :%s", userA.Id, tlf.Owner.Id)
	}
}
