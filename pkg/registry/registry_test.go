package registry

import (
	"context"
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/auth"
	"github.com/dotmesh-io/dotmesh/pkg/kv"
	"github.com/dotmesh-io/dotmesh/pkg/testutil"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
)

const TestPrefix = "/registrytests"

func TestUpdateFilesystemFromEtcdA(t *testing.T) {
	etcdClient, teardown, err := testutil.GetEtcdClient()
	if err != nil {
		t.Fatalf("failed to get etcd client: %s", err)
	}
	defer teardown()

	kvClient := kv.New(etcdClient, TestPrefix)
	um := user.New(kvClient)
	registry := NewRegistry(um, etcdClient, TestPrefix)

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
		Namespace: "def",
		Name:      "n",
	})
	if err != nil {
		t.Fatalf("failed to get tlf by name: %s", err)
	}

	if tlf.Owner.Id != userA.Id {
		t.Errorf("tlf owner ID doesn't match, expected: %s, got :%s", userA.Id, tlf.Owner.Id)
	}
}

func TestUpdateCollaborators(t *testing.T) {
	etcdClient, teardown, err := testutil.GetEtcdClient()
	if err != nil {
		t.Fatalf("failed to get etcd client: %s", err)
	}
	defer teardown()
	kvClient := kv.New(etcdClient, TestPrefix)
	um := user.New(kvClient)
	registry := NewRegistry(um, etcdClient, TestPrefix)

	// Creating owner and collaborator
	userA, err := um.New("foo", "foo@bar.pub", "verysecret")
	if err != nil {
		t.Fatalf("failed to create new user: %s", err)
	}

	userCollaborator, err := um.New("coo", "coo@bar.pub", "verysecret")
	if err != nil {
		t.Fatalf("failed to create new user: %s", err)
	}

	ctx := auth.SetAuthenticationDetailsCtx(context.Background(), userA, user.AuthenticationTypePassword)

	// Creating/updating filesystem
	err = registry.RegisterFilesystem(ctx, types.VolumeName{
		Namespace: "def",
		Name:      "n",
	}, "id-1")
	if err != nil {
		t.Fatalf("failed to update filesystem from etcd: %s", err)
	}

	// Getting initial TLF

	tlfInitial, err := registry.GetByName(types.VolumeName{
		Namespace: "def",
		Name:      "n",
	})
	if err != nil {
		t.Fatalf("failed to get tlf by name: %s", err)
	}

	t.Logf("adding collaborator: %s", userCollaborator.SafeUser())

	// Adding collaborator
	err = registry.UpdateCollaborators(context.Background(), tlfInitial, []user.SafeUser{userCollaborator.SafeUser()})

	if err != nil {
		t.Fatalf("failed to add collaborator to tlf: %s", err)
	}

	// 3. Getting it by name
	tlfUpdated, err := registry.GetByName(types.VolumeName{
		Namespace: "def",
		Name:      "n",
	})
	if err != nil {
		t.Fatalf("failed to get tlf by name: %s", err)
	}

	if len(tlfUpdated.Collaborators) != 1 {
		t.Errorf("expected to find 1 collaborator")
	} else {
		if tlfUpdated.Collaborators[0].Name != userCollaborator.Name {
			t.Errorf("expected to find %s collaborator, got: %s", userCollaborator.Name, tlfUpdated.Collaborators[0].Name)
		}
	}

	if tlfUpdated.Owner.Id != userA.Id {
		t.Errorf("tlf owner ID doesn't match, expected: %s, got :%s", userA.Id, tlfUpdated.Owner.Id)
	}
}
