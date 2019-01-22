package registry

import (
	"context"
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/auth"
	"github.com/dotmesh-io/dotmesh/pkg/store"
	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/dotmesh-io/dotmesh/pkg/user"
)

const TestPrefix = "/registrytests"

func TestUpdateFilesystemFromEtcdA(t *testing.T) {
	idxStore, _ := store.NewKVDBStoreWithIndex(&store.KVDBConfig{
		Type: store.KVTypeMem,
	}, "users")

	um := user.New(idxStore)
	kvClient, _ := store.NewKVDBFilesystemStore(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	registry := NewRegistry(um, kvClient)

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
	idxStore, _ := store.NewKVDBStoreWithIndex(&store.KVDBConfig{
		Type: store.KVTypeMem,
	}, "users")

	um := user.New(idxStore)
	kvClient, _ := store.NewKVDBFilesystemStore(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	registry := NewRegistry(um, kvClient)

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

func TestDumpInternalState(t *testing.T) {
	idxStore, _ := store.NewKVDBStoreWithIndex(&store.KVDBConfig{
		Type: store.KVTypeMem,
	}, "users")

	um := user.New(idxStore)
	kvClient, _ := store.NewKVDBFilesystemStore(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	registry := NewRegistry(um, kvClient)

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

	tlfs := registry.DumpTopLevelFilesystems()
	if len(tlfs) != 1 {
		t.Fatalf("expected to find one tlf, found: %d. %#v", len(tlfs), tlfs)
	}

	if tlfs[0] == nil {
		t.Fatalf("tlfs is nil")
	}
}

func TestGetFilesystemByID(t *testing.T) {
	idxStore, _ := store.NewKVDBStoreWithIndex(&store.KVDBConfig{
		Type: store.KVTypeMem,
	}, "users")

	um := user.New(idxStore)
	kvClient, _ := store.NewKVDBFilesystemStore(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	registry := NewRegistry(um, kvClient)

	// 1. Creating user
	userA, err := um.New("foo", "foo@bar.pub", "verysecret")
	if err != nil {
		t.Fatalf("failed to create new user: %s", err)
	}

	volumeName := types.VolumeName{
		Namespace: "def",
		Name:      "n",
	}

	master := types.RegistryFilesystem{
		Id:      "id-1",
		OwnerId: userA.Id,
	}
	clone := types.Clone{
		FilesystemId: "clone-1",
		Origin: types.Origin{
			FilesystemId: master.Id,
			SnapshotId:   "snapshot-id",
		},
	}

	// 2. Updating filesystem
	err = registry.UpdateFilesystemFromEtcd(volumeName, master)
	if err != nil {
		t.Fatalf("failed to update filesystem from etcd: %s", err)
	}

	err = registry.RegisterClone("clone-x", master.Id, clone)
	if err != nil {
		t.Fatalf("failed to create clone: %s", err)
	}

	foundClone, err := registry.LookupCloneById(clone.FilesystemId)
	if err != nil {
		t.Fatalf("failed to get tlf by name: %s", err)
	}

	if foundClone.FilesystemId != clone.FilesystemId {
		t.Errorf("unexpected clone filesystem ID: %s", clone.FilesystemId)
	}
	if foundClone.Origin.FilesystemId != master.Id {
		t.Errorf("unexpected clone origin fs ID: %s", foundClone.Origin.FilesystemId)
	}
}
