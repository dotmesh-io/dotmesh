package user

import (
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/kv"
	"github.com/dotmesh-io/dotmesh/pkg/testutil"
)

func TestCreateUser(t *testing.T) {
	etcdClient, teardown, err := testutil.GetEtcdClient()
	if err != nil {
		t.Fatalf("failed to get etcd client: %s", err)
	}
	defer teardown()

	kvClient := kv.New(etcdClient, "usertests")

	um := New(kvClient)

	stored, err := um.New("harrypotter", "harry@wizzard.works", "verysecret")
	if err != nil {
		t.Fatalf("failed to create new user: %s", err)
	}

	if stored.Email != "harry@wizzard.works" {
		t.Errorf("unexpected email: %s", stored.Email)
	}

	if string(stored.Password) == "verysecret" {
		t.Errorf("password not encrypted")
	}

	if stored.ApiKey == "" {
		t.Errorf("APIKey not generated")
	}

}

func TestAuthenticateUserByPassword(t *testing.T) {
	etcdClient, teardown, err := testutil.GetEtcdClient()
	if err != nil {
		t.Fatalf("failed to get etcd client: %s", err)
	}
	defer teardown()

	kvClient := kv.New(etcdClient, "usertests")

	um := New(kvClient)

	_, err = um.New("joe", "joe@joe.com", "verysecret")
	if err != nil {
		t.Fatalf("failed to create new user: %s", err)
	}

	authenticated, err := um.Authenticate("joe", "verysecret")
	if err != nil {
		t.Fatalf("unexpected authentication failure: %s", err)
	}

	if authenticated.Name != "joe" {
		t.Errorf("expected to found joe, got: %s", authenticated.Name)
	}
}

func TestAuthenticateUserByAPIKey(t *testing.T) {
	etcdClient, teardown, err := testutil.GetEtcdClient()
	if err != nil {
		t.Fatalf("failed to get etcd client: %s", err)
	}
	defer teardown()

	kvClient := kv.New(etcdClient, "usertests")

	um := New(kvClient)

	stored, err := um.New("joe", "joe@joe.com", "verysecret")
	if err != nil {
		t.Fatalf("failed to create new user: %s", err)
	}

	t.Logf("authenticating by API key '%s'", stored.ApiKey)

	authenticated, err := um.Authenticate("joe", stored.ApiKey)
	if err != nil {
		t.Fatalf("unexpected authentication failure: %s. API key: %s", err, stored.ApiKey)
	}

	if authenticated.Name != "joe" {
		t.Errorf("expected to found joe, got: %s", authenticated.Name)
	}
}
