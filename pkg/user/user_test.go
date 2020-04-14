package user

import (
	"bytes"
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/store"

	uuid "github.com/nu7hatch/gouuid"
)

func TestCreateUser(t *testing.T) {

	client, err := store.NewKVDBClient(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}
	kvClient := store.NewKVDBStoreWithIndex(client, UsersPrefix)

	um := NewInternal(kvClient)

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

func TestImportUser(t *testing.T) {
	client, err := store.NewKVDBClient(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}
	kvClient := store.NewKVDBStoreWithIndex(client, UsersPrefix)

	um := NewInternal(kvClient)

	id, _ := uuid.NewV4()

	immigrant := &User{
		Id:       id.String(),
		ApiKey:   "very_s3cr3t",
		Salt:     []byte("black sea salt"),
		Password: []byte("not sure why I am of type byte"),
		Name:     "name here",
		Email:    "casual@email.com",
	}

	err = um.Import(immigrant)
	if err != nil {
		t.Fatalf("didn't expect import to fail: %s", err)
	}

	stored, err := um.Get(&Query{
		Ref: immigrant.Name,
	})
	if err != nil {
		t.Errorf("failed to get imported user: %s", err)
	}

	if stored.Name != immigrant.Name {
		t.Errorf("unexpected name: %s", stored.Name)
	}
	if stored.Id != immigrant.Id {
		t.Errorf("unexpected id: %s", stored.Id)
	}
	if stored.ApiKey != immigrant.ApiKey {
		t.Errorf("unexpected ApiKey: %s", stored.ApiKey)
	}

	if !bytes.Equal(stored.Password, immigrant.Password) {
		t.Errorf("password doesn't match")
	}
	if !bytes.Equal(stored.Salt, immigrant.Salt) {
		t.Errorf("salt doesn't match")
	}
}

func TestGetWithIndex(t *testing.T) {
	client, err := store.NewKVDBClient(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}
	kvClient := store.NewKVDBStoreWithIndex(client, UsersPrefix)

	um := NewInternal(kvClient)

	_, err = um.New("foo", "foo@bar.works", "verysecret")
	if err != nil {
		t.Fatalf("failed to create new user: %s", err)
	}

	stored, err := um.Get(&Query{Ref: "foo"})
	if err != nil {
		t.Fatalf("failed to get user: %s", err)
	}

	if stored.Name != "foo" {
		t.Errorf("unexpected name: %s", stored.Name)
	}
}

func TestGetWithoutIndex(t *testing.T) {
	client, err := store.NewKVDBClient(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}
	kvClient := store.NewKVDBStoreWithIndex(client, UsersPrefix)

	um := NewInternal(kvClient)

	_, err = um.New("foo", "foo@bar.works", "verysecret")
	if err != nil {
		t.Fatalf("failed to create new user: %s", err)
	}

	err = kvClient.DeleteFromIndex(UsersPrefix, "foo")
	if err != nil {
		t.Errorf("failed to delete from index: %s", err)
	}

	stored, err := um.Get(&Query{Ref: "foo"})
	if err != nil {
		t.Fatalf("failed to get user: %s", err)
	}

	if stored.Name != "foo" {
		t.Errorf("unexpected name: %s", stored.Name)
	}

}

func TestAuthenticateWithoutIndex(t *testing.T) {
	client, err := store.NewKVDBClient(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}
	kvClient := store.NewKVDBStoreWithIndex(client, UsersPrefix)
	um := NewInternal(kvClient)

	_, err = um.New("foo", "foo@bar.works", "verysecret")
	if err != nil {
		t.Fatalf("failed to create new user: %s", err)
	}

	err = kvClient.DeleteFromIndex(UsersPrefix, "foo")
	if err != nil {
		t.Errorf("failed to delete from index: %s", err)
	}

	stored, _, err := um.Authenticate("foo", "verysecret")
	if err != nil {
		t.Fatalf("failed to get user: %s", err)
	}

	if stored.Name != "foo" {
		t.Errorf("unexpected name: %s", stored.Name)
	}
}

func TestAuthenticateUserByPassword(t *testing.T) {
	client, err := store.NewKVDBClient(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}
	kvClient := store.NewKVDBStoreWithIndex(client, UsersPrefix)

	um := NewInternal(kvClient)

	_, err = um.New("joe", "joe@joe.com", "verysecret")
	if err != nil {
		t.Fatalf("failed to create new user: %s", err)
	}

	authenticated, _, err := um.Authenticate("joe", "verysecret")
	if err != nil {
		t.Fatalf("unexpected authentication failure: %s", err)
	}

	if authenticated.Name != "joe" {
		t.Errorf("expected to found joe, got: %s", authenticated.Name)
	}
}

func TestAuthenticateUserByAPIKey(t *testing.T) {
	client, err := store.NewKVDBClient(&store.KVDBConfig{
		Type: store.KVTypeMem,
	})
	if err != nil {
		t.Fatalf("failed to init kv store: %s", err)
	}
	kvClient := store.NewKVDBStoreWithIndex(client, UsersPrefix)

	um := NewInternal(kvClient)

	stored, err := um.New("joe", "joe@joe.com", "verysecret")
	if err != nil {
		t.Fatalf("failed to create new user: %s", err)
	}

	t.Logf("authenticating by API key '%s'", stored.ApiKey)

	authenticated, at, err := um.Authenticate("joe", stored.ApiKey)
	if err != nil {
		t.Fatalf("unexpected authentication failure: %s. API key: %s", err, stored.ApiKey)
	}

	if authenticated.Name != "joe" {
		t.Errorf("expected to found joe, got: %s", authenticated.Name)
	}

	if at != AuthenticationTypeAPIKey {
		t.Errorf("unexpected authentication type: %s", at)
	}
}
