package kv

import (
	"testing"

	"github.com/dotmesh-io/dotmesh/pkg/testutil"
)

func TestSetGet(t *testing.T) {
	client, teardown, err := testutil.GetEtcdClient()
	if err != nil {
		t.Fatalf("failed to get etcd client: %s", err)
	}
	defer teardown()

	kv := New(client, testutil.GetTestPrefix())

	_, err = kv.Set("test", "foo", "bar")
	if err != nil {
		t.Errorf("failed to Put kv: %s", err)
	}
	// getting it

	n, err := kv.Get("test", "foo")
	if err != nil {
		t.Fatalf("failed to get key, error: %s", err)
	}

	if n.Value != "bar" {
		t.Errorf("expected 'foo', got: '%s'", n.Value)
	}
}

func TestCreateWithIndexGetByName(t *testing.T) {
	client, teardown, err := testutil.GetEtcdClient()
	if err != nil {
		t.Fatalf("failed to get etcd client: %s", err)
	}
	defer teardown()

	kv := New(client, testutil.GetTestPrefix())

	_, err = kv.CreateWithIndex("users", "d2a8c37c-0290-446a-8d2e-3c904cb8b0f8", "longid", "bar")
	if err != nil {
		t.Errorf("failed to Put kv: %s", err)
	}
	// getting it by name
	n, err := kv.Get("users", "longid")
	if err != nil {
		t.Fatalf("failed to get key, error: %s", err)
	}

	if n.Value != "bar" {
		t.Errorf("expected 'foo', got: '%s'", n.Value)
	}
}

func TestCreateWithIndexGetByID(t *testing.T) {
	client, teardown, err := testutil.GetEtcdClient()
	if err != nil {
		t.Fatalf("failed to get etcd client: %s", err)
	}
	defer teardown()

	kv := New(client, testutil.GetTestPrefix())

	_, err = kv.CreateWithIndex("users", "aaaaaaaa-0290-446a-8d2e-3c904cb8b0f8", "longid", "barbar")
	if err != nil {
		t.Errorf("failed to Put kv: %s", err)
	}
	// getting it by name
	n, err := kv.Get("users", "aaaaaaaa-0290-446a-8d2e-3c904cb8b0f8")
	if err != nil {
		t.Fatalf("failed to get key, error: %s", err)
	}

	if n.Value != "barbar" {
		t.Errorf("expected 'foo', got: '%s'", n.Value)
	}
}
