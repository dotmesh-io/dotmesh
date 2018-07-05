package kv

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/client"
	"github.com/dotmesh-io/dotmesh/pkg/validator"
)

type KV interface {
	List(prefix string) ([]*client.Node, error)
	CreateWithIndex(prefix, id, name string, val string) (*client.Node, error)

	DeleteFromIndex(prefix, name string) error
	AddToIndex(prefix, name, id string) error

	Set(prefix, id, val string) (*client.Node, error)
	Get(prefix, ref string) (*client.Node, error)
	Delete(prefix, id string, recursive bool) error
}

type EtcdKV struct {
	client client.KeysAPI
	prefix string
}

func New(client client.KeysAPI, prefix string) *EtcdKV {
	return &EtcdKV{
		client: client,
		prefix: prefix,
	}
}

func (k *EtcdKV) List(prefix string) ([]*client.Node, error) {
	resp, err := k.client.Get(context.Background(), k.prefix+"/"+prefix, &client.GetOptions{Recursive: true})
	if err != nil {
		return nil, err
	}

	return resp.Node.Nodes, nil
}

func (k *EtcdKV) CreateWithIndex(prefix, id, name string, val string) (*client.Node, error) {
	resp, err := k.client.Set(context.Background(), k.prefix+"/"+prefix+"/"+id, val, nil)
	if err != nil {
		return nil, err
	}

	err = k.idxAdd(prefix, name, id)
	if err != nil {
		// log it
		fmt.Println("failed to create index: ", err)

	}
	return resp.Node, nil
}

func (k *EtcdKV) AddToIndex(prefix, name, id string) error {
	return k.idxAdd(prefix, name, id)
}

func (k *EtcdKV) DeleteFromIndex(prefix, name string) error {
	return k.idxDelete(prefix, name)
}

func (k *EtcdKV) Set(prefix, id, val string) (*client.Node, error) {
	resp, err := k.client.Set(context.Background(), k.prefix+"/"+prefix+"/"+id, val, nil)
	if err != nil {
		return nil, err
	}

	return resp.Node, nil
}

func (k *EtcdKV) Get(prefix, ref string) (*client.Node, error) {
	if validator.IsUUID(ref) {
		return k.get(prefix, ref)
	}
	id, err := k.idxFindID(prefix, ref)
	if err != nil {
		fmt.Println("index couldn't find it, getting by ref, error: ", err)
		// trying to get it by ref anyway
		return k.get(prefix, ref)
	}
	fmt.Println("index found: ", id)
	return k.get(prefix, id)
}

func (k *EtcdKV) get(prefix, id string) (*client.Node, error) {
	resp, err := k.client.Get(context.Background(), k.prefix+"/"+prefix+"/"+id, &client.GetOptions{Recursive: false})
	if err != nil {
		return nil, err
	}

	return resp.Node, nil
}

func (k *EtcdKV) Delete(prefix, id string, recursive bool) error {
	_, err := k.client.Delete(context.Background(), k.prefix+"/"+prefix+"/"+id, &client.DeleteOptions{Recursive: recursive})
	return err
}
