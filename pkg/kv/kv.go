package kv

import (
	"context"

	"github.com/coreos/etcd/client"
	"github.com/dotmesh-io/dotmesh/pkg/validator"
)

type KV struct {
	client client.KeysAPI
	prefix string
}

func New(client client.KeysAPI, prefix string) *KV {
	return &KV{
		client: client,
	}
}

func (k *KV) List(prefix string) ([]*client.Node, error) {
	resp, err := k.client.Get(context.Background(), k.prefix+"/"+prefix, &client.GetOptions{Recursive: true})
	if err != nil {
		return nil, err
	}

	return resp.Node.Nodes, nil
}

func (k *KV) CreateWithIndex(prefix, id, name string, val string) (*client.Node, error) {
	resp, err := k.client.Set(context.Background(), k.prefix+"/"+prefix+"/"+id, val, nil)
	if err != nil {
		return nil, err
	}

	err = k.idxAdd(prefix, name, id)
	if err != nil {
		// log it
	}
	return resp.Node, nil
}

func (k *KV) DeleteFromIndex(prefix, name string) error {
	return k.idxDelete(prefix, name)
}

func (k *KV) Put(prefix, id, val string) (*client.Node, error) {
	resp, err := k.client.Set(context.Background(), k.prefix+"/"+prefix+"/"+id, val, nil)
	if err != nil {
		return nil, err
	}

	return resp.Node, nil
}

func (k *KV) Get(prefix, ref string) (*client.Node, error) {
	if validator.IsUUID(ref) {
		return k.get(prefix, ref)
	}
	id, err := k.idxFindID(prefix, ref)
	if err != nil {
		return nil, err
	}
	return k.get(prefix, id)
}

func (k *KV) get(prefix, id string) (*client.Node, error) {
	resp, err := k.client.Get(context.Background(), k.prefix+"/"+prefix+"/"+id, &client.GetOptions{Recursive: false})
	if err != nil {
		return nil, err
	}

	return resp.Node, nil
}

func (k *KV) Delete(prefix, id string, recursive bool) error {
	_, err := k.client.Delete(context.Background(), k.prefix+"/"+prefix+"/"+id, &client.DeleteOptions{Recursive: recursive})
	return err
}
