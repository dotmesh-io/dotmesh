package kv

import (
	"context"
	"encoding/json"

	"github.com/coreos/etcd/client"
)

const (
	NameIndexAPIPrefix = "nameidx"
)

type NameIndex struct {
	Prefix string `json:"prefix"`
	Name   string `json:"name"`
	ID     string `json:"id"`
}

func (k *KV) idxFindID(prefix, name string) (id string, err error) {
	var i NameIndex
	resp, err := k.client.Get(context.Background(), k.prefix+"/"+NameIndexAPIPrefix+"/"+prefix+"/"+name, &client.GetOptions{Recursive: true})
	if err != nil {
		return
	}

	err = json.Unmarshal([]byte(resp.Node.Value), &i)
	if err != nil {
		return
	}

	return i.ID, nil
}

func (k *KV) idxAdd(prefix, name, id string) error {
	val := NameIndex{
		Prefix: prefix,
		Name:   name,
		ID:     id,
	}

	bts, err := json.Marshal(&val)
	if err != nil {
		return err
	}

	_, err = k.Put(NameIndexAPIPrefix, name, string(bts))

	return err
}

func (k *KV) idxDelete(prefix, name string) error {
	return k.Delete(NameIndexAPIPrefix, name, false)
}
