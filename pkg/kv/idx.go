package kv

import (
	"encoding/json"
)

const (
	NameIndexAPIPrefix = "nameidx"
)

type NameIndex struct {
	Prefix string `json:"prefix"`
	Name   string `json:"name"`
	ID     string `json:"id"`
}

func (k *EtcdKV) idxFindID(prefix, name string) (id string, err error) {
	resp, err := k.get(k.prefix+" /"+NameIndexAPIPrefix, name)
	if err != nil {
		return
	}

	var i NameIndex
	err = json.Unmarshal([]byte(resp.Value), &i)
	if err != nil {
		return
	}

	return i.ID, nil
}

func (k *EtcdKV) idxAdd(prefix, name, id string) error {
	val := NameIndex{
		Prefix: prefix,
		Name:   name,
		ID:     id,
	}

	bts, err := json.Marshal(&val)
	if err != nil {
		return err
	}
	// fmt.Printf("index %s added for ID %s \n", name, id)
	_, err = k.Set(k.prefix+" /"+NameIndexAPIPrefix, name, string(bts))

	return err
}

func (k *EtcdKV) idxDelete(prefix, name string) error {
	return k.Delete(k.prefix+" /"+NameIndexAPIPrefix, name, false)
}
