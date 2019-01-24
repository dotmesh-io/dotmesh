package store

import (
	"encoding/json"
	"fmt"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/portworx/kvdb"

	log "github.com/sirupsen/logrus"
)

// static ServerStore check
var _ ServerStore = &KVServerStore{}

type KVServerStore struct {
	client kvdb.Kvdb
}

const (
	ServerAddressesPrefix = "servers/addresses/"
	ServerSnapshotsPrefix = "servers/snapshots/"
	ServerStatesPrefix    = "servers/states/"
)

func NewKVServerStore(cfg *KVDBConfig) (*KVServerStore, error) {
	client, err := getKVDBClient(cfg)
	if err != nil {
		return nil, err
	}

	return &KVServerStore{
		client: client,
	}, nil
}

func (s *KVServerStore) ListAddresses() ([]*types.Server, error) {
	pairs, err := s.client.Enumerate(ServerAddressesPrefix)
	if err != nil {
		return nil, err
	}
	var servers []*types.Server

	for _, kvp := range pairs {
		var ss types.Server

		err = json.Unmarshal(kvp.Value, &ss)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"key":   kvp.Key,
				"value": string(kvp.Value),
			}).Error("failed to unmarshal value into types.Server")
			continue
		}

		ss.Meta = getMeta(kvp)

		servers = append(servers, &ss)
	}

	return servers, nil
}

func (s *KVServerStore) SetAddresses(si *types.Server, opts *SetOptions) error {
	_, err := s.client.Put(ServerAddressesPrefix+si.Id, si, opts.TTL)
	return err
}

func (s *KVServerStore) WatchAddresses(idx uint64, cb WatchServerAddressesClonesCB) error {
	watchFunc := func(prefix string, opaque interface{}, kvp *kvdb.KVPair, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"prefix": prefix,
			}).Error("[WatchAddresses] error while watching KV store tree")
		}

		var srv types.Server
		err = s.decode(kvp.Value, &srv)
		if err != nil {
			return fmt.Errorf("failed to decode value from key '%s', error: %s", prefix, err)
		}

		srv.Meta = getMeta(kvp)

		return cb(&srv)
	}

	return s.client.WatchTree(ServerAddressesPrefix, idx, nil, watchFunc)
}

func (s *KVServerStore) SetSnapshots(ss *types.ServerSnapshots) error {
	_, err := s.client.Put(ServerSnapshotsPrefix+ss.ID+"/"+ss.FilesystemID, ss, 0)
	return err
}

func (s *KVServerStore) WatchSnapshots(idx uint64, cb WatchServerSnapshotsClonesCB) error {
	watchFunc := func(prefix string, opaque interface{}, kvp *kvdb.KVPair, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"prefix": prefix,
			}).Error("[WatchSnapshots] error while watching KV store tree")
		}

		var ss types.ServerSnapshots
		err = s.decode(kvp.Value, &ss)
		if err != nil {
			return fmt.Errorf("failed to decode value from key '%s', error: %s", prefix, err)
		}

		ss.Meta = getMeta(kvp)

		return cb(&ss)
	}

	return s.client.WatchTree(ServerSnapshotsPrefix, idx, nil, watchFunc)
}

func (s *KVServerStore) ListSnapshots() ([]*types.ServerSnapshots, error) {
	pairs, err := s.client.Enumerate(ServerSnapshotsPrefix)
	if err != nil {
		return nil, err
	}
	var result []*types.ServerSnapshots

	for _, kvp := range pairs {
		var val types.ServerSnapshots

		err = json.Unmarshal(kvp.Value, &val)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"key":   kvp.Key,
				"value": string(kvp.Value),
			}).Error("failed to unmarshal value")
			continue
		}

		val.Meta = getMeta(kvp)

		result = append(result, &val)
	}

	return result, nil
}

func (s *KVServerStore) SetState(ss *types.ServerState) error {
	_, err := s.client.Put(ServerStatesPrefix+ss.ID+"/"+ss.FilesystemID, ss, 0)
	return err
}

func (s *KVServerStore) WatchStates(idx uint64, cb WatchServerStatesClonesCB) error {
	watchFunc := func(prefix string, opaque interface{}, kvp *kvdb.KVPair, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"prefix": prefix,
			}).Error("[WatchStates] error while watching KV store tree")
		}

		var ss types.ServerState
		err = s.decode(kvp.Value, &ss)
		if err != nil {
			return fmt.Errorf("failed to decode value from key '%s', error: %s", prefix, err)
		}

		ss.Meta = getMeta(kvp)

		return cb(&ss)
	}

	return s.client.WatchTree(ServerStatesPrefix, idx, nil, watchFunc)
}

func (s *KVServerStore) ListStates() ([]*types.ServerState, error) {
	pairs, err := s.client.Enumerate(ServerStatesPrefix)
	if err != nil {
		return nil, err
	}
	var result []*types.ServerState

	for _, kvp := range pairs {
		var val types.ServerState

		err = json.Unmarshal(kvp.Value, &val)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"key":   kvp.Key,
				"value": string(kvp.Value),
			}).Error("failed to unmarshal value")
			continue
		}

		val.Meta = getMeta(kvp)

		result = append(result, &val)
	}

	return result, nil
}
