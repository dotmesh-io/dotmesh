package store

import (
	"encoding/json"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/portworx/kvdb"

	log "github.com/sirupsen/logrus"
)

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
		servers = append(servers, &ss)
	}

	return servers, nil
}

func (s *KVServerStore) SetAddresses(si *types.Server, opts *SetOptions) error {

	_, err := s.client.Put(ServerAddressesPrefix+si.Id, si, opts.TTL)
	return err
}

func (s *KVServerStore) SetSnapshots(ss *types.ServerSnapshots) error {
	_, err := s.client.Put(ServerSnapshotsPrefix+ss.ID+"/"+ss.FilesystemID, ss, 0)
	return err
}

func (s *KVServerStore) SetState(ss *types.ServerState) error {
	_, err := s.client.Put(ServerStatesPrefix+ss.ID+"/"+ss.FilesystemID, ss, 0)
	return err
}
