package store

import (
	"encoding/json"

	"github.com/dotmesh-io/dotmesh/pkg/types"
	"github.com/portworx/kvdb"

	log "github.com/sirupsen/logrus"

	"fmt"
)

// static FilesystemStore check
var _ FilesystemStore = &KVDBFilesystemStore{}

// Master

// SetMaster - creates master entry only if the key didn't previously exist (using Create method)
func (s *KVDBFilesystemStore) SetMaster(fm *types.FilesystemMaster, opts *SetOptions) error {

	if fm.FilesystemID == "" {
		log.WithFields(log.Fields{
			"error":  ErrIDNotSet,
			"object": fm,
		}).Error("[SetMaster] called without FilesystemID")
		return ErrIDNotSet
	}

	bts, err := s.encode(fm)
	if err != nil {
		return err
	}

	if opts.Force {
		_, err = s.client.Put(FilesystemMastersPrefix+fm.FilesystemID, bts, 0)
		return err
	}

	_, err = s.client.Create(FilesystemMastersPrefix+fm.FilesystemID, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) ImportMasters(fs []*types.FilesystemMaster, opts *ImportOptions) error {
	if opts.DeleteExisting {
		err := s.client.DeleteTree(FilesystemMastersPrefix)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Warn("[ImportMasters] failed to delete existing registry tree before importing")
		}
	}
	for _, f := range fs {
		err := s.SetMaster(f, &SetOptions{Force: false})
		if err != nil {
			log.WithFields(log.Fields{
				"error":         err,
				"filesystem_id": f.FilesystemID,
			}).Warn("[ImportMasters] failed to import")
		}
	}
	return nil
}

func (s *KVDBFilesystemStore) CompareAndSetMaster(fm *types.FilesystemMaster, opts *SetOptions) error {
	if fm.FilesystemID == "" {
		log.WithFields(log.Fields{
			"error":  ErrIDNotSet,
			"object": fm,
		}).Error("[CompareAndSetMaster] called without FilesystemID")
		return ErrIDNotSet
	}

	bts, err := s.encode(fm)
	if err != nil {
		return err
	}

	kvp := &kvdb.KVPair{
		Key:           FilesystemMastersPrefix + fm.FilesystemID,
		Value:         bts,
		ModifiedIndex: fm.Meta.ModifiedIndex,
	}

	_, err = s.client.CompareAndSet(kvp, opts.KVFlags, opts.PrevValue)
	return err
}

func (s *KVDBFilesystemStore) GetMaster(id string) (*types.FilesystemMaster, error) {
	node, err := s.client.Get(FilesystemMastersPrefix + id)
	if err != nil {
		return nil, err
	}
	var f types.FilesystemMaster
	err = s.decode(node.Value, &f)

	f.Meta = getMeta(node)

	return &f, err
}

func (s *KVDBFilesystemStore) DeleteMaster(id string) error {
	_, err := s.client.Delete(FilesystemMastersPrefix + id)
	return err
}

func (s *KVDBFilesystemStore) WatchMasters(idx uint64, cb WatchMasterCB) error {
	watchFunc := func(prefix string, opaque interface{}, kvp *kvdb.KVPair, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"prefix": prefix,
			}).Error("[WatchMasters] error while watching KV store tree")
			return err
		}

		var f types.FilesystemMaster

		if kvp.Action == kvdb.KVDelete {
			id, err := extractID(kvp.Key)
			if err != nil {
				return nil
			}
			f.FilesystemID = id
			f.Meta = getMeta(kvp)
			cb(&f)
			return nil
		}

		err = s.decode(kvp.Value, &f)
		if err != nil {
			log.WithFields(log.Fields{
				"prefix": prefix,
				"action": ActionString(kvp.Action),
				"error":  err,
				"value":  string(kvp.Value),
				"key":    kvp.Key,
			}).Error("[WatchMasters] failed to decode JSON")
			return nil
		}
		f.Meta = getMeta(kvp)

		err = cb(&f)
		if err != nil {
			log.WithFields(log.Fields{
				"error":        err,
				"key":          kvp.Key,
				"action":       kvp.Action,
				"modified_idx": kvp.ModifiedIndex,
			}).Error("[WatchMasters] callback returned an error")
		}
		// don't return an error, it will stop the watcher
		return nil
	}

	return s.client.WatchTree(FilesystemMastersPrefix, idx, nil, watchFunc)
}

func (s *KVDBFilesystemStore) ListMaster() ([]*types.FilesystemMaster, error) {
	pairs, err := s.client.Enumerate(FilesystemMastersPrefix)
	if err != nil {
		return nil, err
	}
	var result []*types.FilesystemMaster

	for _, kvp := range pairs {
		var val types.FilesystemMaster

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

// Deleted

func (s *KVDBFilesystemStore) SetDeleted(f *types.FilesystemDeletionAudit, opts *SetOptions) error {

	if f.FilesystemID == "" {
		log.WithFields(log.Fields{
			"error":  ErrIDNotSet,
			"object": f,
		}).Error("[SetDeleted] called without FilesystemID")
		return ErrIDNotSet
	}

	bts, err := s.encode(f)
	if err != nil {
		return err
	}

	_, err = s.client.Create(FilesystemDeletedPrefix+f.FilesystemID, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) GetDeleted(id string) (*types.FilesystemDeletionAudit, error) {
	node, err := s.client.Get(FilesystemDeletedPrefix + id)
	if err != nil {
		return nil, err
	}
	var f types.FilesystemDeletionAudit
	err = s.decode(node.Value, &f)

	f.Meta = getMeta(node)

	return &f, err
}

func (s *KVDBFilesystemStore) DeleteDeleted(id string) error {
	_, err := s.client.Delete(FilesystemDeletedPrefix + id)
	return err
}

func (s *KVDBFilesystemStore) WatchDeleted(idx uint64, cb WatchDeletedCB) error {
	watchFunc := func(prefix string, opaque interface{}, kvp *kvdb.KVPair, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"prefix": prefix,
			}).Error("[WatchDeleted] error while watching KV store tree")
			return err
		}

		var f types.FilesystemDeletionAudit
		if kvp.Action == kvdb.KVDelete {
			id, err := extractID(kvp.Key)
			if err != nil {
				return nil
			}
			f.FilesystemID = id
			f.Meta = getMeta(kvp)
			cb(&f)
			return nil
		}
		err = s.decode(kvp.Value, &f)
		if err != nil {
			log.WithFields(log.Fields{
				"prefix": prefix,
				"action": ActionString(kvp.Action),
				"error":  err,
				"value":  string(kvp.Value),
				"key":    kvp.Key,
			}).Error("[WatchDeleted] failed to decode JSON")
			return nil
		}

		f.Meta = getMeta(kvp)

		err = cb(&f)
		if err != nil {
			log.WithFields(log.Fields{
				"error":        err,
				"key":          kvp.Key,
				"action":       kvp.Action,
				"modified_idx": kvp.ModifiedIndex,
			}).Error("[WatchDeleted] callback returned an error")
		}
		// don't propagate the error, it will stop the watcher
		return nil
	}

	return s.client.WatchTree(FilesystemDeletedPrefix, idx, nil, watchFunc)
}

func (s *KVDBFilesystemStore) ListDeleted() ([]*types.FilesystemDeletionAudit, error) {
	pairs, err := s.client.Enumerate(FilesystemDeletedPrefix)
	if err != nil {
		return nil, err
	}
	var result []*types.FilesystemDeletionAudit

	for _, kvp := range pairs {
		var val types.FilesystemDeletionAudit

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

// CleanupPending

func (s *KVDBFilesystemStore) SetCleanupPending(f *types.FilesystemDeletionAudit, opts *SetOptions) error {

	if f.FilesystemID == "" {
		log.WithFields(log.Fields{
			"error":  ErrIDNotSet,
			"object": f,
		}).Error("[SetCleanupPending] called without FilesystemID")
		return ErrIDNotSet
	}

	bts, err := s.encode(f)
	if err != nil {
		return err
	}

	_, err = s.client.Create(FilesystemCleanupPendingPrefix+f.FilesystemID, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) DeleteCleanupPending(id string) error {
	if id == "" {
		return ErrIDNotSet
	}
	_, err := s.client.Delete(FilesystemCleanupPendingPrefix + id)
	return err
}

func (s *KVDBFilesystemStore) ListCleanupPending() ([]*types.FilesystemDeletionAudit, error) {
	pairs, err := s.client.Enumerate(FilesystemCleanupPendingPrefix)
	if err != nil {
		return nil, err
	}
	var audits []*types.FilesystemDeletionAudit

	for _, kvp := range pairs {
		var f types.FilesystemDeletionAudit
		err = s.decode(kvp.Value, &f)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
				"key":   kvp.Key,
				"value": string(kvp.Value),
			}).Error("failed to unmarshal value into types.FilesystemDeletionAudit")
			continue
		}
		f.Meta = getMeta(kvp)
		audits = append(audits, &f)
	}

	return audits, nil
}

func (s *KVDBFilesystemStore) WatchCleanupPending(idx uint64, cb WatchCleanupPendingCB) error {

	watchFunc := func(prefix string, opaque interface{}, kvp *kvdb.KVPair, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"prefix": prefix,
			}).Error("[WatchCleanupPending] error while watching KV store tree")
			return err
		}

		var f types.FilesystemDeletionAudit
		if kvp.Action == kvdb.KVDelete {
			id, err := extractID(kvp.Key)
			if err != nil {
				return nil
			}
			f.FilesystemID = id
			f.Meta = getMeta(kvp)
			cb(&f)
			return nil
		}

		err = s.decode(kvp.Value, &f)
		if err != nil {
			log.WithFields(log.Fields{
				"prefix": prefix,
				"action": ActionString(kvp.Action),
				"value":  string(kvp.Value),
				"key":    kvp.Key,
				"error":  err,
			}).Error("[WatchCleanupPending] failed to decode JSON")
			return nil
		}

		f.Meta = getMeta(kvp)

		err = cb(&f)
		if err != nil {
			log.WithFields(log.Fields{
				"error":        err,
				"key":          kvp.Key,
				"action":       kvp.Action,
				"modified_idx": kvp.ModifiedIndex,
			}).Error("[WatchCleanupPending] callback returned an error")
		}
		// don't propagate the error, it will stop the watcher
		return nil
	}

	return s.client.WatchTree(FilesystemCleanupPendingPrefix, idx, nil, watchFunc)
}

// Live filesystems

func (s *KVDBFilesystemStore) SetLive(f *types.FilesystemLive, opts *SetOptions) error {
	if f.FilesystemID == "" {
		log.WithFields(log.Fields{
			"error":  ErrIDNotSet,
			"object": f,
		}).Error("[SetLive] called without FilesystemID")
		return ErrIDNotSet
	}

	bts, err := s.encode(f)
	if err != nil {
		return err
	}
	_, err = s.client.Put(FilesystemLivePrefix+f.FilesystemID, bts, opts.TTL)
	return err
}

func (s *KVDBFilesystemStore) GetLive(id string) (*types.FilesystemLive, error) {
	if id == "" {
		return nil, ErrIDNotSet
	}

	node, err := s.client.Get(FilesystemLivePrefix + id)
	if err != nil {
		return nil, err
	}
	var f types.FilesystemLive
	err = s.decode(node.Value, &f)

	f.Meta = getMeta(node)

	return &f, err
}

func (s *KVDBFilesystemStore) SetContainers(f *types.FilesystemContainers, opts *SetOptions) error {
	if f.FilesystemID == "" {
		return ErrIDNotSet
	}

	bts, err := s.encode(f)
	if err != nil {
		return err
	}
	_, err = s.client.Put(FilesystemContainersPrefix+f.FilesystemID, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) DeleteContainers(id string) error {
	if id == "" {
		return ErrIDNotSet
	}

	_, err := s.client.Delete(FilesystemContainersPrefix + id)
	return err
}

func (s *KVDBFilesystemStore) WatchContainers(idx uint64, cb WatchContainersCB) error {

	watchFunc := func(prefix string, opaque interface{}, kvp *kvdb.KVPair, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"prefix": prefix,
			}).Error("[WatchContainers] error while watching KV store tree")
			return err
		}

		var f types.FilesystemContainers
		if kvp.Action == kvdb.KVDelete {
			id, err := extractID(kvp.Key)
			if err != nil {
				return nil
			}
			f.FilesystemID = id
			f.Meta = getMeta(kvp)
			cb(&f)
			return nil
		}

		err = s.decode(kvp.Value, &f)
		if err != nil {
			log.WithFields(log.Fields{
				"prefix": prefix,
				"action": ActionString(kvp.Action),
				"error":  err,
			}).Error("[WatchContainers] failed to decode JSON")
			return nil
		}

		f.Meta = getMeta(kvp)

		err = cb(&f)
		if err != nil {
			log.WithFields(log.Fields{
				"error":        err,
				"key":          kvp.Key,
				"action":       kvp.Action,
				"modified_idx": kvp.ModifiedIndex,
			}).Error("[WatchContainers] callback returned an error")
		}
		// don't propagate the error, it will stop the watcher
		return nil
	}

	return s.client.WatchTree(FilesystemContainersPrefix, idx, nil, watchFunc)
}

func (s *KVDBFilesystemStore) ListContainers() ([]*types.FilesystemContainers, error) {
	pairs, err := s.client.Enumerate(FilesystemContainersPrefix)
	if err != nil {
		return nil, err
	}
	var result []*types.FilesystemContainers

	for _, kvp := range pairs {
		var val types.FilesystemContainers

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

func (s *KVDBFilesystemStore) SetDirty(f *types.FilesystemDirty, opts *SetOptions) error {
	if f.FilesystemID == "" {
		log.WithFields(log.Fields{
			"error":  ErrIDNotSet,
			"object": f,
		}).Error("[SetDirty] called without FilesystemID")
		return ErrIDNotSet
	}

	bts, err := s.encode(f)
	if err != nil {
		return err
	}
	_, err = s.client.Put(FilesystemDirtyPrefix+f.FilesystemID, bts, 0)
	return err
}

func (s *KVDBFilesystemStore) DeleteDirty(id string) error {
	_, err := s.client.Delete(FilesystemDirtyPrefix + id)
	return err
}

func (s *KVDBFilesystemStore) WatchDirty(idx uint64, cb WatchDirtyCB) error {
	watchFunc := func(prefix string, opaque interface{}, kvp *kvdb.KVPair, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"prefix": prefix,
			}).Error("[WatchDirty] error while watching KV store tree")
			return err
		}

		var f types.FilesystemDirty
		if kvp.Action == kvdb.KVDelete {
			id, err := extractID(kvp.Key)
			if err != nil {
				return nil
			}
			f.FilesystemID = id
			f.Meta = getMeta(kvp)
			cb(&f)
			return nil
		}

		err = s.decode(kvp.Value, &f)
		if err != nil {
			log.WithFields(log.Fields{
				"prefix": prefix,
				"action": ActionString(kvp.Action),
				"error":  err,
				"val":    string(kvp.Value),
			}).Error("[WatchDirty] failed to decode JSON")
			return nil
		}

		f.Meta = getMeta(kvp)

		err = cb(&f)
		if err != nil {
			log.WithFields(log.Fields{
				"error":        err,
				"key":          kvp.Key,
				"action":       kvp.Action,
				"modified_idx": kvp.ModifiedIndex,
			}).Error("[WatchDirty] callback returned an error")
		}
		// don't propagate the error, it will stop the watcher
		return nil
	}

	return s.client.WatchTree(FilesystemDirtyPrefix, idx, nil, watchFunc)
}

func (s *KVDBFilesystemStore) ListDirty() ([]*types.FilesystemDirty, error) {
	pairs, err := s.client.Enumerate(FilesystemDirtyPrefix)
	if err != nil {
		return nil, err
	}
	var result []*types.FilesystemDirty

	for _, kvp := range pairs {
		var val types.FilesystemDirty

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

func (s *KVDBFilesystemStore) SetTransfer(t *types.TransferPollResult, opts *SetOptions) error {
	if t.TransferRequestId == "" {
		log.WithFields(log.Fields{
			"error":  ErrIDNotSet,
			"object": t,
		}).Error("[SetTransfer] called without TransferRequestId")
		return ErrIDNotSet
	}

	bts, err := s.encode(t)
	if err != nil {
		return err
	}
	_, err = s.client.Put(FilesystemTransfersPrefix+t.TransferRequestId, bts, 0)

	return err
}

func (s *KVDBFilesystemStore) WatchTransfers(idx uint64, cb WatchTransfersCB) error {

	watchFunc := func(prefix string, opaque interface{}, kvp *kvdb.KVPair, err error) error {
		if err != nil {
			log.WithFields(log.Fields{
				"error":  err,
				"prefix": prefix,
			}).Error("[WatchTransfers] error while watching KV store tree")
			return err
		}

		var t types.TransferPollResult
		if kvp.Action == kvdb.KVDelete {
			id, err := extractID(kvp.Key)
			if err != nil {
				return nil
			}
			t.TransferRequestId = id
			t.Meta = getMeta(kvp)
			cb(&t)
			return nil
		}

		err = s.decode(kvp.Value, &t)
		if err != nil {
			log.WithFields(log.Fields{
				"prefix": prefix,
				"action": ActionString(kvp.Action),
				"error":  err,
				"val":    string(kvp.Value),
			}).Error("[WatchTransfers] failed to decode JSON")
			return nil
		}

		t.Meta = getMeta(kvp)

		err = cb(&t)
		if err != nil {
			log.WithFields(log.Fields{
				"error":        err,
				"key":          kvp.Key,
				"action":       kvp.Action,
				"modified_idx": kvp.ModifiedIndex,
			}).Error("[WatchTransfers] callback returned an error")
		}
		// don't propagate the error, it will stop the watcher
		return nil
	}

	return s.client.WatchTree(FilesystemTransfersPrefix, idx, nil, watchFunc)
}

func (s *KVDBFilesystemStore) ListTransfers() ([]*types.TransferPollResult, error) {
	pairs, err := s.client.Enumerate(FilesystemTransfersPrefix)
	if err != nil {
		return nil, err
	}
	var result []*types.TransferPollResult

	for _, kvp := range pairs {
		var val types.TransferPollResult

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
