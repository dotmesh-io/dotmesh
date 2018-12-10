package fsm

import "github.com/dotmesh-io/dotmesh/pkg/types"

func (f *FsMachine) GetMetadata(nodeID string) map[string]string {
	res := make(map[string]string)
	f.stateMachineMetadataMu.RLock()
	defer f.stateMachineMetadataMu.RUnlock()
	m, ok := f.stateMachineMetadata[nodeID]
	if !ok {
		return res
	}

	for k, v := range m {
		res[k] = v
	}
	return res
}

func (f *FsMachine) ListMetadata() map[string]map[string]string {
	res := make(map[string]map[string]string)
	f.stateMachineMetadataMu.RLock()
	for server, serverMeta := range f.stateMachineMetadata {

		cp := make(map[string]string)
		for k, v := range serverMeta {
			cp[k] = v
		}

		res[server] = cp
	}
	f.stateMachineMetadataMu.RUnlock()

	return res
}

func (f *FsMachine) SetMetadata(nodeID string, meta map[string]string) {
	f.stateMachineMetadataMu.Lock()
	f.stateMachineMetadata[nodeID] = meta
	f.stateMachineMetadataMu.Unlock()
}

func (f *FsMachine) GetSnapshots(nodeID string) []*types.Snapshot {
	res := []*types.Snapshot{}
	f.snapshotCacheMu.RLock()
	defer f.snapshotCacheMu.RUnlock()

	snaps, ok := f.snapshotCache[nodeID]
	if !ok {
		return res
	}

	for _, s := range snaps {
		res = append(res, s.DeepCopy())
	}

	return res
}

func (f *FsMachine) ListSnapshots() map[string][]*types.Snapshot {
	f.snapshotCacheMu.RLock()
	defer f.snapshotCacheMu.RUnlock()

	r := make(map[string][]*types.Snapshot)

	for key, snapshots := range f.snapshotCache {

		snaps := make([]*types.Snapshot, len(snapshots))
		for idx, s := range snapshots {
			snaps[idx] = s.DeepCopy()
		}
		r[key] = snaps
	}

	return r
}

func (f *FsMachine) SetSnapshots(nodeID string, snapshots []*types.Snapshot) {
	f.snapshotCacheMu.Lock()
	f.snapshotCache[nodeID] = snapshots
	f.snapshotCacheMu.Unlock()
}
