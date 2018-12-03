package main

func (f *fsMachine) GetMetadata(nodeID string) map[string]string {
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

func (f *fsMachine) ListMetadata() map[string]map[string]string {
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

func (f *fsMachine) SetMetadata(nodeID string, meta map[string]string) {
	f.stateMachineMetadataMu.Lock()
	f.stateMachineMetadata[nodeID] = meta
	f.stateMachineMetadataMu.Unlock()
}

func (f *fsMachine) GetSnapshots(nodeID string) []*Snapshot {
	res := []*Snapshot{}
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

func (f *fsMachine) ListSnapshots() map[string][]*Snapshot {
	f.snapshotCacheMu.RLock()
	defer f.snapshotCacheMu.RUnlock()

	r := make(map[string][]*Snapshot)

	for key, snapshots := range f.snapshotCache {

		snaps := make([]*Snapshot, len(snapshots))
		for idx, s := range snapshots {
			snaps[idx] = s.DeepCopy()
		}
		r[key] = snaps
	}

	return r
}

func (f *fsMachine) SetSnapshots(nodeID string, snapshots []*Snapshot) {
	f.snapshotCacheMu.Lock()
	f.snapshotCache[nodeID] = snapshots
	f.snapshotCacheMu.Unlock()
}
