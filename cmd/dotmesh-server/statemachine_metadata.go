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

func (f *fsMachine) GetSnapshots(nodeID string) []*snapshot {
	res := []*snapshot{}
	f.snapshotCacheMu.RLock()
	defer f.snapshotCacheMu.RUnlock()
	// TODO: need to create deep copy of this slice
	// as it contains pointers inside
	snaps, ok := f.snapshotCache[nodeID]
	if !ok {
		return res
	}
	return snaps
}

func (f *fsMachine) ListSnapshots() map[string][]*snapshot {
	f.snapshotCacheMu.RLock()
	defer f.snapshotCacheMu.RUnlock()
	return f.snapshotCache
}

func (f *fsMachine) SetSnapshots(nodeID string, snapshots []*snapshot) {
	f.snapshotCacheMu.Lock()
	f.snapshotCache[nodeID] = snapshots
	f.snapshotCacheMu.Unlock()
}
