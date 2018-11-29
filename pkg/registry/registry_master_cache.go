package registry

import "fmt"

func (r *DefaultRegistry) CurrentMasterNode(filesystemID string) (string, error) {
	r.mastersCacheLock.RLock()
	defer r.mastersCacheLock.RUnlock()

	masterNodeID, ok := r.mastersCache[filesystemID]
	if !ok {
		return "", fmt.Errorf("No known filesystem with id %s", filesystemID)
	}
	return masterNodeID, nil
}

func (r *DefaultRegistry) SetMasterNode(filesystemID, nodeID string) {
	r.mastersCacheLock.Lock()
	r.mastersCache[filesystemID] = nodeID
	r.mastersCacheLock.Unlock()
}

func (r *DefaultRegistry) GetMasterNode(filesystemID string) (string, bool) {
	r.mastersCacheLock.RLock()
	masterNodeID, ok := r.mastersCache[filesystemID]
	r.mastersCacheLock.RUnlock()
	return masterNodeID, ok
}

type ListMasterNodesQuery struct {
	NodeID string
}

func (r *DefaultRegistry) ListMasterNodes(query *ListMasterNodesQuery) map[string]string {
	r.mastersCacheLock.RLock()
	defer r.mastersCacheLock.RUnlock()

	results := make(map[string]string)
	for filesystemID, nodeID := range r.mastersCache {
		if query.NodeID != "" && nodeID != query.NodeID {
			continue
		}
		results[filesystemID] = nodeID
	}
	return results
}

func (r *DefaultRegistry) DeleteMasterNode(filesystemID string) {
	r.mastersCacheLock.Lock()
	delete(r.mastersCache, filesystemID)
	r.mastersCacheLock.Unlock()
}
