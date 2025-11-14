package replication

import (
	"strconv"
	"sync"
)

// Manager manages all replica connections
type Manager struct {
	replicas map[string]*ReplicaInfo // Map of replica ID -> ReplicaInfo
	mu       sync.RWMutex            // Protects concurrent access
}


var (
	manager *Manager
	once sync.Once
)

// GetManager creates a new replication manager
func GetManager() *Manager {
	once.Do(func() {
		manager = &Manager{
			replicas: make(map[string]*ReplicaInfo),
		}
	})
	return manager
}

// AddReplica adds a new replica with the given ID
func (m *Manager) AddReplica(replicaInfo *ReplicaInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.replicas[strconv.Itoa(replicaInfo.ListeningPort)] = replicaInfo
}

// GetReplica returns a replica by ID
func (m *Manager) GetReplica(replicaID string) (*ReplicaInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	replica, exists := m.replicas[replicaID]
	return replica, exists
}

// GetReplicaCount returns the number of connected replicas
func (m *Manager) GetReplicaCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.replicas)
}
