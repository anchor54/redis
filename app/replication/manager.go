package replication

import (
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// Manager manages all replica connections
type Manager struct {
	replicas map[*core.RedisConnection]*ReplicaInfo // Map of replica ID -> ReplicaInfo
	mu       sync.RWMutex   // Protects concurrent access
}

var (
	manager *Manager
	once    sync.Once
)

// GetManager creates a new replication manager
func GetManager() *Manager {
	once.Do(func() {
		manager = &Manager{
			replicas: make(map[*core.RedisConnection]*ReplicaInfo),
		}
	})
	return manager
}

// AddReplica adds a new replica with the given ID
func (m *Manager) AddReplica(replicaInfo *ReplicaInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.replicas[replicaInfo.Connection] = replicaInfo
}

// GetAllReplicas returns all replicas
func (m *Manager) GetAllReplicas() []*ReplicaInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Collect(maps.Values(m.replicas))
}

// GetReplica returns the replica info for the given connection
func (m *Manager) GetReplica(conn *core.RedisConnection) *ReplicaInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.replicas[conn]
}

// GetReplicaCount returns the number of connected replicas
func (m *Manager) GetReplicaCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.replicas)
}

// PropagateCommand sends a command to all connected replicas
func (m *Manager) PropagateCommand(cmdName string, args []string) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Build RESP array format for the command
	cmdArray := []string{cmdName}
	cmdArray = append(cmdArray, args...)
	respCommand := utils.ToArray(cmdArray)

	fmt.Printf("Propagating command to %d replicas: %s\n", len(m.replicas), respCommand)

	// Send to all connected replicas
	for _, replica := range m.replicas {
		if replica.IsHandshakeDone && replica.Connection != nil {
			replica.Connection.SendResponse(respCommand)
		}
	}
}
