package replication

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// Manager manages all replica connections
type Manager struct {
	replicas map[*core.RedisConnection]*ReplicaInfo // Map of replica ID -> ReplicaInfo
	mu       sync.RWMutex                           // Protects concurrent access
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
	if config.GetInstance().Role == config.Slave {
		return
	}
	
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Build RESP array format for the command
	cmdArray := []string{cmdName}
	cmdArray = append(cmdArray, args...)
	respCommand := utils.ToArray(cmdArray)

	// Increment the master offset
	config.GetInstance().Offset += len(respCommand)

	// Send to all connected replicas
	eligibleReplicas := m.getEligibleReplicas()
	for _, replica := range eligibleReplicas {
		replica.Connection.SendResponse(respCommand)
	}
}

func (m *Manager) WaitForReplicas(minReplicasToWaitFor int, timeout int) int {
	// If no replicas (who have completed the handshake) are available, return 0
	// If no replicas are needed to be waited for, return 0
	if minReplicasToWaitFor == 0 || len(m.getEligibleReplicas()) == 0 || config.GetInstance().Offset == 0 {
		return 0
	}

	// If no timeout is specified, wait indefinitely
	if timeout == 0 {
		for {
			updatedReplicas := m.sendGetAckToReplicas()
			if updatedReplicas >= minReplicasToWaitFor {
				return updatedReplicas
			}
		}
	}
	
	updatedReplicas := make(chan int)
	timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
	defer timer.Stop()

	for {
		updatedReplicas <- m.sendGetAckToReplicas()
		select {
		case <-timer.C:
			return <-updatedReplicas
		case updatedReplicas := <-updatedReplicas:
			if updatedReplicas >= minReplicasToWaitFor {
				return updatedReplicas
			}
		}
	}
}

func (m *Manager) getEligibleReplicas() []*ReplicaInfo {
	eligibleReplicas := make([]*ReplicaInfo, 0)
	for _, replica := range m.replicas {
		if replica.IsHandshakeDone {
			eligibleReplicas = append(eligibleReplicas, replica)
		}
	}
	return eligibleReplicas
}

func (m *Manager) sendGetAckToReplicas() int {
	eligibleReplicas := m.getEligibleReplicas()

	if len(eligibleReplicas) == 0 {
		return 0
	}

	replicaOffsets := make(chan int, len(eligibleReplicas))

	var wg sync.WaitGroup

	for _, replica := range eligibleReplicas {
		wg.Add(1)
		go func(r *ReplicaInfo) {
			defer wg.Done()
			r.Connection.SendResponse(utils.ToArray([]string{"REPLCONF", "GETACK", "*"}))
			replicaOffset := m.readReplicaOffset(r)
			replicaOffsets <- replicaOffset
		}(replica)
	}

	// Close the channel once all goroutines are done
	go func() {
		wg.Wait()
		close(replicaOffsets)
	}()

	updatedReplicas := 0
	for offset := range replicaOffsets {
		if offset >= config.GetInstance().Offset {
			updatedReplicas++
		}
	}

	return updatedReplicas
}

func (m *Manager) readReplicaOffset(replica *ReplicaInfo) int {
	buf := make([]byte, 1024)
	n, err := replica.Connection.Read(buf)
	if err != nil {
		return -1
	}

	respArray, err := utils.ParseRESPArray(string(buf[:n]))
	if err != nil {
		fmt.Printf("Failed to parse RESP array from replica: %v\n", err)
		return -1
	}

	// Flatten to get string array
	cmdParts := utils.FlattenRESPArray(respArray)

	// Verify it's REPLCONF ACK <number>
	if len(cmdParts) != 3 {
		fmt.Printf("Invalid response format: expected 3 parts, got %d\n", len(cmdParts))
		return -1
	}

	if cmdParts[0] != "REPLCONF" || cmdParts[1] != "ACK" {
		fmt.Printf("Invalid response: expected REPLCONF ACK, got %s %s\n", cmdParts[0], cmdParts[1])
		return -1
	}

	// Extract the number (offset)
	offset := cmdParts[2]
	fmt.Printf("Replica acknowledged with offset: %s\n", offset)

	// If you need the offset as an integer:
	offsetNum, err := strconv.Atoi(offset)
	if err != nil {
		fmt.Printf("Invalid offset number: %s\n", offset)
		return -1
	}

	return offsetNum
}
