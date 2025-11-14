package replication

// ReplicaInfo holds basic information about a connected replica
type ReplicaInfo struct {
	ListeningPort int      // Port the replica is listening on (from REPLCONF listening-port)
	Capabilities  []string // Capabilities reported by replica (from REPLCONF capa, e.g., "psync2")
}

// NewReplicaInfo creates a new ReplicaInfo
func NewReplicaInfo() *ReplicaInfo {
	return &ReplicaInfo{
		Capabilities: make([]string, 0),
	}
}

// AddCapability adds a capability to the replica
func (r *ReplicaInfo) AddCapability(capability string) {
	r.Capabilities = append(r.Capabilities, capability)
}

// HasCapability checks if the replica has a specific capability
func (r *ReplicaInfo) HasCapability(capability string) bool {
	for _, cap := range r.Capabilities {
		if cap == capability {
			return true
		}
	}
	return false
}
