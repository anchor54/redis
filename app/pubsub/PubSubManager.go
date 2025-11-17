package pubsub

import (
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/connection"
)

type PubSubManager struct {
	mu            sync.RWMutex
	channels      map[string]map[*connection.RedisConnection]bool
	subscriptions map[string]map[string]bool
}

var (
	manager *PubSubManager
	once    sync.Once
)

func GetManager() *PubSubManager {
	once.Do(func() {
		manager = &PubSubManager{
			channels:      make(map[string]map[*connection.RedisConnection]bool),
			subscriptions: make(map[string]map[string]bool),
		}
	})
	return manager
}

func (m *PubSubManager) Subscribe(conn *connection.RedisConnection, channel string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.channels[channel]; !ok {
		m.channels[channel] = make(map[*connection.RedisConnection]bool)
	}
	m.channels[channel][conn] = true

	if _, ok := m.subscriptions[conn.Id]; !ok {
		m.subscriptions[conn.Id] = make(map[string]bool)
	}
	m.subscriptions[conn.Id][channel] = true
	return len(m.subscriptions[conn.Id])
}

func (m *PubSubManager) Unsubscribe(conn *connection.RedisConnection, channel string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if connections, ok := m.channels[channel]; ok {
		delete(connections, conn)
	}

	if subscriptions, ok := m.subscriptions[conn.Id]; ok {
		delete(subscriptions, channel)
	}
	return len(m.subscriptions[conn.Id])
}

func (m *PubSubManager) Publish(channel string, message string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if connections, ok := m.channels[channel]; ok {
		for conn := range connections {
			conn.SendResponse(message)
		}
	}
}
