package pubsub

import (
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/connection"
)

type PubSubManager struct {
	mu            sync.RWMutex
	subscriptions map[*connection.RedisConnection][]string
	channels      map[string][]*connection.RedisConnection
}

var (
	manager *PubSubManager
	once    sync.Once
)

func GetManager() *PubSubManager {
	once.Do(func() {
		manager = &PubSubManager{
			channels:      make(map[string][]*connection.RedisConnection),
			subscriptions: make(map[*connection.RedisConnection][]string),
		}
	})
	return manager
}

func (m *PubSubManager) Subscribe(conn *connection.RedisConnection, channel string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.channels[channel]; !ok {
		m.channels[channel] = make([]*connection.RedisConnection, 0)
		m.subscriptions[conn] = append(m.subscriptions[conn], channel)
	}
	m.channels[channel] = append(m.channels[channel], conn)
	m.subscriptions[conn] = append(m.subscriptions[conn], channel)

	return len(m.channels[channel])
}

func (m *PubSubManager) Unsubscribe(conn *connection.RedisConnection, channel string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if connections, ok := m.channels[channel]; ok {
		for i, c := range connections {
			if c == conn {
				m.channels[channel] = append(connections[:i], connections[i+1:]...)
				break
			}
		}
	}

	if subscriptions, ok := m.subscriptions[conn]; ok {
		for i, c := range subscriptions {
			if c == channel {
				m.subscriptions[conn] = append(subscriptions[:i], subscriptions[i+1:]...)
				break
			}
		}
	}
	return len(m.subscriptions[conn])
}

func (m *PubSubManager) Publish(channel string, message string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if connections, ok := m.channels[channel]; ok {
		for _, conn := range connections {
			conn.SendResponse(message)
		}
	}
}
