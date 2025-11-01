package main

import "sync"

// Generic wrapper around sync.Map
type SyncMap[K comparable, V any] struct {
	m sync.Map
}

func (sm *SyncMap[K, V]) Store(key K, value V) {
	sm.m.Store(key, value)
}

func (sm *SyncMap[K, V]) Load(key K) (V, bool) {
	val, ok := sm.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return val.(V), true
}

func (sm *SyncMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	val, loaded := sm.m.LoadOrStore(key, value)
	return val.(V), loaded
}

func (sm *SyncMap[K, V]) Delete(key K) {
	sm.m.Delete(key)
}

func (sm *SyncMap[K, V]) Range(f func(key K, value V) bool) {
	sm.m.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}
