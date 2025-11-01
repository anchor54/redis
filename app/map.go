package main

import "sync"

// Encapsulated, type-safe concurrent map
type SyncMap[K comparable, V any] struct {
	m sync.Map // map[K]*entry[V]
}

type entry[V any] struct {
	mu  sync.Mutex
	val V
}

// Store overwrites the value for key with per-key lock.
func (sm *SyncMap[K, V]) Store(key K, value V) {
	e, _ := sm.getOrCreateEntry(key)
	e.mu.Lock()
	e.val = value
	e.mu.Unlock()
}

// Load returns a copy of the value if present.
func (sm *SyncMap[K, V]) Load(key K) (V, bool) {
	eVal, ok := sm.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	e := eVal.(*entry[V])
	e.mu.Lock()
	val := e.val
	e.mu.Unlock()
	return val, true
}

// Update applies a function to the current value atomically.
func (sm *SyncMap[K, V]) Update(key K, fn func(old V) V) {
	e, _ := sm.getOrCreateEntry(key)
	e.mu.Lock()
	e.val = fn(e.val)
	e.mu.Unlock()
}

// Delete removes a key from the map.
func (sm *SyncMap[K, V]) Delete(key K) {
	sm.m.Delete(key)
}

// Range iterates over all entries (calls fn with copies of the values).
func (sm *SyncMap[K, V]) Range(fn func(K, V) bool) {
	sm.m.Range(func(k, v any) bool {
		key := k.(K)
		e := v.(*entry[V])
		e.mu.Lock()
		val := e.val
		e.mu.Unlock()
		return fn(key, val)
	})
}

// helper: create or get existing *entry[V]
func (sm *SyncMap[K, V]) getOrCreateEntry(key K) (*entry[V], bool) {
	e := &entry[V]{}
	actual, loaded := sm.m.LoadOrStore(key, e)
	return actual.(*entry[V]), loaded
}
