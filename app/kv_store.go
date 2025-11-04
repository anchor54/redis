package main

import (
	"fmt"
	"time"
)

type KVStore struct {
	store SyncMap[string, RedisObject]
}

func (store *KVStore) Store(key string, value *RedisObject) {
	store.store.Store(key, value)
}

func (store *KVStore) GetValue(key string) (*RedisObject, bool) {
	val, ok := store.store.Load(key)
	if !ok {
		return nil, false
	}
	fmt.Printf("Key %s has ttl %v\n", key, val.ttl)
	if val.ttl != nil && val.ttl.Before(time.Now()) {
		fmt.Printf("Key %s has expired\n", key)
		store.Delete(key)
		return nil, false
	}
	return val, true
}

func (store *KVStore) GetString(key string) (string, bool) {
	val, ok := store.GetValue(key)
	if !ok {
		return "", false
	}
	return AsString(val.Get())
}

func (store *KVStore) GetList(key string) (*Deque[string], bool) {
	val, ok := store.GetValue(key)
	if !ok {
		return nil, false
	}
	return AsList[string](val.Get())
}

func (store *KVStore) StoreList(key string, value *Deque[string]) {
	kv_obj := NewListObject(value)
	store.store.Store(key, &kv_obj)
}

func (store *KVStore) LoadOrStoreList(key string, value *Deque[string]) (*Deque[string], bool) {
	kv_obj := NewListObject(value)
	val, loaded := store.store.LoadOrStore(key, &kv_obj)
	list, ok := AsList[string](val.Get())
	
	if !ok {
		return nil, false
	}

	return list, loaded
}

func (store *KVStore) Delete(key string) {
	store.store.Delete(key)
}

// implement a function to schedule the deletion of the key
