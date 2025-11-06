package core

import (
	"time"

	datastructure "github.com/codecrafters-io/redis-starter-go/app/data-structure"
)

type KVStore struct {
	store datastructure.SyncMap[string, RedisObject]
}

func (store *KVStore) Delete(key string) {
	store.store.Delete(key)
}

func (store *KVStore) Store(key string, value *RedisObject) {
	store.store.Store(key, value)
}

func (store *KVStore) GetValue(key string) (*RedisObject, bool) {
	val, ok := store.store.Load(key)
	if !ok {
		return nil, false
	}
	if val.ttl != nil && val.ttl.Before(time.Now()) {
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
	return datastructure.AsString(val.Get())
}

func (store *KVStore) GetList(key string) (*datastructure.Deque[string], bool) {
	val, ok := store.GetValue(key)
	if !ok {
		return nil, false
	}
	return datastructure.AsList[string](val.Get())
}

func (store *KVStore) StoreList(key string, value *datastructure.Deque[string]) {
	kvObj := NewListObject(value)
	store.store.Store(key, &kvObj)
}

func (store *KVStore) LoadOrStoreList(key string) (*datastructure.Deque[string], bool) {
	kvObj := NewListObject(datastructure.NewDeque[string]())
	val, loaded := store.store.LoadOrStore(key, &kvObj)
	list, ok := datastructure.AsList[string](val.Get())
	if !ok {
		return nil, false
	}
	return list, loaded
}

func (store *KVStore) LoadOrStoreStream(key string) (*datastructure.Stream, bool) {
	kvObj := RedisObject{value: datastructure.NewStream(), ttl: nil}
	val, loaded := store.store.LoadOrStore(key, &kvObj)
	stream, ok := datastructure.AsStream(val.Get())
	if !ok {
		return nil, false
	}
	return stream, loaded
}

func (store *KVStore) GetStream(key string) (*datastructure.Stream, bool) {
	val, ok := store.GetValue(key)
	if !ok {
		return nil, false
	}
	return datastructure.AsStream(val.Get())
}
