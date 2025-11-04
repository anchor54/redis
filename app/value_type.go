package main

import "time"

type RType int

type RedisValue interface {
	string | Deque[string]
}

const (
	RString RType = iota
	RList
	RUnknown
)

type RValue interface {
	kind() RType
}


type RedisObject struct {
	value RValue
	ttl   *time.Time
}

func (o *RedisObject) SetExpiry(expiry time.Duration) {
		t := time.Now().Add(expiry)
		o.ttl = &t
}

func NewStringObject(value string) RedisObject {
	return RedisObject{value: NewStringValue(value), ttl: nil}
}

func NewListObject(value *Deque[string]) RedisObject {
	return RedisObject{value: value, ttl: nil}
}

func (o *RedisObject) Get() RValue {
	return o.value
}
