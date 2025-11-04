package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

var db = KVStore{store: SyncMap[string, RedisObject]{}}

var INVALID_ARGUMENTS_ERROR = "Invalid argumetns"
var OK_RESPONSE = "OK"
var SOMETHING_WENT_WRONG = "Something went wrong"

func (conn *RedisConnection) sendResponse(message string) {
	fmt.Printf("Sending %q\n", message)
	n, err := conn.Write([]byte(message))
	if err != nil {
		fmt.Printf("An error occurred when writing data back: %s", err.Error())
		os.Exit(1)
	}
	fmt.Printf("wrote %d bytes\n", n)
}

func pingHandler(args ...string) (string, error) {
	return ToSimpleString("PONG"), nil
}

func echoHandler(args ...string) (string, error) {
	if len(args) == 0 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	} else {
		return ToSimpleString(args[0]), nil
	}
}

func setHandler(args ...string) (string, error) {
	if len(args) < 2 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	} else {
		key, value := args[0], args[1]
		kv_obj := NewStringObject(value)
		db.Store(key, &kv_obj)

		// check if expiry is set
		if len(args) >= 4 {
			exp_time, err := strconv.Atoi(args[3])

			if err != nil {
				return "", err
			}

			switch exp_type := args[2]; exp_type {
			case "EX":
				kv_obj.SetExpiry(time.Duration(exp_time) * time.Second)
			case "PX":
				kv_obj.SetExpiry(time.Duration(exp_time) * time.Millisecond)
			}
		}

		return ToSimpleString(OK_RESPONSE), nil
	}
}

func getHandler(args ...string) (string, error) {
	if len(args) < 1 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}
	val, ok := db.GetString(args[0])
	if !ok {
		return ToNullBulkString(), nil
	}
	return ToBulkString(val), nil
}

func lpushHandler(args ...string) (string, error) {
	if len(args) < 2 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key, value := args[0], args[1:]
	reverse(value)
	dq, _ := db.LoadOrStoreList(key)

	if dq == nil {
		return "", errors.New("failed to load or store list")
	}

	n := dq.PushFront(value...)
	return ToRespInt(n), nil
}

func rpushHandler(args ...string) (string, error) {
	if len(args) < 2 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key, value := args[0], args[1:]
	dq, _ := db.LoadOrStoreList(key)

	if dq == nil {
		return "", errors.New("failed to load or store list")
	}

	n := dq.PushBack(value...)
	return ToRespInt(n), nil
}

func lrangeHandler(args ...string) (string, error) {
	if len(args) < 3 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}
	key := args[0]
	l, err := strconv.Atoi(args[1])

	if err != nil {
		return "", err
	}

	r, err := strconv.Atoi(args[2])

	if err != nil {
		return "", err
	}

	list, ok := db.GetList(key)

	if !ok {
		return ToArray(make([]string, 0)), nil
	}

	return ToArray(list.LRange(l, r)), nil
}

func llenHandler(args ...string) (string, error) {
	if len(args) < 1 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key := args[0]
	list, ok := db.GetList(key)
	if !ok {
		return ToRespInt(0), nil
	}

	return ToRespInt(list.Length()), nil
}

func lpopHandler(args ...string) (string, error) {
	if len(args) < 1 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key := args[0]
	dq, ok := db.GetList(key)
	if !ok {
		return ToNullBulkString(), nil
	}

	if len(args) > 1 {
		n_ele_to_rem, _ := strconv.Atoi(args[1])
		removed_elements, n_removed_elements := dq.TryPopN(n_ele_to_rem)
		if n_removed_elements == 0 {
			return ToNullBulkString(), nil
		}
		return ToArray(removed_elements), nil
	}

	removed_element, ok := dq.TryPop()
	if !ok {
		return ToNullBulkString(), nil
	}
	return ToBulkString(removed_element), nil
}

func blpopHandler(args ...string) (string, error) {
	if len(args) < 2 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key := args[0]
	timeout, _ := strconv.ParseFloat(args[1], 32)
	dq, _ := db.LoadOrStoreList(key)

	if dq == nil {
		return "", errors.New("failed to load or store list")
	}

	val, ok := dq.PopFront(timeout)
	if !ok {
		return ToArray(nil), nil
	}

	return ToArray([]string{key, val}), nil
}

func typeHandler(args ...string) (string, error) {
	if len(args) < 1 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key := args[0]

	val, ok := db.GetValue(key)
	if !ok {
		return ToBulkString("none"), nil
	}

	switch val.Get().kind() {
	case RString:
		return ToBulkString("string"), nil
	case RList:
		return ToBulkString("list"), nil
	case RStream:
		return ToBulkString("stream"), nil
	default:
		return ToBulkString("none"), nil
	}
}

func xaddHandler(args ...string) (string, error) {
	if len(args) < 4 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key, entry_id := args[0], args[1]
	fields := args[2:]

	kv_obj := CreateEntry(entry_id, fields...)
	stream, _ := db.LoadOrStoreStream(key)
	stream.AppendEntry(kv_obj)
	return ToBulkString(kv_obj.ID), nil
}

var handlers = map[string]func(...string) (string, error){
	"PING":   pingHandler,
	"ECHO":   echoHandler,
	"SET":    setHandler,
	"GET":    getHandler,
	"LPUSH":  lpushHandler,
	"RPUSH":  rpushHandler,
	"LRANGE": lrangeHandler,
	"LLEN":   llenHandler,
	"LPOP":   lpopHandler,
	"BLPOP":  blpopHandler,
	"TYPE":   typeHandler,
	"XADD":   xaddHandler,
}
