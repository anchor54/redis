package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

var KV_store SyncMap[string, string]
var key_expiry_store SyncMap[string, *time.Timer]
var list_store SyncMap[string, *Deque[string]]

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
		KV_store.Store(key, value)

		// check if expiry is set
		if len(args) >= 4 {
			if timer, ok := key_expiry_store.Load(key); ok {
				timer.Stop()
			}

			exp_time, err := strconv.Atoi(args[3])

			if err != nil {
				return "", err
			}

			switch exp_type := args[2]; exp_type {
			case "EX":
				timer := time.AfterFunc(time.Duration(exp_time) * time.Second, func() {
					KV_store.Delete(key)
				})
				key_expiry_store.Store(key, timer)

			case "PX":
				timer := time.AfterFunc(time.Duration(exp_time) * time.Millisecond, func() {
					KV_store.Delete(key)
				})
				key_expiry_store.Store(key, timer)
			}
		}

		return ToSimpleString(OK_RESPONSE), nil
	}
}

func getHandler(args ...string) (string, error) {
	if len(args) < 1 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}
	val, ok := KV_store.Load(args[0])
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
	dq, _ := list_store.LoadOrStore(key, NewDeque[string]())
	n := dq.PushFront(value...)
	return ToRespInt(n), nil
}

func rpushHandler(args ...string) (string, error) {
	if len(args) < 2 {
		return "", errors.New(INVALID_ARGUMENTS_ERROR)
	}

	key, value := args[0], args[1:]
	dq, _ := list_store.LoadOrStore(key, NewDeque[string]())
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

	list, ok := list_store.Load(key)

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
	list, ok := list_store.Load(key)
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
	dq, ok := list_store.Load(key)
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
	timeout, _ := strconv.Atoi(args[1])
	dq, _ := list_store.LoadOrStore(key, NewDeque[string]())

	val, ok := dq.PopFront(timeout)
	if !ok {
		return ToArray(nil), nil
	}

	return ToArray([]string{key, val}), nil
}

var handlers = map[string]func (...string) (string, error) {
	"PING": pingHandler,
	"ECHO": echoHandler,
	"SET": setHandler,
	"GET": getHandler,
	"LPUSH": lpushHandler,
	"RPUSH": rpushHandler,
	"LRANGE": lrangeHandler,
	"LLEN": llenHandler,
	"LPOP": lpopHandler,
	"BLPOP": blpopHandler,
}