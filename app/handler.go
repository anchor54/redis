package main

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"
)

var KV_store SyncMap[string, string]
var key_expiry_store SyncMap[string, *time.Timer]
var list_store SyncMap[string, []string]


func (conn *RedisConnection) sendResponse(message string) {
	fmt.Printf("Sending %q\n", message)
	n, err := conn.Write([]byte(message))
	if err != nil {
		fmt.Printf("An error occurred when writing data back: %s", err.Error())
		os.Exit(1)
	}
	fmt.Printf("wrote %d bytes\n", n)
}

func pingHandler(conn *RedisConnection, args ...string) {
	conn.sendResponse(ToSimpleString("PONG"))
}

func echoHandler(conn *RedisConnection, args ...string) {
	if len(args) == 0 {
		conn.sendResponse(ToNullBulkString())
	} else {
		conn.sendResponse(ToSimpleString(args[0]))
	}
}

func setHandler(conn *RedisConnection, args ...string) {
	if len(args) < 2 {
		conn.sendResponse(ToNullBulkString())
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
				conn.sendResponse(ToNullBulkString())
				return
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

		conn.sendResponse(ToSimpleString("OK"))
	}
}

func getHandler(conn *RedisConnection, args ...string) {
	if len(args) < 1 {
		conn.sendResponse(ToNullBulkString())
	} else {
		val, ok := KV_store.Load(args[0])
		if !ok {
			conn.sendResponse(ToNullBulkString())
		} else {
			conn.sendResponse(ToBulkString(val))
		}
	}
}

func rPushHandler(conn *RedisConnection, args ...string) {
	if len(args) < 2 {
		conn.sendResponse(ToNullBulkString())
	} else {
		key, value := args[0], args[1:]
		list_store.Update(key, func(old []string) []string {
			new_list := append(old, value...)
			defer conn.sendResponse(ToRespInt(len(new_list)))
			return new_list
		})
	}
}

func lrangeHandler(conn *RedisConnection, args ...string) {
	if len(args) < 3 {
		conn.sendResponse(ToNullBulkString())
	} else {
		key := args[0]
		l, err := strconv.Atoi(args[1])

		if err != nil {
			conn.sendResponse(ToNullBulkString())
		}

		r, err := strconv.Atoi(args[2])

		if err != nil {
			conn.sendResponse(ToNullBulkString())
		}

		list, ok := list_store.Load(key)
	
		if !ok {
			conn.sendResponse(ToArray(make([]string, 0)))
			return
		}

		n := len(list)
		
		if r < 0 {
			r += n
		}
		
		if l < 0 {
			l += n
		}

		r = max(0, min(r, n - 1))
		l = max(0, min(l, n - 1))
		
		if l >= n || l > r {
			conn.sendResponse(ToArray(make([]string, 0)))
			return
		}

		conn.sendResponse(ToArray(list[l : r + 1]))
	}
}

func lPushHandler(conn *RedisConnection, args ...string) {
	if len(args) < 2 {
		conn.sendResponse(ToNullBulkString())
		return
	}
	key, value := args[0], args[1:]
	list_store.Update(key, func(old []string) []string {
		new_list := make([]string, len(old))
		copy(new_list, old)
		for _, v := range value {
			new_list = slices.Insert(new_list, 0, v)
		}
		defer conn.sendResponse(ToRespInt(len(new_list)))
		return new_list
	})
}

func llenHandler(conn *RedisConnection, args ...string) {
	if len(args) < 1 {
		conn.sendResponse(ToNullBulkString())
		return
	}

	key := args[0]
	list, ok := list_store.Load(key)
	if !ok {
		list = make([]string, 0)
	}

	conn.sendResponse(ToRespInt(len(list)))
}

func lpopHandler(conn *RedisConnection, args ...string) {
	if len(args) < 1 {
		conn.sendResponse(ToNullBulkString())
		return
	}

	key := args[0]
	n_ele_to_rem := 1

	if len(args) > 1 {
		n_ele_to_rem, _ = strconv.Atoi(args[1])
	}
	
	list_store.Update(key, func(old []string) []string {
		n := len(old)

		if n == 0 {
			conn.sendResponse(ToNullBulkString())
			return make([]string, 0)
		}
		
		n_ele_to_rem = min(n_ele_to_rem, n)
		if len(args) > 1 {
			defer conn.sendResponse(ToArray(old[:n_ele_to_rem]))
		} else {
			defer conn.sendResponse(ToBulkString(old[0]))
		}
		return old[n_ele_to_rem:]
	})
}

var handlers = map[string]func (*RedisConnection, ...string) {
	"PING": pingHandler,
	"ECHO": echoHandler,
	"SET": setHandler,
	"GET": getHandler,
	"LPUSH": lPushHandler,
	"RPUSH": rPushHandler,
	"LRANGE": lrangeHandler,
	"LLEN": llenHandler,
	"LPOP": lpopHandler,
}