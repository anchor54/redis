package main

import (
	"fmt"
	"os"
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

func listPushHandler(conn *RedisConnection, args ...string) {
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

var handlers = map[string]func (*RedisConnection, ...string) {
	"PING": pingHandler,
	"ECHO": echoHandler,
	"SET": setHandler,
	"GET": getHandler,
	"RPUSH": listPushHandler,
}