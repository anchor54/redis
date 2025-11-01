package main

import (
	"fmt"
	"os"
	"sync"
)

var KV_store sync.Map

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
			conn.sendResponse(ToBulkString(val.(string)))
		}
	}
}

var handlers = map[string]func (*RedisConnection, ...string) {
	"PING": pingHandler,
	"ECHO": echoHandler,
	"SET": setHandler,
	"GET": getHandler,
}