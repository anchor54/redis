package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type RedisConnection struct {
	net.Conn
}

func main() {
	const (
		host = "0.0.0.0"
		port = 6379
	)

	localAddress := fmt.Sprintf("%s:%d", host, port)

	// Prepare a listener at port 6379
	listener, err := net.Listen("tcp", localAddress)
	if err != nil {
		fmt.Printf("Error listening on %s: %s\n", localAddress, err)
		os.Exit(1)
	}
	defer listener.Close()

	for {
		// Accept connection
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %s\n", err)
			continue // Continue accepting other connections instead of exiting
		}

		go handleSession(&RedisConnection{conn})
	}
}

func handleSession(conn *RedisConnection) {
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Printf("Error reading data: %s\n", err)
			}
			return
		}
		if n == 0 {
			return
		}
		conn.handleRequest(string(buf[:n]))
	}
}

func (conn *RedisConnection) handleRequest(data string) {
	parsedArray, err := utils.ParseRESPArray(data)
	if err != nil {
		fmt.Printf("Error parsing command: %s\n", err)
		conn.sendResponse(utils.ToError("invalid command format"))
		return
	}

	// Flatten nested arrays to []string for command processing
	commands := utils.FlattenRESPArray(parsedArray)
	if len(commands) == 0 {
		conn.sendResponse(utils.ToError("empty command"))
		return
	}

	command := strings.ToUpper(strings.TrimSpace(commands[0]))
	args := commands[1:]

	handler, ok := core.Handlers[command]
	if !ok {
		conn.sendResponse(utils.ToError(fmt.Sprintf("unknown command: %s", command)))
		return
	}

	resp, err := handler(args...)
	if err != nil {
		conn.sendResponse(utils.ToError(err.Error()))
		return
	}

	conn.sendResponse(resp)
}

func (conn *RedisConnection) sendResponse(message string) {
	_, err := conn.Write([]byte(message))
	if err != nil {
		fmt.Printf("Error writing response: %s\n", err)
	}
}
