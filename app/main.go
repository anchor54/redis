package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/core"
)

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

		go handleSession(core.NewRedisConnection(conn))
	}
}

func handleSession(conn *core.RedisConnection) {
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
		conn.HandleRequest(string(buf[:n]))
	}
}
