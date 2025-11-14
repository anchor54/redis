package server

import (
	"errors"
	"fmt"
	"io"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// SessionHandler manages a client connection session
type SessionHandler struct {
	router *CommandRouter
}

// NewSessionHandler creates a new session handler
func NewSessionHandler(queue *command.Queue) *SessionHandler {
	return &SessionHandler{
		router: NewCommandRouter(queue),
	}
}

// Handle handles a client connection session
func (sh *SessionHandler) Handle(conn *core.RedisConnection) {
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		n, readErr := conn.Read(buf)
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				fmt.Printf("Error reading data: %s\n", readErr)
			}
			return
		}
		if n == 0 {
			return
		}
		sh.handleRequest(conn, string(buf[:n]))
	}
}

// handleRequest processes a single request from the client
func (sh *SessionHandler) handleRequest(conn *core.RedisConnection, data string) {
	cmdName, args, parseErr := command.ParseRequest(data)
	if parseErr != nil {
		fmt.Printf("Error parsing command: %s\n", parseErr)
		conn.SendResponse(utils.ToError(parseErr.Error()))
		return
	}

	fmt.Printf("command: %s, args: %v\n", cmdName, args)

	// Route the command to the appropriate handler
	sh.router.Route(conn, cmdName, args)
}
