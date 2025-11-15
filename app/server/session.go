package server

import (
	"errors"
	"fmt"
	"io"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/config"
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
	sh.HandleWithInitialData(conn, []byte{})
}

// HandleWithInitialData handles a client connection session with initial data
func (sh *SessionHandler) HandleWithInitialData(conn *core.RedisConnection, initialData []byte) {
	defer conn.Close()

	buf := make([]byte, 4096)
	buffer := string(initialData) // Per-connection buffer to accumulate incomplete data


	if len(buffer) > 0 {
		buffer = sh.handleRequests(conn, buffer)
	}

	for {
		n, readErr := conn.Read(buf)
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				fmt.Printf("Error reading data: %s\n", readErr)
			}
			continue
		}
		if n == 0 {
			continue
		}

		// Append new data to buffer
		buffer += string(buf[:n])
		
		// Parse and handle all complete commands in the buffer
		buffer = sh.handleRequests(conn, buffer)
	}
}

// handleRequests processes multiple requests from the accumulated buffer
// Returns the remaining unparsed data
func (sh *SessionHandler) handleRequests(conn *core.RedisConnection, buffer string) string {
	// Try to parse multiple commands from the buffer
	commands, remaining, err := command.ParseMultipleRequests(buffer)
	if err != nil {
		fmt.Printf("Error parsing commands: %s\n", err)
		// On error, try to parse as single command (for backward compatibility)
		sh.handleRequest(conn, buffer)
		return ""
	}

	// Process each parsed command
	for _, cmd := range commands {
		fmt.Printf("role: %s, command: %s, args: %v\n", config.GetInstance().Role, cmd.Name, cmd.Args)
		sh.router.Route(conn, cmd.Name, cmd.Args)

		cmdArray := []string{cmd.Name}
		cmdArray = append(cmdArray, cmd.Args...)
		respCommand := utils.ToArray(cmdArray)

		if conn.IsMaster() {
			fmt.Printf("Received %d bytes from master for command: %s, args: %v\n", len(respCommand), cmd.Name, cmd.Args)
			config.GetInstance().Offset += len(respCommand)
			fmt.Printf("Current Offset: %d\n", config.GetInstance().Offset)
		}
	}

	// Return remaining unparsed data
	return remaining
}

// handleRequest processes a single request (fallback for old behavior)
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
