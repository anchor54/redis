package server

import (
	"errors"
	"fmt"
	"io"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/core"
	err "github.com/codecrafters-io/redis-starter-go/app/error"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// SessionHandler manages a client connection session
type SessionHandler struct {
	queue *command.Queue
}

// NewSessionHandler creates a new session handler
func NewSessionHandler(queue *command.Queue) *SessionHandler {
	return &SessionHandler{
		queue: queue,
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

	// If in transaction and not EXEC/DISCARD, queue the command
	if conn.IsInTransaction() && cmdName != "EXEC" && cmdName != "DISCARD" {
		queued, queueErr := conn.EnqueueCommand(command.CreateCommand(cmdName, args))
		if queueErr != nil {
			conn.SendResponse(utils.ToError(queueErr.Error()))
			return
		}
		if queued {
			conn.SendResponse(utils.ToSimpleString("QUEUED"))
		}
		return
	}

	// Handle MULTI command
	if cmdName == "MULTI" {
		resp, multiErr := conn.StartTransaction()
		if multiErr != nil {
			conn.SendResponse(utils.ToError(multiErr.Error()))
			return
		}
		conn.SendResponse(resp)
		return
	}

	// Handle DISCARD command
	if cmdName == "DISCARD" {
		response, discardErr := conn.DiscardTransaction()
		if discardErr != nil {
			conn.SendResponse(utils.ToError(discardErr.Error()))
			return
		}
		conn.SendResponse(response)
		return
	}

	// Handle EXEC command
	if cmdName == "EXEC" {
		if !conn.IsInTransaction() {
			conn.SendResponse(utils.ToError(err.ErrExecWithoutMulti.Error()))
			return
		}
		conn.EndTransaction()
		transaction := command.Transaction{
			Commands: conn.QueuedCommands(),
			Response: make(chan string),
		}
		sh.queue.EnqueueTransaction(&transaction)
		response := <-transaction.Response
		conn.SendResponse(response)
		return
	}

	// Handle other commands
	commandObj := command.CreateCommand(cmdName, args)
	
	// If in transaction, enqueue command
	if conn.IsInTransaction() {
		queued, queueErr := conn.EnqueueCommand(commandObj)
		if queueErr != nil {
			conn.SendResponse(utils.ToError(queueErr.Error()))
			return
		}
		if queued {
			conn.SendResponse(utils.ToSimpleString("QUEUED"))
		}
		return
	}

	// Execute command immediately
	cmdPtr := &commandObj
	sh.queue.EnqueueCommand(cmdPtr)
	response := <-commandObj.Response
	conn.SendResponse(response)
}

