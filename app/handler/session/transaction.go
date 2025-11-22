package session

import (
	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/connection"
	err "github.com/codecrafters-io/redis-starter-go/app/error"
)

// MultiHandler handles the MULTI command
type MultiHandler struct{}

func (h *MultiHandler) Execute(cmd *connection.Command, conn *connection.RedisConnection) (int, []string, string, error) {
	resp, multiErr := conn.StartTransaction()
	if multiErr != nil {
		return -1, []string{}, "", multiErr
	}
	conn.SendResponse(resp)
	return -1, []string{}, "", nil // Return empty response since we sent it directly
}

// ExecHandler handles the EXEC command
type ExecHandler struct{}

func (h *ExecHandler) Execute(cmd *connection.Command, conn *connection.RedisConnection) (int, []string, string, error) {
	if !conn.IsInTransaction() {
		return -1, []string{}, "", err.ErrExecWithoutMulti
	}

	conn.EndTransaction()
	transaction := command.Transaction{
		Commands: conn.QueuedCommands(),
		Response: make(chan string),
	}
	command.GetQueueInstance().EnqueueTransaction(&transaction)
	response := <-transaction.Response
	conn.SendResponse(response)
	return -1, []string{}, "", nil // Return empty response since we sent it directly
}

// DiscardHandler handles the DISCARD command
type DiscardHandler struct{}

func (h *DiscardHandler) Execute(cmd *connection.Command, conn *connection.RedisConnection) (int, []string, string, error) {
	response, discardErr := conn.DiscardTransaction()
	if discardErr != nil {
		return -1, []string{}, "", discardErr
	}
	conn.SendResponse(response)
	return -1, []string{}, "", nil // Return empty response since we sent it directly
}
