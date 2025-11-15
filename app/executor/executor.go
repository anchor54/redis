package executor

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/replication"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// writeCommands is a set of commands that modify the database
var writeCommands = map[string]bool{
	"SET":   true,
	"LPUSH": true,
	"RPUSH": true,
	"LPOP":  true,
	"XADD":  true,
	"INCR":  true,
}

// Executor handles command and transaction execution
type Executor struct {
	queue           *command.Queue
	blockingMgr     *BlockingCommandManager
	transactionExec *TransactionExecutor
}

// NewExecutor creates a new executor
func NewExecutor(queue *command.Queue) *Executor {
	blockingMgr := NewBlockingCommandManager()
	return &Executor{
		queue:           queue,
		blockingMgr:     blockingMgr,
		transactionExec: NewTransactionExecutor(blockingMgr),
	}
}

// Start begins the executor's event loop
func (e *Executor) Start() {
	for {
		select {
		case cmd := <-e.queue.CommandQueue():
			e.handleCommand(cmd)
		case trans := <-e.queue.TransactionQueue():
			e.transactionExec.Execute(trans)
		}
	}
}

// isWriteCommand checks if a command modifies the database
func isWriteCommand(cmdName string) bool {
	return writeCommands[cmdName]
}

// handleCommand processes a single command
func (e *Executor) handleCommand(cmd *core.Command) {
	handler, ok := core.Handlers[cmd.Command]
	if !ok {
		cmd.Response <- utils.ToError(fmt.Sprintf("unknown command: %s", cmd.Command))
		return
	}

	timeout, keys, resp, err := handler(cmd)
	fmt.Printf("command: %s, timeout: %d, setKeys: %v, response: %s, error: %v\n", cmd.Command, timeout, keys, resp, err)

	// if the command is blocking, add it to the blocking commands map
	if timeout >= 0 {
		e.blockingMgr.AddWaitingCommand(timeout, keys, cmd)
		return
	}

	// otherwise, if the command had set a list of keys, we need to check if any of them blocked another command in waiting state
	e.blockingMgr.UnblockCommandsWaitingForKey(keys)

	// if the command had an error, send it to the client
	if err != nil {
		cmd.Response <- utils.ToError(err.Error())
		return
	}

	// If this is a write command and it succeeded, propagate to replicas
	if isWriteCommand(cmd.Command) {
		replication.GetManager().PropagateCommand(cmd.Command, cmd.Args)
	}
	cmd.Response <- resp
}
