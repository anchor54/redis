package command

import (
	"github.com/codecrafters-io/redis-starter-go/app/core"
)

// Queue manages command and transaction queues
type Queue struct {
	cmdQueue     chan *core.Command
	transactions chan *Transaction
}

// Transaction represents a Redis transaction
type Transaction struct {
	Commands []core.Command
	Response chan string
}

// NewQueue creates a new command queue
func NewQueue(cmdQueueSize, transactionQueueSize int) *Queue {
	return &Queue{
		cmdQueue:     make(chan *core.Command, cmdQueueSize),
		transactions: make(chan *Transaction, transactionQueueSize),
	}
}

// EnqueueCommand adds a command to the command queue
func (q *Queue) EnqueueCommand(cmd *core.Command) {
	q.cmdQueue <- cmd
}

// EnqueueTransaction adds a transaction to the transaction queue
func (q *Queue) EnqueueTransaction(trans *Transaction) {
	q.transactions <- trans
}

// CommandQueue returns the command channel
func (q *Queue) CommandQueue() <-chan *core.Command {
	return q.cmdQueue
}

// TransactionQueue returns the transaction channel
func (q *Queue) TransactionQueue() <-chan *Transaction {
	return q.transactions
}

