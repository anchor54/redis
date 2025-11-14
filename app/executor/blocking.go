package executor

import (
	"fmt"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// WaitingCommand represents a command that is waiting for a key to be available
type WaitingCommand struct {
	Command     *core.Command
	WaitingKeys []string
}

// BlockingCommandManager manages commands that are blocked waiting for keys
type BlockingCommandManager struct {
	blockingCommands map[string][]*WaitingCommand
}

// NewBlockingCommandManager creates a new blocking command manager
func NewBlockingCommandManager() *BlockingCommandManager {
	return &BlockingCommandManager{
		blockingCommands: make(map[string][]*WaitingCommand),
	}
}

// AddWaitingCommand adds a command to the blocking commands map
func (bcm *BlockingCommandManager) AddWaitingCommand(timeout int, keys []string, cmd *core.Command) {
	waitingCommand := &WaitingCommand{Command: cmd, WaitingKeys: keys}
	for _, key := range keys {
		bcm.blockingCommands[key] = append(bcm.blockingCommands[key], waitingCommand)
	}

	// Start a timer to remove the command after timeout expires
	if timeout > 0 {
		go func(wc *WaitingCommand) {
			time.Sleep(time.Duration(timeout) * time.Millisecond)
			for _, key := range wc.WaitingKeys {
				bcm.blockingCommands[key] = bcm.removeWaitingCommand(bcm.blockingCommands[key], wc)
			}
			wc.Command.Response <- utils.ToArray(nil)
		}(waitingCommand)
	}
}

// UnblockCommandsWaitingForKey unblocks commands waiting for the given keys
func (bcm *BlockingCommandManager) UnblockCommandsWaitingForKey(keys []string) {
	for _, key := range keys {
		blockedCommands := bcm.blockingCommands[key]
		fmt.Printf("unblocking commands for key: %s, blocked commands: %v\n", key, blockedCommands)
		lastExecutedCommand := -1
		for i, blockedCmd := range blockedCommands {
			command := blockedCmd.Command
			fmt.Printf("unblocking command: %s, args: %v\n", command.Command, command.Args)
			handler, ok := core.Handlers[command.Command]
			if !ok {
				command.Response <- utils.ToError(fmt.Sprintf("unknown command: %s", command.Command))
				continue
			}
			// Here we assume that the command won't unblock another command (causing a chain reaction)
			// and so we ignore the keys that were set in its execution
			timeout, _, resp, err := handler(command)
			fmt.Printf("Blocked command => command: %s, timeout: %d, response: %s, error: %v\n", command.Command, timeout, resp, err)
			if timeout >= 0 {
				break
			}

			// remove the waiting command from the blocking commands map
			waitingKeys := blockedCmd.WaitingKeys
			for _, waitingKey := range waitingKeys {
				bcm.blockingCommands[waitingKey] = bcm.removeWaitingCommand(bcm.blockingCommands[waitingKey], blockedCmd)
			}

			lastExecutedCommand = i
			if err != nil {
				command.Response <- utils.ToError(err.Error())
				continue
			}
			command.Response <- resp
		}
		bcm.blockingCommands[key] = blockedCommands[lastExecutedCommand+1:]
	}
}

// removeWaitingCommand removes a waiting command from the list
func (bcm *BlockingCommandManager) removeWaitingCommand(waitingCommands []*WaitingCommand, blockedCommand *WaitingCommand) []*WaitingCommand {
	for i, waitingCommand := range waitingCommands {
		if waitingCommand.Command == blockedCommand.Command {
			return append(waitingCommands[:i], waitingCommands[i+1:]...)
		}
	}
	return waitingCommands
}

