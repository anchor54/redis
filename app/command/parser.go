package command

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// ParseRequest parses a RESP-formatted request string and returns the command name and arguments
func ParseRequest(data string) (string, []string, error) {
	parsedArray, err := utils.ParseRESPArray(data)
	if err != nil {
		return "", nil, fmt.Errorf("error parsing command: %w", err)
	}

	// Flatten nested arrays to []string for command processing
	commands := utils.FlattenRESPArray(parsedArray)
	if len(commands) == 0 {
		return "", nil, fmt.Errorf("empty command")
	}

	command := strings.ToUpper(strings.TrimSpace(commands[0]))
	args := commands[1:]

	return command, args, nil
}

// CreateCommand creates a Command object from command name and arguments
func CreateCommand(command string, args []string) core.Command {
	return core.CreateCommand(command, args...)
}

