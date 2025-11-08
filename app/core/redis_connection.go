package core

import (
	"fmt"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

type RedisConnection struct {
	Conn 			net.Conn
	InTransaction 	bool
	QueuedCommands 	[]Command
}

type Command struct {
	Command string
	Args    []string
}

func (conn *RedisConnection) EnqueueCommand(command Command) {
	conn.QueuedCommands = append(conn.QueuedCommands, command)
}

func (conn *RedisConnection) DequeueCommand() (Command, bool) {
	if len(conn.QueuedCommands) == 0 {
		return Command{}, false
	}
	return conn.QueuedCommands[0], true
}

func (conn *RedisConnection) Close() error {
	return conn.Conn.Close()
}

func (conn *RedisConnection) Read(p []byte) (int, error) {
	return conn.Conn.Read(p)
}

func (conn *RedisConnection) sendResponse(message string) {
	_, err := conn.Conn.Write([]byte(message))
	if err != nil {
		fmt.Printf("Error writing response: %s\n", err)
	}
}

func (conn *RedisConnection) HandleRequest(data string) {
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
	fmt.Printf("command: %s, args: %v\n", command, args)

	handler, ok := Handlers[command]
	if !ok {
		conn.sendResponse(utils.ToError(fmt.Sprintf("unknown command: %s", command)))
		return
	}

	resp, err := handler(args...)
	fmt.Printf("resp: %q, err: %v\n", resp, err)
	if err != nil {
		conn.sendResponse(utils.ToError(err.Error()))
		return
	}

	conn.sendResponse(resp)
}
