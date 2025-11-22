package session

import (
	"github.com/codecrafters-io/redis-starter-go/app/acl"
	"github.com/codecrafters-io/redis-starter-go/app/connection"
	err "github.com/codecrafters-io/redis-starter-go/app/error"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// ACLHandler handles the ACL command
type ACLHandler struct{}

func (h *ACLHandler) Execute(cmd *connection.Command, conn *connection.RedisConnection) (int, []string, string, error) {
	if len(cmd.Args) < 1 {
		return -1, []string{}, "", err.ErrInvalidArguments
	}

	subcommand := cmd.Args[0]
	username := conn.GetUsername()
	manager := acl.GetInstance()

	switch subcommand {
	case "WHOAMI":
		// Return the username of the connection that issued this command
		conn.SendResponse(utils.ToBulkString(username))
		return -1, []string{}, "", nil

	case "GETUSER":
		// Get user info for the specified username (or current user if not specified)
		targetUsername := username
		if len(cmd.Args) > 1 {
			targetUsername = cmd.Args[1]
		}

		user, exists := manager.GetUser(targetUsername)
		if !exists {
			conn.SendError("User " + targetUsername + " not found")
			return -1, []string{}, "", nil
		}

		// Build response array with user info
		// Format: ["flags", ["on", "allkeys"], "passwords", [...], ...]
		response := []string{
			utils.ToBulkString("flags"),
			utils.ToArray(user.Flags),
		}

		if len(user.Passwords) > 0 {
			response = append(response, utils.ToBulkString("passwords"))
			response = append(response, utils.ToArray(user.Passwords))
		}

		if len(user.Keys) > 0 {
			response = append(response, utils.ToBulkString("keys"))
			response = append(response, utils.ToArray(user.Keys))
		}

		if len(user.Commands) > 0 {
			response = append(response, utils.ToBulkString("commands"))
			response = append(response, utils.ToArray(user.Commands))
		}

		if len(user.Channels) > 0 {
			response = append(response, utils.ToBulkString("channels"))
			response = append(response, utils.ToArray(user.Channels))
		}

		conn.SendResponse(utils.ToSimpleRespArray(response))
		return -1, []string{}, "", nil
	}

	conn.SendError("Unknown ACL subcommand: " + subcommand)
	return -1, []string{}, "", nil
}
