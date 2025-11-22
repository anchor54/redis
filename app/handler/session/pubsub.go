package session

import (
	"github.com/codecrafters-io/redis-starter-go/app/connection"
	err "github.com/codecrafters-io/redis-starter-go/app/error"
	"github.com/codecrafters-io/redis-starter-go/app/pubsub"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// SubscribeHandler handles the SUBSCRIBE command
type SubscribeHandler struct{}

func (h *SubscribeHandler) Execute(cmd *connection.Command, conn *connection.RedisConnection) (int, []string, string, error) {
	if len(cmd.Args) < 1 {
		return -1, []string{}, "", err.ErrInvalidArguments
	}
	channel := cmd.Args[0]
	pubsubManager := pubsub.GetManager()
	channels := pubsubManager.Subscribe(conn, channel)
	conn.SendResponse(
		utils.ToSimpleRespArray([]string{
			utils.ToBulkString("subscribe"),
			utils.ToBulkString(channel),
			utils.ToRespInt(channels),
		}),
	)
	return -1, []string{}, "", nil // Return empty response since we sent it directly
}

// UnsubscribeHandler handles the UNSUBSCRIBE command
type UnsubscribeHandler struct{}

func (h *UnsubscribeHandler) Execute(cmd *connection.Command, conn *connection.RedisConnection) (int, []string, string, error) {
	if len(cmd.Args) < 1 {
		return -1, []string{}, "", err.ErrInvalidArguments
	}
	channel := cmd.Args[0]
	pubsubManager := pubsub.GetManager()
	channels := pubsubManager.Unsubscribe(conn, channel)
	conn.SendResponse(
		utils.ToSimpleRespArray([]string{
			utils.ToBulkString("unsubscribe"),
			utils.ToBulkString(channel),
			utils.ToRespInt(channels),
		}),
	)
	return -1, []string{}, "", nil // Return empty response since we sent it directly
}

