package session

import "github.com/codecrafters-io/redis-starter-go/app/connection"

// SessionHandler is the interface that all session handlers must implement
type SessionHandler interface {
	Execute(cmd *connection.Command, conn *connection.RedisConnection) (timeout int, keys []string, response string, err error)
}

var SessionHandlers = map[string]SessionHandler{
	"MULTI":       &MultiHandler{},
	"EXEC":        &ExecHandler{},
	"DISCARD":     &DiscardHandler{},
	"REPLCONF":    &ReplconfHandler{},
	"PSYNC":       &PsyncHandler{},
	"WAIT":        &WaitHandler{},
	"SUBSCRIBE":   &SubscribeHandler{},
	"UNSUBSCRIBE": &UnsubscribeHandler{},
	"ACL":         &ACLHandler{},
}
