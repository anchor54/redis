package server

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/core"
)

// MasterServer represents a Redis master server
type MasterServer struct {
	config         *config.Config
	sessionHandler *SessionHandler
}

// NewMasterServer creates a new master server
func NewMasterServer(cfg *config.Config, queue *command.Queue) *MasterServer {
	return &MasterServer{
		config:         cfg,
		sessionHandler: NewSessionHandler(queue),
	}
}

// Start starts the master server
func (ms *MasterServer) Start() error {
	localAddress := fmt.Sprintf("%s:%d", ms.config.Host, ms.config.Port)
	listener, err := net.Listen("tcp", localAddress)
	if err != nil {
		fmt.Printf("Error listening on %s: %s\n", localAddress, err)
		return err
	}
	defer listener.Close()

	fmt.Printf("Master server listening on %s\n", localAddress)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %s\n", err)
			continue
		}

		go ms.sessionHandler.Handle(core.NewRedisConnection(conn))
	}
}

// Run is a convenience method to start the server and exit on error
func (ms *MasterServer) Run() {
	if err := ms.Start(); err != nil {
		os.Exit(1)
	}
}

