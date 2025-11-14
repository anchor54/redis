package main

import (
	"flag"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/executor"
	"github.com/codecrafters-io/redis-starter-go/app/server"
)

// parseFlags parses command-line flags and returns server configuration
func parseFlags() (int, config.ServerRole, string) {
	var port int
	var masterAddress string

	flag.IntVar(&port, "port", 6379, "The port to run the server on.")
	flag.StringVar(&masterAddress, "replicaof", "", "Master address in format 'host port'")
	flag.Parse()

	role := config.Master
	if masterAddress != "" {
		role = config.Slave
	}

	return port, role, masterAddress
}

func main() {
	// Parse command-line flags
	port, role, masterAddress := parseFlags()

	// Initialize configuration
	config.Init(&config.ConfigOptions{
		Port:          port,
		Role:          role,
		MasterAddress: masterAddress,
	})
	cfg := config.GetInstance()

	// Create command queue
	queue := command.NewQueue(1000, 1000)

	// Start command executor in background
	exec := executor.NewExecutor(queue)
	go exec.Start()

	// Create and start appropriate server (master or replica)
	srv := server.NewServer(cfg, queue)
	srv.Run()
}
