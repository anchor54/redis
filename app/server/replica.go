package server

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/command"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/core"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// ReplicaServer represents a Redis replica server
type ReplicaServer struct {
	config         *config.Config
	sessionHandler *SessionHandler
}

// NewReplicaServer creates a new replica server
func NewReplicaServer(cfg *config.Config, queue *command.Queue) *ReplicaServer {
	return &ReplicaServer{
		config:         cfg,
		sessionHandler: NewSessionHandler(queue),
	}
}

// Start starts the replica server
func (rs *ReplicaServer) Start() error {
	localAddress := fmt.Sprintf("%s:%d", rs.config.Host, rs.config.Port)
	listener, err := net.Listen("tcp", localAddress)
	if err != nil {
		fmt.Printf("Error listening on %s: %s\n", localAddress, err)
		return err
	}
	defer listener.Close()

	fmt.Printf("Replica server listening on %s\n", localAddress)

	// Connect to master in a goroutine
	go rs.connectToMaster()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %s\n", err)
			continue
		}

		go rs.sessionHandler.Handle(core.NewRedisConnection(conn))
	}
}

// Run is a convenience method to start the server and exit on error
func (rs *ReplicaServer) Run() {
	if err := rs.Start(); err != nil {
		os.Exit(1)
	}
}

// connectToMaster establishes a connection to the master server
func (rs *ReplicaServer) connectToMaster() {
	masterAddress := net.JoinHostPort(rs.config.MasterHost, rs.config.MasterPort)
	conn, err := net.DialTimeout("tcp", masterAddress, 10*time.Second)
	if err != nil {
		fmt.Printf("Error connecting to master at %s: %s\n", masterAddress, err)
		return
	}
	defer conn.Close()

	// Send PING
	ping := utils.ToArray([]string{"PING"})
	conn.Write([]byte(ping))

	// Handle master communication and replication
	masterConn := core.NewRedisConnection(conn)
	response, err := rs.getMasterResponse(masterConn)
	if err != nil {
		fmt.Printf("Error getting response from master: %s\n", err)
		return
	}

	if response != "PONG" {
		fmt.Printf("Unexpected response from master: %s\n", response)
		return
	}

	fmt.Printf("Connected to master\n")

	// Send REPLCONF commands
	if err := rs.sendReplconfCommands(conn, masterConn); err != nil {
		fmt.Printf("Error during REPLCONF: %s\n", err)
		return
	}

	// Send PSYNC
	if err := rs.sendPsync(conn); err != nil {
		fmt.Printf("Error sending PSYNC: %s\n", err)
		return
	}
}

// sendReplconfCommands sends REPLCONF commands to the master
func (rs *ReplicaServer) sendReplconfCommands(conn net.Conn, masterConn *core.RedisConnection) error {
	// Send REPLCONF listening-port
	replconfListeningPort := utils.ToArray([]string{"REPLCONF", "listening-port", strconv.Itoa(rs.config.Port)})
	if _, err := conn.Write([]byte(replconfListeningPort)); err != nil {
		return fmt.Errorf("error sending REPLCONF listening-port: %w", err)
	}

	// Send REPLCONF capa
	replconfCapa := utils.ToArray([]string{"REPLCONF", "capa", "psync2"})
	if _, err := conn.Write([]byte(replconfCapa)); err != nil {
		return fmt.Errorf("error sending REPLCONF capa: %w", err)
	}

	// Get response for listening-port
	response, err := rs.getMasterResponse(masterConn)
	if err != nil {
		return fmt.Errorf("error getting REPLCONF listening-port response: %w", err)
	}
	if response != "OK" {
		return fmt.Errorf("unexpected REPLCONF listening-port response: %s", response)
	}

	// Get response for capa
	response, err = rs.getMasterResponse(masterConn)
	if err != nil {
		return fmt.Errorf("error getting REPLCONF capa response: %w", err)
	}
	if response != "OK" {
		return fmt.Errorf("unexpected REPLCONF capa response: %s", response)
	}

	return nil
}

func (rs *ReplicaServer) sendPsync(conn net.Conn) error {
	// Use replication ID and offset from config
	psync := utils.ToArray([]string{"PSYNC", rs.config.ReplicationID, strconv.Itoa(rs.config.Offset)})
	if _, err := conn.Write([]byte(psync)); err != nil {
		return fmt.Errorf("error sending PSYNC: %w", err)
	}
	return nil
}

// getMasterResponse reads and parses a response from the master
func (rs *ReplicaServer) getMasterResponse(conn *core.RedisConnection) (string, error) {
	buff := make([]byte, 1024)
	n, err := conn.Read(buff)
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", errors.New("no data from master")
	}

	response, err := utils.FromSimpleString(string(buff[:n]))
	if err != nil {
		return "", err
	}
	fmt.Printf("Response from master: %s\n", response)

	return response, nil
}
