package server

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
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

	// 1) Kick off master handshake in a separate goroutine
    go func() {
        masterConn, err := rs.connectToMaster()
        if err != nil {
            fmt.Printf("Error connecting to master: %s\n", err)
            return
        }
        defer masterConn.Close()

        // This will block on the master link, but only in this goroutine
        rs.sessionHandler.Handle(masterConn)
    }()

    // 2) Use the listener loop to keep the server process alive
    rs.handleConnection(&listener)
	return nil
}

func (rs *ReplicaServer) handleConnection(listener *net.Listener) {
	defer (*listener).Close()

	for {
		conn, err := (*listener).Accept()
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
func (rs *ReplicaServer) connectToMaster() (*core.RedisConnection, error) {
	masterAddress := net.JoinHostPort(rs.config.MasterHost, rs.config.MasterPort)
	conn, err := net.DialTimeout("tcp", masterAddress, 10*time.Second)
	if err != nil {
		fmt.Printf("Error connecting to master at %s: %s\n", masterAddress, err)
		return nil, err
	}

	// Send PING
	ping := utils.ToArray([]string{"PING"})
	conn.Write([]byte(ping))

	// Handle master communication and replication
	masterConn := core.NewRedisConnection(conn)
	response, err := rs.getMasterResponse(masterConn)
	if err != nil {
		fmt.Printf("Error getting response from master: %s\n", err)
		return nil, err
	}

	if response != "PONG" {
		fmt.Printf("Unexpected response from master: %s\n", response)
		return nil, errors.New("unexpected response from master")
	}

	fmt.Printf("Connected to master\n")

	// Send REPLCONF commands
	if err := rs.sendReplconfCommands(masterConn); err != nil {
		fmt.Printf("Error during REPLCONF: %s\n", err)
		return nil, err
	}

	// Send PSYNC
	if err := rs.sendPsync(masterConn); err != nil {
		fmt.Printf("Error sending PSYNC: %s\n", err)
		return nil, err
	}

	fmt.Printf("Handshake with master complete\n")
	masterConn.SetSuppressResponse(false)
	return masterConn, nil
}

// sendReplconfCommands sends REPLCONF commands to the master
func (rs *ReplicaServer) sendReplconfCommands(masterConn *core.RedisConnection) error {
	// Send REPLCONF listening-port
	replconfListeningPort := utils.ToArray([]string{"REPLCONF", "listening-port", strconv.Itoa(rs.config.Port)})
	masterConn.SendResponse(replconfListeningPort)

	// Send REPLCONF capa
	replconfCapa := utils.ToArray([]string{"REPLCONF", "capa", "psync2"})
	masterConn.SendResponse(replconfCapa)

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

func (rs *ReplicaServer) sendPsync(conn *core.RedisConnection) error {
	// Use replication ID and offset from config
	psync := utils.ToArray([]string{"PSYNC", rs.config.ReplicationID, strconv.Itoa(rs.config.Offset)})
	conn.SendResponse(psync)

	// Read response - may contain FULLRESYNC and start of RDB in one buffer
	buff := make([]byte, 4096)
	n, err := conn.Read(buff)
	if err != nil {
		return fmt.Errorf("error reading PSYNC response: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("no data from master")
	}

	data := buff[:n]

	// Parse the FULLRESYNC simple string (format: +FULLRESYNC <replid> <offset>\r\n)
	crlfIdx := bytes.Index(data, []byte("\r\n"))
	if crlfIdx < 0 {
		return fmt.Errorf("no CRLF found in PSYNC response")
	}

	simpleString := string(data[:crlfIdx+2])
	response, err := utils.FromSimpleString(simpleString)
	if err != nil {
		return fmt.Errorf("error parsing FULLRESYNC: %w", err)
	}

	if !strings.Contains(response, "FULLRESYNC") {
		return fmt.Errorf("unexpected PSYNC response: %s", response)
	}

	parts := strings.Split(response, " ")
	if len(parts) < 3 {
		return fmt.Errorf("unexpected PSYNC response: %s", response)
	}
	replicationID := parts[1]
	offset, err := strconv.Atoi(parts[2])
	if err != nil {
		return fmt.Errorf("error parsing PSYNC offset: %w", err)
	}

	config := config.GetInstance()
	config.ReplicationID = replicationID
	config.Offset = offset

	// The rest of the buffer contains the RDB bulk string (format: $<len>\r\n<binary-data>)
	remainingData := data[crlfIdx+2:]

	// Parse bulk string header to get RDB length
	if len(remainingData) == 0 {
		// RDB data might come in next read
		remainingData = make([]byte, 4096)
		n, err = conn.Read(remainingData)
		if err != nil {
			return fmt.Errorf("error reading RDB file: %w", err)
		}
		remainingData = remainingData[:n]
	}

	// Extract the RDB file from bulk string format
	_, err = extractBinary(remainingData)
	if err != nil {
		return fmt.Errorf("error extracting RDB binary: %w", err)
	}

	fmt.Printf("Successfully received RDB file from master\n")
	return nil
}

// input: "<len>\r\n<binary-content>"
func extractBinary(data []byte) ([]byte, error) {
	// Find the separator between length and content
	sep := []byte("\r\n")
	i := bytes.Index(data, sep)
	if i < 0 {
		return nil, fmt.Errorf("no CRLF separator found")
	}

	if data[0] != '$' {
		return nil, fmt.Errorf("expected '$', got %q", data[0])
	}

	// Parse length
	lenStr := string(data[1:i])
	n, err := strconv.Atoi(lenStr)
	if err != nil {
		return nil, fmt.Errorf("invalid length %q: %w", lenStr, err)
	}

	// Extract content
	content := data[i+len(sep):]
	if len(content) < n {
		return nil, fmt.Errorf("data shorter than declared length: have %d, need %d", len(content), n)
	}

	return content[:n], nil
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

	fmt.Printf("Raw response from master: %v\n", string(buff[:n]))
	response, err := utils.FromSimpleString(string(buff[:n]))
	if err != nil {
		return "", err
	}
	fmt.Printf("Response from master: %s\n", response)

	return response, nil
}
