package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

// parseRESPCommand takes the raw string from the client
// and parses it as an array of bulk strings.
func ParseRESPCommand(raw string) ([]string, error) {
    // Use a strings.Reader and bufio.Reader to read line by line
    reader := bufio.NewReader(strings.NewReader(raw))

    // 1. Read the array prefix (e.g., "*1\r\n")
    line, err := reader.ReadString('\n')
    if err != nil || line[0] != '*' {
        return nil, fmt.Errorf("expected array prefix, got: %s", line)
    }

    // 2. Parse the array length
    var arrayLen int
    _, err = fmt.Sscanf(line[1:], "%d\r\n", &arrayLen)
    if err != nil {
        return nil, fmt.Errorf("failed to parse array length: %s", err)
    }

    // 3. Create a string slice to hold the commands
    commands := make([]string, 0, arrayLen)

    // 4. Loop for each item in the array
    for i := 0; i < arrayLen; i++ {
        // 5. Read the bulk string prefix (e.g., "$4\r\n")
        line, err := reader.ReadString('\n')
        if err != nil || line[0] != '$' {
            return nil, fmt.Errorf("expected bulk string prefix, got: %s", line)
        }

        // 6. Parse the string length
        var strLen int
        _, err = fmt.Sscanf(line[1:], "%d\r\n", &strLen)
        if err != nil {
            return nil, fmt.Errorf("failed to parse string length: %s", err)
        }

        // 7. Read that many bytes for the data
        buf := make([]byte, strLen)
        _, err = reader.Read(buf) // Read the data
        if err != nil {
            return nil, fmt.Errorf("failed to read string data: %s", err)
        }

        // 8. Read the trailing "\r\n"
        _, err = reader.ReadString('\n')
        if err != nil {
            return nil, fmt.Errorf("failed to read trailing CRLF: %s", err)
        }

        // 9. Add the string to our slice
        commands = append(commands, string(buf))
    }

    return commands, nil
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	HOST := "0.0.0.0"
	PORT := 6379
	LOCAL_ADDRESS := fmt.Sprintf("%s:%d", HOST, PORT)
	IN_DATA_BUF_SZ := 1024

	// preparing a listener at port 6379 on localhost
	l, err := net.Listen("tcp", LOCAL_ADDRESS)
	if err != nil {
		fmt.Printf("An Error occured when Listening to localhost at port 6379: %s", err.Error())
		// exit on error
		os.Exit(1)
	}
	
	// close the listener before exiting
	defer l.Close()

	for {
		// accept connection
		conn, err := l.Accept()
		if err != nil {
			fmt.Printf("An Error occured when accepting connection: %s\n", err.Error())
			// exit on error
			os.Exit(1)
		}
	
		// define a buffer of 1KB
		buf := make([]byte, IN_DATA_BUF_SZ)
		
		go func (conn net.Conn)  {
			// try to read data from the accepted connection
			n, err := conn.Read(buf)
		
			if err != nil {
				fmt.Printf("An error occured when reading data from request: %s\n", err.Error())
				os.Exit(1)
			}
		
			// convert received data from byte to string
			commands, err := ParseRESPCommand(string(buf[:n]))
			if err != nil {
				fmt.Printf("An error occurred when parsing the command: %s", err.Error())
				os.Exit(1)
			}

			for _, command := range commands {
				fmt.Printf("Executing command: %s\n", command)
				// if the data is PING we should write back PONG
				// if command == "PING" {
					_, err = conn.Write([]byte("+PONG\r\n"))
					if err != nil {
						fmt.Printf("An error occurred when writing data back: %s", err.Error())
						os.Exit(1)
					}
				// }
			} 
			
			conn.Close()
		
		}(conn)
	}
}
