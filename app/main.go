package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	HOST := "0.0.0.0"
	PORT := 6379
	LOCAL_ADDRESS := fmt.Sprintf("%s:%d", HOST, PORT)

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
		
		go func (conn net.Conn)  {
			// try to read data from the accepted connection
			scanner := bufio.NewScanner(conn)
		
			// convert received data from byte to string
			if err != nil {
				fmt.Printf("An error occurred when parsing the command: %s", err.Error())
				os.Exit(1)
			}

			for scanner.Scan() {
				command := strings.TrimSpace(scanner.Text())
				fmt.Printf("Executing command: %s\n", command)
				// if the data is PING we should write back PONG
				if command == "PING" {
					_, err = conn.Write([]byte("+PONG\r\n"))
					if err != nil {
						fmt.Printf("An error occurred when writing data back: %s", err.Error())
						os.Exit(1)
					}
				}
			} 
			
			conn.Close()
		
		}(conn)
	}
}
