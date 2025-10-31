package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	host := "0.0.0.0"
	port := 6379
	local_address := fmt.Sprintf("%s:%d", host, port)
	
	// preparing a listener at port 6379 on localhost
	l, err := net.Listen("tcp", local_address)
	if err != nil {
		fmt.Printf("An Error occured when Listening to localhost at port 6379: %s", err.Error())
		// exit on error
		os.Exit(1)
	}
	
	// close the listener before function ends
	defer l.Close()

	// accept connection
	_, err = l.Accept()
	if err != nil {
		fmt.Printf("An Error occured when accepting connection: %s", err.Error())
		// exit on error
		os.Exit(1)
	}
}
