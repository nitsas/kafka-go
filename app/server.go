package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	var err error

	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	errs := make(chan error)
	defer close(errs)

	// Print errors from goroutines that handle connections
	go func() {
		for err := range errs {
			fmt.Printf("Error: %s\n", err.Error())
		}
	}()

	for {
		fmt.Println("Waiting for a connection...")
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn, errs)
	}
}
