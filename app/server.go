package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	fmt.Println("Writing hardcoded response with length 4 and correlation_id 7.")
	conn.Write([]byte{0, 0, 0, 4, 0, 0, 0, 7})

	time.Sleep(5 * time.Second)
}
