package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

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

	buf := make([]byte, 100)
	n, err := conn.Read(buf)
	panicIf(err)
	if n < 12 {
		panic(fmt.Errorf("Read too few bytes! Expected at least 12 but got %d.\n", n))
	}

	correlationId := buf[8:12]

	fmt.Printf("Writing response with length 4 and correlation_id %#v.", correlationId)
	conn.Write([]byte{0, 0, 0, 4})
	conn.Write(correlationId)

	time.Sleep(5 * time.Second)
}
