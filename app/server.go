package main

import (
	"encoding/binary"
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

	// requestLength := buf[0:4]
	requestAPIKey := int16(binary.BigEndian.Uint16(buf[4:6]))
	requestAPIVersion := int16(binary.BigEndian.Uint16(buf[6:8]))
	correlationId := buf[8:12]

	fmt.Printf("Got a request with API key %d and API version %d\n", requestAPIKey, requestAPIVersion)

	if requestAPIKey != 18 {
		panic(fmt.Errorf("We only know how to respond to APIVersions requests (API key: 18)\n"))
	}

	if requestAPIVersion < 0 || requestAPIVersion > 4 {
		fmt.Printf("Unrecognized API version %d - expected 0, 1, 2, 3, or 4.\n", requestAPIVersion)
		conn.Write([]byte{0, 0, 0, 6})
		conn.Write(correlationId)

		responseCodeUnsupportedVersion := make([]byte, 2)
		binary.BigEndian.PutUint16(responseCodeUnsupportedVersion, uint16(35))
		conn.Write(responseCodeUnsupportedVersion)

		fmt.Printf("Wrote response with length 6, correlation_id %#v, error code 35.\n", correlationId)
	} else {
		fmt.Printf("Got a supported API version. Responding with the correlation id.\n")
		conn.Write([]byte{0, 0, 0, 4})
		conn.Write(correlationId)
	}

	time.Sleep(5 * time.Second)
}
