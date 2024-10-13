package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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
	var err error

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
	defer conn.Close()

	var buf []byte

	buf = make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	panicIf(err)

	length := binary.BigEndian.Uint32(buf)

	if length < 8 {
		panic(fmt.Errorf("Expected a message of at least 8 bytes, but got a length of %d.\n", length))
	}

	buf = make([]byte, length)
	_, err = io.ReadFull(conn, buf)
	panicIf(err)

	requestAPIKey := binary.BigEndian.Uint16(buf[0:2])
	requestAPIVersion := binary.BigEndian.Uint16(buf[2:4])
	correlationIdBytes := buf[4:8]
	correlationId := binary.BigEndian.Uint32(correlationIdBytes)
	fmt.Printf("Got request: %#v\n", buf)
	fmt.Printf("Got API key %d, API version %d, correlationId %d\n", requestAPIKey, requestAPIVersion, correlationId)

	respMsgSizeBytes := make([]byte, 4)

	switch requestAPIKey {
	case APIKeyVal["ApiVersions"]:
		if requestAPIVersion < 0 || requestAPIVersion > 4 {
			fmt.Printf("Unrecognized API version %d - expected 0, 1, 2, 3, or 4.\n", requestAPIVersion)
			respMsg := bytes.NewBuffer(nil)
			respMsg.Write(correlationIdBytes)
			respMsg.Write([]byte{0, 35})

			respMsgSize := respMsg.Len()
			binary.BigEndian.PutUint32(respMsgSizeBytes, uint32(respMsgSize))

			response := make([]byte, 0, len(respMsgSizeBytes)+respMsgSize)
			response = append(response, respMsgSizeBytes...)
			response = append(response, respMsg.Bytes()...)

			_, err = conn.Write(response)
			panicIf(err)

			fmt.Printf("Wrote response with size %d, correlation_id %#v, error code 35.\n", respMsgSize, correlationIdBytes)
		} else {
			fmt.Printf("Got a supported API version (%d)!\n", requestAPIVersion)
			respMsg := bytes.NewBuffer(nil)
			respMsg.Write(correlationIdBytes)
			respMsg.Write([]byte{0, 0}) // error code: 0
			respMsg.Write([]byte{2})    // length of array (+1) of APIs whose versions we'll publish

			// supported versions for request ApiVersions
			respMsg.Write(APIKeyBytes["ApiVersions"])
			respMsg.Write([]byte{0, 3})       // min version
			respMsg.Write([]byte{0, 4})       // max version
			respMsg.Write([]byte{0})          // _tagged_fields
			respMsg.Write([]byte{0, 0, 0, 0}) // throttle time
			respMsg.Write([]byte{0})          // _tagged_fields

			respMsgSize := respMsg.Len()
			// fmt.Printf("respMsgSize: %d\n", respMsgSize)
			binary.BigEndian.PutUint32(respMsgSizeBytes, uint32(respMsgSize))

			response := make([]byte, 0, len(respMsgSizeBytes)+respMsgSize)
			response = append(response, respMsgSizeBytes...)
			response = append(response, respMsg.Bytes()...)

			_, err = conn.Write(response)
			panicIf(err)

			fmt.Printf("Wrote response %#v\n", response)
		}
	default:
		panic(fmt.Errorf("We only know how to respond to APIVersions requests (API key: %d)\n", APIKeyVal["ApiVersions"]))
	}

	time.Sleep(3 * time.Second)
}
