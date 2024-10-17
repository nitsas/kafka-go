package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

func handleConnection(conn net.Conn, errs chan<- error) {
	defer conn.Close()

	fmt.Println("Handling connection!")

	var err error
	var buf []byte

	for {
		fmt.Println("Waiting for a request...")

		buf = make([]byte, 4)
		_, err = io.ReadFull(conn, buf)
		if err != nil {
			errs <- err
			return
		}

		length := binary.BigEndian.Uint32(buf)

		if length < 8 {
			err = fmt.Errorf("Expected a message of at least 8 bytes, but got a length of %d.\n", length)
			errs <- err
			return
		}

		buf = make([]byte, length)
		_, err = io.ReadFull(conn, buf)
		if err != nil {
			errs <- err
			return
		}

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
				if err != nil {
					errs <- err
					return
				}

				fmt.Printf("Wrote response with size %d, correlation_id %#v, error code 35.\n", respMsgSize, correlationIdBytes)

				continue
			} else {
				fmt.Printf("Got request ApiVersions with API version %d!\n", requestAPIVersion)

				respMsg := bytes.NewBuffer(nil)
				respMsg.Write(correlationIdBytes)
				respMsg.Write([]byte{0, 0}) // error code: 0
				respMsg.Write([]byte{3})    // length of array (+1) of APIs whose versions we'll publish

				// supported versions for request Fetch
				respMsg.Write(APIKeyBytes["Fetch"])
				respMsg.Write([]byte{0, 16}) // min version
				respMsg.Write([]byte{0, 16}) // max version
				respMsg.Write([]byte{0})     // _tagged_fields

				// supported versions for request ApiVersions
				respMsg.Write(APIKeyBytes["ApiVersions"])
				respMsg.Write([]byte{0, 3}) // min version
				respMsg.Write([]byte{0, 4}) // max version
				respMsg.Write([]byte{0})    // _tagged_fields

				// trailing response fields
				respMsg.Write([]byte{0, 0, 0, 0}) // throttle time
				respMsg.Write([]byte{0})          // _tagged_fields

				respMsgSize := respMsg.Len()
				// fmt.Printf("respMsgSize: %d\n", respMsgSize)
				binary.BigEndian.PutUint32(respMsgSizeBytes, uint32(respMsgSize))

				response := make([]byte, 0, len(respMsgSizeBytes)+respMsgSize)
				response = append(response, respMsgSizeBytes...)
				response = append(response, respMsg.Bytes()...)

				_, err = conn.Write(response)
				if err != nil {
					errs <- err
					return
				}

				fmt.Printf("Wrote response %#v\n", response)
			}
		default:
			err = fmt.Errorf("We only know how to respond to APIVersions requests (API key: %d)\n", APIKeyVal["ApiVersions"])
			if err != nil {
				errs <- err
				return
			}
		}
	}
}
