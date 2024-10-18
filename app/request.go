package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type Request struct {
	Length uint32
	Header RequestHeader
	Body   RequestBody
}

type RequestHeader struct {
	Version            uint8
	APIKey             uint16
	APIVersion         uint16
	CorrelationId      uint32
	CorrelationIdBytes []byte
	// ClientId      string
	// TaggedFields  []TaggedField
}

type RequestBody struct {
	bytes []byte
}

func ReadRequest(conn net.Conn) (req Request, err error) {
	var buf []byte

	buf = make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return
	}

	req.Length = binary.BigEndian.Uint32(buf)

	if req.Length < 8 {
		err = fmt.Errorf("Expected a message of at least 8 bytes, but got a length of %d.\n", req.Length)
		return
	}

	buf = make([]byte, req.Length)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return
	}

	// Read request header:
	req.Header.Version = 0 // TODO: Add support for later request versions
	req.Header.APIKey = binary.BigEndian.Uint16(buf[0:2])
	req.Header.APIVersion = binary.BigEndian.Uint16(buf[2:4])
	req.Header.CorrelationIdBytes = buf[4:8]
	req.Header.CorrelationId = binary.BigEndian.Uint32(buf[4:8])

	req.Body.bytes = buf[9:]

	return
}
