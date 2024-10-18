package main

import (
	"bytes"
	"encoding/binary"
	"net"
)

type Response struct {
	Header ResponseHeader
	Body   ResponseBody
}

type ResponseHeader struct {
	Version       uint8
	CorrelationId uint32
}

type ResponseBody struct {
	ErrorCode uint16
}

func (resp *Response) WriteTo(conn net.Conn) (err error) {
	buf := bytes.NewBuffer(nil)

	correlationIdBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIdBytes, resp.Header.CorrelationId)
	buf.Write(correlationIdBytes)

	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, resp.Body.ErrorCode)
	buf.Write(errorCodeBytes)

	size := buf.Len()
	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, uint32(size))

	responseBytes := make([]byte, 0, len(sizeBytes)+size)
	responseBytes = append(responseBytes, sizeBytes...)
	responseBytes = append(responseBytes, buf.Bytes()...)

	_, err = conn.Write(responseBytes)

	return
}
