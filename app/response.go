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
	Bytes []byte
}

func (resp *Response) WriteTo(conn net.Conn) (err error) {
	correlationIdBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIdBytes, resp.Header.CorrelationId)

	size := len(correlationIdBytes) + len(resp.Body.Bytes)
	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, uint32(size))

	buf := bytes.NewBuffer(nil)
	buf.Write(sizeBytes)
	buf.Write(correlationIdBytes)
	if resp.Header.Version == 1 {
		buf.WriteByte(byte(0)) // _tagged_fields
	}
	buf.Write(resp.Body.Bytes)

	_, err = conn.Write(buf.Bytes())

	return
}

func NewErrorResponse(correlationId uint32, errorCode uint16) (resp Response) {
	resp.Header.Version = 0
	resp.Header.CorrelationId = correlationId

	resp.Body.Bytes = make([]byte, 2)
	binary.BigEndian.PutUint16(resp.Body.Bytes, errorCode)

	return
}
