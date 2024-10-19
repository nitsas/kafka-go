package main

import (
	"bytes"
)

func NewFetchResponse(req Request) (resp Response) {
	if req.Header.APIVersion != 16 {
		resp = NewErrorResponse(req.Header.CorrelationId, ErrorCode["UNSUPPORTED_VERSION"])
		return
	}

	// Response header

	resp.Header.Version = 1
	resp.Header.CorrelationId = req.Header.CorrelationId

	// Response body

	buf := bytes.NewBuffer(nil)

	buf.Write([]byte{0, 0, 0, 0}) // throttle_time_ms
	buf.Write([]byte{0, 0})       // error code: 0
	buf.Write([]byte{0, 0, 0, 0}) // session_id

	buf.Write([]byte{1}) // responses array: empty

	buf.Write([]byte{0}) // _tagged_fields

	resp.Body.Bytes = buf.Bytes()

	return
}
