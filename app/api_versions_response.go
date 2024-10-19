package main

import (
	"bytes"
	"encoding/binary"
)

type APIVersions struct{ Min, Max uint16 }

var AllAPIVersions = map[string]APIVersions{
	"Fetch":       {16, 16},
	"ApiVersions": {3, 4},
}

func NewAPIVersionsResponse(request Request) (resp Response) {
	v := request.Header.APIVersion

	if v < AllAPIVersions["ApiVersions"].Min || v > AllAPIVersions["ApiVersions"].Max {
		resp = NewErrorResponse(request.Header.CorrelationId, ErrorCode["UNSUPPORTED_VERSION"])
		return
	}

	// Response header

	resp.Header.Version = 0
	resp.Header.CorrelationId = request.Header.CorrelationId

	// Response body

	buf := bytes.NewBuffer(nil)

	buf.Write([]byte{0, 0}) // error code

	numAPIs := uint8(len(AllAPIVersions))
	buf.WriteByte(byte(numAPIs))

	for APIString, minmax := range AllAPIVersions {
		nums := []uint16{APIKeyVal[APIString], minmax.Min, minmax.Max}

		numBytes := make([]byte, 2)
		for num := range nums {
			binary.BigEndian.PutUint16(numBytes, uint16(num))
			buf.Write(numBytes)
		}
		buf.WriteByte(byte(0)) // _tagged_fields
	}

	buf.Write([]byte{0, 0, 0, 0}) // throttle time
	buf.WriteByte(byte(0))        // _tagged_fields

	resp.Body.Bytes = buf.Bytes()

	return
}
