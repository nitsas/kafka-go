package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

func handleConnection(conn net.Conn, errs chan<- error) {
	defer conn.Close()

	fmt.Println("Handling connection!")

	for {
		fmt.Println("Waiting for a request...")

		request, err := ReadRequest(conn)
		if err != nil {
			errs <- err
			return
		}

		fmt.Printf("Got request with API key %d, API version %d, correlationId %d\n",
			request.Header.APIKey, request.Header.APIVersion, request.Header.CorrelationId)

		respMsgSizeBytes := make([]byte, 4)

		switch request.Header.APIKey {
		case APIKeyVal["ApiVersions"]:
			if request.Header.APIVersion < 0 || request.Header.APIVersion > 4 {
				fmt.Printf("Unrecognized API version %d - expected 0, 1, 2, 3, or 4.\n", request.Header.APIVersion)

				response := Response{
					Header: ResponseHeader{
						Version:       0,
						CorrelationId: request.Header.CorrelationId,
					},
					Body: ResponseBody{
						ErrorCode: 35,
					},
				}

				err := response.WriteTo(conn)
				if err != nil {
					errs <- err
					return
				}

				fmt.Printf("Wrote response with correlation_id %d, error code %d.\n",
					response.Header.CorrelationId, response.Body.ErrorCode)

				continue
			}

			fmt.Printf("Got request ApiVersions with API version %d!\n", request.Header.APIVersion)

			respMsg := bytes.NewBuffer(nil)
			respMsg.Write(request.Header.CorrelationIdBytes)
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
		case APIKeyVal["Fetch"]:
			if request.Header.APIVersion != 16 {
				fmt.Printf("Unexpected API version %d - expected version 16.\n", request.Header.APIVersion)

				respMsg := bytes.NewBuffer(nil)
				respMsg.Write(request.Header.CorrelationIdBytes)
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

				fmt.Printf("Wrote response with size %d, correlation_id %d, error code 35.\n",
					respMsgSize, request.Header.CorrelationIdBytes)

				continue
			}

			fmt.Printf("Got request Fetch with API version %d!\n", request.Header.APIVersion)

			respMsg := bytes.NewBuffer(nil)
			respMsg.Write(request.Header.CorrelationIdBytes)
			respMsg.Write([]byte{0}) // _tagged_fields

			respMsg.Write([]byte{0, 0, 0, 0}) // throttle_time_ms
			respMsg.Write([]byte{0, 0})       // error code: 0
			respMsg.Write([]byte{0, 0, 0, 0}) // session_id

			// responses array:
			respMsg.Write([]byte{1}) // empty array

			respMsg.Write([]byte{0}) // _tagged_fields

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
		default:
			err = fmt.Errorf("We only know how to respond to APIVersions requests (API key: %d)\n", APIKeyVal["ApiVersions"])
			if err != nil {
				errs <- err
				return
			}
		}
	}
}
