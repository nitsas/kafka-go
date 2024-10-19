package main

import (
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
			fmt.Printf("Got request ApiVersions with API version %d!\n", request.Header.APIVersion)

			response := NewAPIVersionsResponse(request)
			err := response.WriteTo(conn)
			if err != nil {
				errs <- err
				return
			}

			fmt.Printf("Wrote response %#v\n", response)
		case APIKeyVal["Fetch"]:
			fmt.Printf("Got request Fetch with API version %d!\n", request.Header.APIVersion)

			response := NewFetchResponse(request)
			err := response.WriteTo(conn)
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
