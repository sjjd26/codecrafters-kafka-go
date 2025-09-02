package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

const port = 9092
const minApiVersionsVersion int16 = 0
const maxApiVersionsVersion int16 = 4
const invalidApiVersionErrorCode uint16 = 35
const tagBuffer = 0x00

type ApiVersion struct {
	apiKey       uint16
	minSupported uint16
	maxSupported uint16
}

var apiVersions = []*ApiVersion{
	// ApiVersions API
	{
		apiKey:       18,
		minSupported: 3,
		maxSupported: 4,
	},
}

func main() {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", port))
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	readBuf := make([]byte, 1024)
	_, err = conn.Read(readBuf)
	if err != nil {
		panic(fmt.Sprintf("Error reading input %s", err))
	}

	// get correlation_id from v2 request header: https://kafka.apache.org/protocol.html#protocol_messages
	// 4 bytes for message_size
	// 2 bytes for request_api_key
	// 2 bytes for request_api_version (6:8)
	requestApiVersion := readBuf[6:8]
	requestApiVersionInt := int16(binary.BigEndian.Uint16(requestApiVersion))
	// 4 bytes for correlation_id (8:12)
	correlationId := readBuf[8:12]

	// Build response, first add correlation_id header
	response := correlationId

	// Api Versions response body:
	// Add error code
	if requestApiVersionInt > maxApiVersionsVersion || requestApiVersionInt < minApiVersionsVersion {
		response = binary.BigEndian.AppendUint16(response, invalidApiVersionErrorCode)
	} else {
		// 2 byte error code
		response = append(response, 0x00, 0x00)
		response = append(response, createApiVersionsBytes()...)
		// 4 byte throttle time int + tag buffer
		response = append(response, 0x00, 0x00, 0x00, 0x00, tagBuffer)
	}

	// now add the message size at the beginning
	messageSize := binary.BigEndian.AppendUint32(nil, uint32(len(response)))
	response = append(messageSize, response...)

	_, err = conn.Write(response)
	if err != nil {
		panic(fmt.Sprintf("Error writing response: %s", err))
	}
}

func createApiVersionsBytes() []byte {
	// 1 array length byte + 3 * 2byte integers + 1byte tag buffer per api
	// numBytes := len(apiVersions)*(3*2+1) + 1
	var resp []byte
	resp = append(resp, byte(len(apiVersions)+1))
	for i := range apiVersions {
		resp = binary.BigEndian.AppendUint16(resp, apiVersions[i].apiKey)
		resp = binary.BigEndian.AppendUint16(resp, apiVersions[i].minSupported)
		resp = binary.BigEndian.AppendUint16(resp, apiVersions[i].maxSupported)
		resp = append(resp, tagBuffer)
	}
	return resp
}
