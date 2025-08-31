package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

const port = 9092

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

	// get correlation_id from v2 request header
	// 4 bytes for message_size
	// 2 bytes for request_api_key
	// 2 bytes for request_api_version
	// 4 bytes for correlation_id (8:12)
	correlationId := readBuf[8:12]

	response := binary.BigEndian.AppendUint32(nil, uint32(0))
	response = append(response, correlationId...)

	_, err = conn.Write(response)
	if err != nil {
		panic(fmt.Sprintf("Error writing response: %s", err))
	}
}
