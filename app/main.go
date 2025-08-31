package main

import (
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
	_, err = l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
}
