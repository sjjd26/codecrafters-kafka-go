package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"syscall"
)

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
	listener, err := createNonBlockingListener("0.0.0.0:9092")
	if err != nil {
		log.Fatalf("Failed to create non-blocking listener: %s", err)
	}
	defer listener.Close()
	listenerFd := listener.Fd()

	epollFd, err := syscall.EpollCreate1(0)
	if err != nil {
		log.Fatalf("Failed to create epoll file descriptor: %s", err)
	}

	// need to create an event with the required settings for the listenerFd
	// probably just read as this is the listener fd not a connection?
	// look through the settings anyway
	// afterwards need to set up the event loop via EpollWait
	err = addNewFd(epollFd, int(listenerFd))
	if err != nil {
		log.Fatalf("Failed to add listener to epoll: %s", err)
	}

	eventLoop(epollFd, int(listenerFd))
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

func createNonBlockingListener(addr string) (*os.File, error) {
	listenConfig := net.ListenConfig{Control: setNonBlocking}
	listener, err := listenConfig.Listen(context.TODO(), "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("Error starting listener: %w", err)
	}

	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		return nil, fmt.Errorf("Failed to assert listener as TCPListener")
	}
	// according to docs, closing the listener fd has no effect on the file fd
	defer tcpListener.Close()

	file, err := tcpListener.File()
	if err != nil {
		return nil, fmt.Errorf("Error getting listener file: %w", err)
	}

	return file, nil
}

// setNonBlocking makes the socket non-blocking.
func setNonBlocking(network, address string, c syscall.RawConn) error {
	var err error
	err = c.Control(func(fd uintptr) {
		err = syscall.SetNonblock(int(fd), true)
	})
	return err
}

func addNewFd(epollFd int, fd int) error {
	// Events int is achieved by ORing together the events that we want epoll to wait for https://man7.org/linux/man-pages/man2/epoll_ctl.2.html
	// EPOLLIN is for read events, EPOLLERR (error) and EPOLLHUP (hangup) are both included by default
	event := &syscall.EpollEvent{Events: syscall.EPOLLIN}
	err := syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, fd, event)
	if err != nil {
		return fmt.Errorf("Error adding FD: %w", err)
	}
	return nil
}

func eventLoop(epollFd int, listenerFd int) {
	events := make([]syscall.EpollEvent, 10)
	timeoutMs := -1 // negative timeout -> indefinite
	for {
		n, err := syscall.EpollWait(epollFd, events, timeoutMs)
		if err != nil {
			log.Fatalf("Epoll wait returned an error: %s", err)
		}

		for i := range n {
			fd := int(events[i].Fd)
			if fd == listenerFd {
				// new connection
				connFd, err := acceptNewConnection(listenerFd)
				if err != nil {
					log.Fatalf("Error accepting connection: %s", err)
				}
				addNewFd(epollFd, connFd)
			} else {
				// new input from client conn

			}
		}
	}

}

// Need to handle the error conditions including EPOLLHUP
// Then handle the client inputs
func handleEpollEventErrors(event syscall.EpollEvent) error {
	// events int is a bitmask achieved by ORing together whichever events occurred
	// check if an event occurred by ANDing with the specific event
	if event.Events&syscall.EPOLLERR > 0 {
		return nil
	}
	return nil
}

func acceptNewConnection(listenerFd int) (int, error) {
	connFd, _, err := syscall.Accept(listenerFd)
	if err != nil {
		return -1, fmt.Errorf("Error accepting connection: %w", err)
	}
	err = syscall.SetNonblock(connFd, true)
	if err != nil {
		return -1, fmt.Errorf("Error setting connection to non blocking: %w", err)
	}

	log.Printf("Accepted new connection FD: %v", connFd)
	return connFd, nil
}

func handleInput() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
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
	n, err := conn.Read(readBuf)
	if errors.Is(err, net.ErrClosed) {
		log.Println("Connection closed")
	}
	if err != nil {
		panic(fmt.Sprintf("Error reading input %s", err))
	}
	if n < 12 {
		panic(fmt.Sprintln("Not enough bytes read"))
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
