package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"syscall"
)

const MIN_API_VERSION int16 = 0
const MAX_API_VERSION int16 = 4
const INVALID_VERSION_ERR uint16 = 35
const TAG_BUFFER = 0x00

const API_VERSIONS uint16 = 18
const DESCRIBE_TOPIC_PARTITIONS uint16 = 75

type ApiVersion struct {
	apiKey       uint16
	minSupported uint16
	maxSupported uint16
}

var apiVersions = []*ApiVersion{
	// ApiVersions
	{
		apiKey:       API_VERSIONS,
		minSupported: 3,
		maxSupported: 4,
	},
	// DescribeTopicPartitions
	{
		apiKey:       DESCRIBE_TOPIC_PARTITIONS,
		minSupported: 0,
		maxSupported: 0,
	},
}

func main() {
	listener, err := createNonBlockingListener("0.0.0.0:9092")
	if err != nil {
		log.Fatalf("Failed to create non-blocking listener: %s", err)
	}
	defer listener.Close()
	listenerFd := listener.Fd()
	// defer syscall.Close(int(listenerFd))

	epollFd, err := syscall.EpollCreate1(0)
	if err != nil {
		log.Fatalf("Failed to create epoll file descriptor: %s", err)
	}
	defer syscall.Close(epollFd)

	// need to create an event with the required settings for the listenerFd
	// probably just read as this is the listener fd not a connection?
	// look through the settings anyway
	// afterwards need to set up the event loop via EpollWait
	err = addNewFd(epollFd, int(listenerFd))
	if err != nil {
		log.Fatalf("Failed to add listener to epoll: %s", err)
	}

	log.Printf("Added listener %v to epoll %v", int(listenerFd), epollFd)

	eventLoop(epollFd, int(listenerFd))
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
	event := &syscall.EpollEvent{Events: syscall.EPOLLIN, Fd: int32(fd)}
	err := syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_ADD, fd, event)
	if err != nil {
		return fmt.Errorf("Error adding FD %v: %w", fd, err)
	}
	return nil
}

func removeAndCloseFd(epollFd int, fd int) error {
	defer syscall.Close(fd)
	err := syscall.EpollCtl(epollFd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return fmt.Errorf("Error removing FD %v: %w", fd, err)
	}
	return nil
}

func eventLoop(epollFd int, listenerFd int) {
	defer syscall.Close(epollFd)
	events := make([]syscall.EpollEvent, 10)
	timeoutMs := -1 // negative timeout -> indefinite
	exit := false

	log.Println("Beginning event loop")
	for exit == false {
		n, err := syscall.EpollWait(epollFd, events, timeoutMs)
		if err != nil {
			log.Fatalf("Epoll wait returned an error: %s", err)
		}

		for i := range n {
			fd := int(events[i].Fd)
			if fd == listenerFd {
				// check event bitmask
				if events[i].Events&syscall.EPOLLERR > 0 {
					log.Fatalf("Listener received error event")
				}
				if events[i].Events&syscall.EPOLLHUP > 0 {
					log.Println("Listener received hangup event")
					exit = true
				}
				if events[i].Events&syscall.EPOLLIN == 0 {
					log.Println("Listener didn't receive read event")
					continue
				}
				// if read bit is present, there is a new connection
				connFd, err := acceptNewConnection(listenerFd)
				if err != nil {
					log.Fatalf("Error accepting connection: %s", err)
				}
				err = addNewFd(epollFd, connFd)
				if err != nil {
					log.Fatalf("Error adding new conn %v to epoll: %s", connFd, err)
				}
			} else {
				// check event bitmask
				if events[i].Events&syscall.EPOLLERR > 0 {
					log.Printf("Connection %v received error event", fd)
					err = removeAndCloseFd(epollFd, fd)
					if err != nil {
						log.Fatalln(err)
					}
				}
				if events[i].Events&syscall.EPOLLHUP > 0 {
					log.Printf("Connection %v received hangup event", fd)
					err = removeAndCloseFd(epollFd, fd)
					if err != nil {
						log.Fatalln(err)
					}
				}
				err = handleClient(fd)
				if err != nil {
					log.Printf("Error handling client %v: %s", fd, err)
				}
			}
		}
	}
	log.Println("Exited event loop")
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

func handleClient(clientFd int) error {
	log.Printf("Received input from client connection %v", clientFd)
	readBuf := make([]byte, 1024)
	_, err := syscall.Read(clientFd, readBuf)
	if err != nil {
		return fmt.Errorf("Error reading client input: %w", err)
	}
	// input := bytes.TrimRight(readBuf, "\x00\n\r\t ")

	// if len(input) < 12 {
	// 	return fmt.Errorf("Invalid input: not enough bytes")
	// }

	// for now just assume the input is always one command and valid
	// no detailed parsing
	response := handleInput(readBuf)
	_, err = syscall.Write(clientFd, response)
	if err != nil {
		return fmt.Errorf("Error writing response to client: %w", err)
	}

	return nil
}

func handleInput(input []byte) []byte {
	// get correlation_id from v2 request header: https://kafka.apache.org/protocol.html#protocol_messages
	// 4 bytes for message_size
	// 2 bytes for request_api_key
	// 2 bytes for request_api_version (6:8)
	requestApiVersion := input[6:8]
	requestApiVersionInt := int16(binary.BigEndian.Uint16(requestApiVersion))
	// 4 bytes for correlation_id (8:12)
	correlationId := input[8:12]

	// Build response, first add correlation_id header
	response := correlationId

	// Api Versions response body:
	// Add error code
	if requestApiVersionInt > MAX_API_VERSION || requestApiVersionInt < MIN_API_VERSION {
		response = binary.BigEndian.AppendUint16(response, INVALID_VERSION_ERR)
	} else {
		// 2 byte error code
		response = append(response, 0x00, 0x00)
		response = append(response, createApiVersionsBytes()...)
		// 4 byte throttle time int + tag buffer
		response = append(response, 0x00, 0x00, 0x00, 0x00, TAG_BUFFER)
	}

	// now add the message size at the beginning
	messageSize := binary.BigEndian.AppendUint32(nil, uint32(len(response)))
	response = append(messageSize, response...)

	return response
}

func createApiVersionsBytes() []byte {
	// 1 array length byte + 3 * 2byte integers + 1byte tag buffer per api
	numBytes := 1 + (3*2+1)*len(apiVersions)
	resp := make([]byte, 0, numBytes)
	resp = append(resp, byte(len(apiVersions)+1))
	for i := range apiVersions {
		resp = binary.BigEndian.AppendUint16(resp, apiVersions[i].apiKey)
		resp = binary.BigEndian.AppendUint16(resp, apiVersions[i].minSupported)
		resp = binary.BigEndian.AppendUint16(resp, apiVersions[i].maxSupported)
		resp = append(resp, TAG_BUFFER)
	}
	return resp
}
