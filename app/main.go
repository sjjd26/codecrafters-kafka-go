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

const INVALID_VERSION_ERR uint16 = 35
const UNKNOWN_TOPIC_ERR uint16 = 3
const TAG_BUFFER = 0x00

const API_VERSIONS uint16 = 18
const DESCRIBE_TOPIC_PARTITIONS uint16 = 75

type KafkaRequest struct {
	apiKey        uint16
	apiVersion    uint16
	correlationId uint32
	clientId      []byte
	body          []byte
}

type ApiVersion struct {
	apiKey       uint16
	minSupported uint16
	maxSupported uint16
}

var apiVersions = map[uint16]*ApiVersion{
	API_VERSIONS: {
		apiKey:       API_VERSIONS,
		minSupported: 3,
		maxSupported: 4,
	},
	DESCRIBE_TOPIC_PARTITIONS: {
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

	// for now just assume the input is always one command and valid
	// no detailed parsing
	response, err := handleInput(readBuf)
	if err != nil {
		return fmt.Errorf("Error handling input: %w", err)
	}
	_, err = syscall.Write(clientFd, response)
	if err != nil {
		return fmt.Errorf("Error writing response to client: %w", err)
	}

	return nil
}

func handleInput(input []byte) ([]byte, error) {
	var request *KafkaRequest
	request, err := parseInput(input)
	if err != nil {
		return nil, fmt.Errorf("Error parsing input: %w", err)
	}

	// Build response v0 header (just a correlation_id)
	response := binary.BigEndian.AppendUint32(nil, request.correlationId)
	response = append(response, TAG_BUFFER)

	// Add response body
	var body []byte
	switch request.apiKey {
	case API_VERSIONS:
		body, err = handleApiVersionsRequest(request)
	case DESCRIBE_TOPIC_PARTITIONS:
		body, err = handleDescribeTopicPartitionsRequest(request)
	default:
		return nil, fmt.Errorf("Unsupported request api key: %v", request.apiKey)
	}
	if err != nil {
		return nil, fmt.Errorf("Error handling request: %w", err)
	}
	response = append(response, body...)

	// now add the message size at the beginning
	messageSize := binary.BigEndian.AppendUint32(nil, uint32(len(response)))
	response = append(messageSize, response...)

	return response, nil
}

func parseInput(input []byte) (*KafkaRequest, error) {
	if len(input) < 15 {
		return nil, fmt.Errorf("Input length, %v, is shorter than minimum 15", len(input))
	}
	// v2 request headers https://kafka.apache.org/protocol.html#protocol_messages
	// 4 bytes for message_size
	messageSize := binary.BigEndian.Uint32(input[:4])
	if len(input)-4 < int(messageSize) {
		return nil, fmt.Errorf("Remaining input length, %v, is shorter than declared message size: %v", len(input), messageSize)
	}
	// 2 bytes for request_api_key
	apiKey := binary.BigEndian.Uint16(input[4:6])
	// 2 bytes for request_api_version
	apiVersion := binary.BigEndian.Uint16(input[6:8])
	// 4 bytes for correlation_id
	correlationId := binary.BigEndian.Uint32(input[8:12])
	// client_id nullable string
	clientId, err := extractNullableString(input[12:])
	if err != nil {
		return nil, fmt.Errorf("Error parsing client id: %w", err)
	}
	// 1 byte tag buffer and then message body
	bodyStart := 4 + 2 + 2 + 4 + 2 + len(clientId) + 1
	bodyEnd := int(messageSize) + 4
	body := make([]byte, bodyEnd-bodyStart)
	copy(body, input[bodyStart:bodyEnd])

	return &KafkaRequest{
		apiKey:        apiKey,
		apiVersion:    apiVersion,
		correlationId: correlationId,
		clientId:      clientId,
		body:          body,
	}, nil
}

// Nullable string: 2 bytes for length, followed by the string
// A null string is a string with length 0, this results in an empty byte slice
func extractNullableString(input []byte) ([]byte, error) {
	if len(input) < 2 {
		return nil, fmt.Errorf("Nullable string input length, %v, is shorter than minimum 2", len(input))
	}
	end := int(binary.BigEndian.Uint16(input[:2])) + 2
	if len(input) < end {
		return nil, fmt.Errorf("Declared string end, %v, is longer than input length %v", end, len(input))
	}
	str := make([]byte, end-2)
	copy(str, input[2:])
	return str, nil
}

func handleApiVersionsRequest(request *KafkaRequest) ([]byte, error) {
	var body []byte

	// Add error code
	if request.apiVersion > apiVersions[API_VERSIONS].maxSupported || request.apiVersion < apiVersions[API_VERSIONS].minSupported {
		body = binary.BigEndian.AppendUint16(body, INVALID_VERSION_ERR)
	} else {
		// 2 byte error code
		body = append(body, 0x00, 0x00)
		body = append(body, createApiVersionsBytes()...)
		// 4 byte throttle time int + tag buffer
		body = append(body, 0x00, 0x00, 0x00, 0x00, TAG_BUFFER)
	}

	return body, nil
}

func handleDescribeTopicPartitionsRequest(request *KafkaRequest) ([]byte, error) {
	// extract request body
	log.Printf("Received request body: %q", request.body)
	// topics array: 1 byte for array length
	arrayLen := int(request.body[0]) - 1
	var topics [][]byte
	p := 1
	for range arrayLen {
		topicLen := int(request.body[p])
		p++
		topicName := make([]byte, topicLen-1)
		copy(topicName, request.body[p:p+topicLen])
		// topics = append(topics, request.body[p:p+topicLen])
		topics = append(topics, topicName)
		// + 1 for tag buffer
		p += topicLen + 1
	}

	log.Printf("Got %v topics from DescribeTopicPartitions request, topics: %s", len(topics), topics)

	// partitionLimit := binary.BigEndian.Uint32(request.body[p : p+4])
	// p += 4
	// cursor := request.body[p]

	// now create body from request
	var body []byte
	// 4 byte throttle time
	body = append(body, 0x00, 0x00, 0x00, 0x00)
	// topics array, first the length
	body = append(body, byte(arrayLen+1))
	for i := range topics {
		// error code, for now always return uknown topic error
		body = binary.BigEndian.AppendUint16(body, UNKNOWN_TOPIC_ERR)
		// topic length
		body = append(body, byte(len(topics[i])+1))
		// topic name
		body = append(body, topics[i]...)
		// topic ID (16-byte UUID) all zeros, indicating null / unassigned UUID
		topicId := make([]byte, 16)
		body = append(body, topicId...)
		// is internal (0 for no)
		body = append(body, 0x00)
		// partitions array (just return length 1 for now to indicate an empty array)
		body = append(body, 0x01)
		// topic authorized operations, a 4 byte bitfield representing the authorized topics,
		// see list https://github.com/apache/kafka/blob/1962917436f463541f9bb63791b7ed55c23ce8c1/clients/src/main/java/org/apache/kafka/common/acl/AclOperation.java#L44
		authorizedOperations := uint32(0b0000_1101_1111_1000)
		body = binary.BigEndian.AppendUint32(body, authorizedOperations)
		body = append(body, TAG_BUFFER)
	}
	// next cursor (used for pagination, return 0xff null value for now) + tag buffer
	body = append(body, 0xff, TAG_BUFFER)

	log.Printf("Response body length: %v", len(body))

	return body, nil
}

func createApiVersionsBytes() []byte {
	// 1 array length byte + 3 * 2 byte integers + 1 byte tag buffer per api
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
