package main

import (
	"encoding/binary"
	"fmt"
	"log"
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

func handleInput(input []byte) ([]byte, error) {
	var request *KafkaRequest
	request, err := parseInput(input)
	if err != nil {
		return nil, fmt.Errorf("Error parsing input: %w", err)
	}

	// Build response v0 header (just a correlation_id)
	response := binary.BigEndian.AppendUint32(nil, request.correlationId)

	// Add response body
	var body []byte
	switch request.apiKey {
	case API_VERSIONS:
		body, err = handleApiVersionsRequest(request)
	case DESCRIBE_TOPIC_PARTITIONS:
		// v1 response header has a tag buffer
		response = append(response, TAG_BUFFER)
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

// -------------------- API Versions ------------------------

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

// ---------------- Describe Topic Partitions -----------------

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
