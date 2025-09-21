package main

import (
	"encoding/binary"
	"fmt"
	"log"
)

const INVALID_VERSION_ERR int16 = 35
const UNKNOWN_TOPIC_ERR int16 = 3
const TAG_BUFFER = 0x00

const FETCH int16 = 1
const API_VERSIONS int16 = 18
const DESCRIBE_TOPIC_PARTITIONS int16 = 75

type KafkaContext struct {
	topicsByName TopicsByName
}

type KafkaRequest struct {
	apiKey        uint16
	apiVersion    uint16
	correlationId uint32
	clientId      []byte
	body          []byte
}

type KafkaRequestHeaders struct {
	apiKey        int16
	apiVersion    int16
	correlationId int32
	clientId      []byte
}

type ApiVersion struct {
	apiKey       int16
	minSupported int16
	maxSupported int16
}

var apiVersions = map[int16]*ApiVersion{
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
	FETCH: {
		apiKey:       FETCH,
		minSupported: 16,
		maxSupported: 16,
	},
}

func handleInput(d *Decoder, kContext *KafkaContext) ([]byte, error) {
	headers, err := parseInputHeaders(d)
	if err != nil {
		return nil, fmt.Errorf("Error parsing input: %w", err)
	}

	// Build response v0 header (just a correlation_id)
	response := binary.BigEndian.AppendUint32(nil, uint32(headers.correlationId))

	// Add response body
	var body []byte
	switch headers.apiKey {
	case API_VERSIONS:
		body, err = handleApiVersionsRequest(headers)
	case DESCRIBE_TOPIC_PARTITIONS:
		// v1 response header has a tag buffer
		response = append(response, TAG_BUFFER)
		body, err = handleDescribeTopicPartitionsRequest(d, kContext.topicsByName)
	default:
		return nil, fmt.Errorf("Unsupported request api key: %v", headers.apiKey)
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

func parseInputHeaders(d *Decoder) (*KafkaRequestHeaders, error) {
	// 4 bytes for message_size
	_, err := d.Int32()
	if err != nil {
		return nil, err
	}
	apiKey, err := d.Int16()
	if err != nil {
		return nil, err
	}
	apiVersion, err := d.Int16()
	if err != nil {
		return nil, err
	}
	correlationId, err := d.Int32()
	if err != nil {
		return nil, err
	}
	clientId, err := d.NullableBytes()
	if err != nil {
		return nil, err
	}
	// 1 byte tag buffer and then message body
	_, err = d.Int8()
	if err != nil {
		return nil, err
	}
	bodyLen := d.pos
	body := make([]byte, bodyLen)
	copy(body, d.buf[:bodyLen])

	return &KafkaRequestHeaders{
		apiKey:        apiKey,
		apiVersion:    apiVersion,
		correlationId: correlationId,
		clientId:      clientId,
	}, nil
}

// -------------------- API Versions ------------------------

func handleApiVersionsRequest(headers *KafkaRequestHeaders) ([]byte, error) {
	var body []byte

	if headers.apiVersion > apiVersions[API_VERSIONS].maxSupported || headers.apiVersion < apiVersions[API_VERSIONS].minSupported {
		body = binary.BigEndian.AppendUint16(body, uint16(INVALID_VERSION_ERR))
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
		resp = binary.BigEndian.AppendUint16(resp, uint16(apiVersions[i].apiKey))
		resp = binary.BigEndian.AppendUint16(resp, uint16(apiVersions[i].minSupported))
		resp = binary.BigEndian.AppendUint16(resp, uint16(apiVersions[i].maxSupported))
		resp = append(resp, TAG_BUFFER)
	}
	return resp
}

// ---------------- Describe Topic Partitions -----------------

func handleDescribeTopicPartitionsRequest(d *Decoder, topicsByName TopicsByName) ([]byte, error) {
	arrayLen, err := d.Int8()
	arrayLen--
	if err != nil {
		return nil, err
	}
	var topicNames [][]byte
	for range arrayLen {
		topicLen, err := d.Int8()
		topicLen--
		if err != nil {
			return nil, err
		}
		topicName, err := d.ByteArray(int(topicLen))
		if err != nil {
			return nil, err
		}
		topicNames = append(topicNames, topicName)
		// tag buffer
		_, err = d.Int8()
		if err != nil {
			return nil, err
		}
	}

	log.Printf("Got %v topics from DescribeTopicPartitions request, topics: %s", len(topicNames), topicNames)

	// body
	var body []byte
	// 4 byte throttle time
	body = append(body, 0x00, 0x00, 0x00, 0x00)
	// topics array, first the length
	body = binary.AppendUvarint(body, uint64(len(topicNames)+1))
	for _, topicName := range topicNames {
		topic, exists := topicsByName[string(topicName)]
		if !exists {
			log.Printf("Unknown topic %s", topicName)
			body = append(body, unknownTopicResponse(topicName)...)
		} else {
			log.Printf("Known topic %s: %v", topicName, &topic)
			body = append(body, topicResponse(topic)...)
		}
	}
	// next cursor (used for pagination, return 0xff null value for now) + tag buffer
	body = append(body, 0xff, TAG_BUFFER)

	log.Printf("Response body length: %v", len(body))

	return body, nil
}

func unknownTopicResponse(topicName []byte) []byte {
	respLength := 2 + 1 + len(topicName) + 16 + 1 + 1 + 4 + 1
	resp := make([]byte, 0, respLength)
	// 2 byte error code
	resp = binary.BigEndian.AppendUint16(resp, uint16(UNKNOWN_TOPIC_ERR))
	// topic name length
	resp = binary.AppendUvarint(resp, uint64(len(topicName)+1))
	// topic name
	resp = append(resp, topicName...)
	// topic ID (16-byte UUID) all zeros, indicating null / unassigned UUID
	var topicId uuid
	resp = append(resp, topicId[:]...)
	// is internal (0 for no)
	resp = append(resp, 0x00)
	// partitions array (just return length 1 for now to indicate an empty array)
	resp = binary.AppendUvarint(resp, 1)
	// topic authorized operations, a 4 byte bitfield representing the authorized topics,
	// see list https://github.com/apache/kafka/blob/1962917436f463541f9bb63791b7ed55c23ce8c1/clients/src/main/java/org/apache/kafka/common/acl/AclOperation.java#L44
	authorizedOperations := uint32(0x0d_f8)
	resp = binary.BigEndian.AppendUint32(resp, authorizedOperations)
	resp = append(resp, TAG_BUFFER)
	return resp
}

func topicResponse(topic *Topic) []byte {
	respLength := 2 + 1 + len(topic.name) + 16 + 1 + 1 + 4 + 1
	resp := make([]byte, 0, respLength)
	// 2 byte error code
	resp = binary.BigEndian.AppendUint16(resp, 0)
	// topic name length
	resp = binary.AppendUvarint(resp, uint64(len(topic.name)+1))
	// topic name
	resp = append(resp, topic.name...)
	// topic ID (16-byte UUID)
	resp = append(resp, topic.id[:]...)
	// is internal (0 for no)
	if topic.isInternal {
		resp = append(resp, 0x01)
	} else {
		resp = append(resp, 0x00)
	}
	// partitions array, first the length
	log.Printf("adding %d partitions: %v", len(topic.partitions), &topic.partitions)
	// resp = append(resp, byte(len(topic.partitions)+1))
	resp = binary.AppendUvarint(resp, uint64(len(topic.partitions)+1))
	for _, partition := range topic.partitions {
		// 2 byte error code
		resp = binary.BigEndian.AppendUint16(resp, uint16(partition.errorCode))
		// 4 byte partition index
		resp = binary.BigEndian.AppendUint32(resp, uint32(partition.index))
		// 4 byte leader ID
		resp = binary.BigEndian.AppendUint32(resp, uint32(partition.leaderId))
		// 4 byte leader epoch
		resp = binary.BigEndian.AppendUint32(resp, uint32(partition.leaderEpoch))
		// 4 byte array of 4 byte replica broker IDs
		resp = binary.AppendUvarint(resp, uint64(len(partition.replicas)+1))
		for _, replica := range partition.replicas {
			resp = binary.BigEndian.AppendUint32(resp, uint32(replica))
		}
		// 4 byte array of 4 byte in-sync replica broker IDs
		resp = binary.AppendUvarint(resp, uint64(len(partition.inSyncReplicas)+1))
		for _, isr := range partition.inSyncReplicas {
			resp = binary.BigEndian.AppendUint32(resp, uint32(isr))
		}
		// eligible leaders array
		resp = binary.AppendUvarint(resp, uint64(len(partition.eligibleLeaderReplicas)+1))
		for _, elr := range partition.eligibleLeaderReplicas {
			resp = binary.BigEndian.AppendUint32(resp, uint32(elr))
		}
		// last known eligible leaders array
		resp = binary.AppendUvarint(resp, uint64(len(partition.lastKnownEligibleLeaders)+1))
		for _, lk := range partition.lastKnownEligibleLeaders {
			resp = binary.BigEndian.AppendUint32(resp, uint32(lk))
		}
		// offline replica IDs array
		resp = binary.AppendUvarint(resp, uint64(len(partition.offlineReplicas)+1))
		for _, or := range partition.offlineReplicas {
			resp = binary.BigEndian.AppendUint32(resp, uint32(or))
		}
		resp = append(resp, TAG_BUFFER)
	}
	// topic authorized operations, a 4 byte bitfield representing the authorized topics
	resp = binary.BigEndian.AppendUint32(resp, uint32(topic.authorizedOperations))
	resp = append(resp, TAG_BUFFER)

	return resp
}
