package main

import (
	"encoding/binary"
	"fmt"
	"log"
)

const INVALID_VERSION_ERR int16 = 35
const UNKNOWN_TOPIC_ERR int16 = 3
const UNKNOWN_TOPIC_ID_ERR int16 = 100
const TAG_BUFFER = 0x00

const FETCH int16 = 1
const API_VERSIONS int16 = 18
const DESCRIBE_TOPIC_PARTITIONS int16 = 75

type KafkaContext struct {
	topicsByName TopicsByName
	topicsById   TopicsById
}

type KafkaRequestHeaders struct {
	apiKey        int16
	apiVersion    int16
	correlationId int32
	clientId      []byte
}

type FetchRequest struct {
	KafkaRequestHeaders
	maxWait         int32
	minBytes        int32
	maxBytes        int32
	isolationLevel  int8
	sessionId       int32
	sessionEpoch    int32
	topics          []FetchTopic
	forgottenTopics []ForgottenFetchTopic
}

type FetchTopic struct {
	topicId    uuid
	partitions []FetchPartition
}

type FetchPartition struct {
	partition          int32
	currentLeaderEpoch int32
	fetchOffset        int64
	lastFetchedEpoch   int32
	logStartOffset     int64
	maxBytes           int32
}

type ForgottenFetchTopic struct {
	topicId    uuid
	partitions []int32
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
		return nil, fmt.Errorf("error parsing input: %w", err)
	}

	e := NewEncoder(4)
	// Build response v0 header (just a correlation_id)
	e.Int32(headers.correlationId)

	// Add response body
	switch headers.apiKey {
	case API_VERSIONS:
		err = handleApiVersionsRequest(e, headers)
	case DESCRIBE_TOPIC_PARTITIONS:
		// v1 response header has a tag buffer
		e.Int8(TAG_BUFFER)
		err = handleDescribeTopicPartitionsRequest(e, d, kContext.topicsByName)
	case FETCH:
		// v1 response header has a tag buffer
		e.Int8(TAG_BUFFER)
		err = handleFetchRequest(e, d, kContext.topicsById)
	default:
		return nil, fmt.Errorf("unsupported request api key: %v", headers.apiKey)
	}
	if err != nil {
		return nil, fmt.Errorf("error handling request: %w", err)
	}

	// now add the message size at the beginning
	messageSize := binary.BigEndian.AppendUint32(nil, uint32(len(e.Bytes())))
	e.Prepend(messageSize)

	return e.Bytes(), nil
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

func handleApiVersionsRequest(e *Encoder, headers *KafkaRequestHeaders) error {
	if headers.apiVersion > apiVersions[API_VERSIONS].maxSupported || headers.apiVersion < apiVersions[API_VERSIONS].minSupported {
		// 2 byte invalid version error code
		e.Int16(INVALID_VERSION_ERR)
	} else {
		// 2 byte error code
		e.Int16(0)
		writeApiVersionsBytes(e)
		// 4 byte throttle time int + tag buffer
		e.Int32(0)
		e.Int8(TAG_BUFFER)
	}
	return nil
}

func writeApiVersionsBytes(e *Encoder) {
	e.UVarint(uint64(len(apiVersions) + 1))
	for _, v := range apiVersions {
		e.Int16(v.apiKey)
		e.Int16(v.minSupported)
		e.Int16(v.maxSupported)
		e.Int8(TAG_BUFFER)
	}
}

// ---------------- Describe Topic Partitions -----------------

func handleDescribeTopicPartitionsRequest(e *Encoder, d *Decoder, topicsByName TopicsByName) error {
	topicNames, err := parseDescribeTopicPartitionsRequest(d)
	if err != nil {
		return fmt.Errorf("error parsing DescribeTopicPartitions body: %w", err)
	}

	// 4 byte throttle time
	e.Int32(0)
	// topics array, first the length
	e.UVarint(uint64(len(topicNames) + 1))
	for _, topicName := range topicNames {
		topic, exists := topicsByName[string(topicName)]
		if !exists {
			log.Printf("Unknown topic %s", topicName)
			writeUnknownTopicResponse(e, topicName)
		} else {
			log.Printf("Known topic %s: %v", topicName, &topic)
			writeTopicResponse(e, topic)
		}
	}
	// next cursor (used for pagination, return -1/0xff null value for now) + tag buffer
	e.Int8(-1)
	e.Int8(TAG_BUFFER)

	return nil
}

func parseDescribeTopicPartitionsRequest(d *Decoder) ([][]byte, error) {
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
	return topicNames, nil
}

func writeUnknownTopicResponse(e *Encoder, topicName []byte) {
	e.Int16(UNKNOWN_TOPIC_ERR)
	// topic name
	e.UVarint(uint64(len(topicName) + 1))
	e.WriteBytes(topicName)
	// topic ID (16-byte UUID) all zeros, indicating null / unassigned UUID
	var topicId uuid
	e.UUID(topicId)
	// is internal (0 for no)
	e.Int8(0)
	// partitions array (just return length 1 for now to indicate an empty array)
	e.UVarint(1)
	// topic authorized operations, a 4 byte bitfield representing the authorized topics,
	// see list https://github.com/apache/kafka/blob/1962917436f463541f9bb63791b7ed55c23ce8c1/clients/src/main/java/org/apache/kafka/common/acl/AclOperation.java#L44
	authorizedOperations := int32(0x0d_f8)
	e.Int32(authorizedOperations)
	e.Int8(TAG_BUFFER)
}

func writeTopicResponse(e *Encoder, topic *Topic) {
	// error code
	e.Int16(0)
	// topic name
	e.UVarint(uint64(len(topic.name) + 1))
	e.WriteBytes([]byte(topic.name))
	// topic ID (16-byte UUID)
	e.UUID(topic.id)
	// is internal (0 for no)
	if topic.isInternal {
		e.Int8(0)
	} else {
		e.Int8(1)
	}
	// partitions array, first the length
	e.UVarint(uint64(len(topic.partitions) + 1))
	for _, partition := range topic.partitions {
		// 2 byte error code
		e.Int16(partition.errorCode)
		// 4 byte partition index
		e.Int32(partition.index)
		// 4 byte leader ID
		e.Int32(partition.leaderId)
		// 4 byte leader epoch
		e.Int32(partition.leaderEpoch)
		// 4 byte array of 4 byte replica broker IDs
		writeReplicaArray(e, partition.replicas)
		// 4 byte array of 4 byte in-sync replica broker IDs
		writeReplicaArray(e, partition.inSyncReplicas)
		// eligible leaders array
		writeReplicaArray(e, partition.eligibleLeaderReplicas)
		// last known eligible leaders array
		writeReplicaArray(e, partition.lastKnownEligibleLeaders)
		// offline replica IDs array
		writeReplicaArray(e, partition.offlineReplicas)
		e.Int8(TAG_BUFFER)
	}
	// topic authorized operations, a 4 byte bitfield representing the authorized topics
	e.Int32(int32(topic.authorizedOperations))
	e.Int8(TAG_BUFFER)
}

func writeReplicaArray(e *Encoder, replicas []int32) {
	e.UVarint(uint64(len(replicas) + 1))
	for _, replica := range replicas {
		e.Int32(replica)
	}
}

// -------------------- Fetch -------------------------

func handleFetchRequest(e *Encoder, d *Decoder, topicsById TopicsById) error {
	fetchRequest, err := parseFetchRequest(d)
	if err != nil {
		return fmt.Errorf("error parsing Fetch body: %w", err)
	}

	writeFetchResponse(e, fetchRequest, topicsById)
	return nil
}

func parseFetchRequest(d *Decoder) (*FetchRequest, error) {
	maxWait, err := d.Int32()
	if err != nil {
		return nil, err
	}
	minBytes, err := d.Int32()
	if err != nil {
		return nil, err
	}
	maxBytes, err := d.Int32()
	if err != nil {
		return nil, err
	}
	isolationLevel, err := d.Int8()
	if err != nil {
		return nil, err
	}
	sessionId, err := d.Int32()
	if err != nil {
		return nil, err
	}
	sessionEpoch, err := d.Int32()
	if err != nil {
		return nil, err
	}

	// topics
	var topics []FetchTopic
	topicCount, err := d.UVarint()
	topicCount--
	if err != nil {
		return nil, err
	}
	log.Printf("Fetch request for %d topics", topicCount)
	for range topicCount {
		var topic FetchTopic
		topicId, err := d.UUID()
		if err != nil {
			return nil, err
		}
		topic.topicId = topicId
		partitionCount, err := d.UVarint()
		partitionCount--
		if err != nil {
			return nil, err
		}
		log.Printf("Topic %s has %d partitions", topicId, partitionCount)
		for range partitionCount {
			var partition FetchPartition
			partition.partition, err = d.Int32()
			if err != nil {
				return nil, err
			}
			partition.currentLeaderEpoch, err = d.Int32()
			if err != nil {
				return nil, err
			}
			partition.fetchOffset, err = d.Int64()
			if err != nil {
				return nil, err
			}
			partition.lastFetchedEpoch, err = d.Int32()
			if err != nil {
				return nil, err
			}
			partition.logStartOffset, err = d.Int64()
			if err != nil {
				return nil, err
			}
			partition.maxBytes, err = d.Int32()
			if err != nil {
				return nil, err
			}
			_, err = d.Int8() // tag buffer
			if err != nil {
				return nil, err
			}
			topic.partitions = append(topic.partitions, partition)
		}
		_, err = d.Int8() // tag buffer
		if err != nil {
			return nil, err
		}
		topics = append(topics, topic)
	}

	// forgotten topics
	var forgottenTopics []ForgottenFetchTopic
	forgottenTopicCount, err := d.UVarint()
	forgottenTopicCount--
	if err != nil {
		return nil, err
	}
	log.Printf("Fetch request with %d forgotten topics", forgottenTopicCount)
	for range forgottenTopicCount {
		var forgottenTopic ForgottenFetchTopic
		topicId, err := d.UUID()
		if err != nil {
			return nil, err
		}
		forgottenTopic.topicId = topicId
		partitionCount, err := d.UVarint()
		partitionCount--
		if err != nil {
			return nil, err
		}
		for range partitionCount {
			partition, err := d.Int32()
			if err != nil {
				return nil, err
			}
			forgottenTopic.partitions = append(forgottenTopic.partitions, partition)
		}
		_, err = d.Int8() // tag buffer
		if err != nil {
			return nil, err
		}
		forgottenTopics = append(forgottenTopics, forgottenTopic)
	}

	fetchRequest := &FetchRequest{
		maxWait:         maxWait,
		minBytes:        minBytes,
		maxBytes:        maxBytes,
		isolationLevel:  isolationLevel,
		sessionId:       sessionId,
		sessionEpoch:    sessionEpoch,
		topics:          topics,
		forgottenTopics: forgottenTopics,
	}
	return fetchRequest, nil
}

func writeFetchResponse(e *Encoder, fetchRequest *FetchRequest, topicsById TopicsById) {
	// throttle time
	e.Int32(0)
	// error code
	e.Int16(0)
	// session id
	e.Int32(fetchRequest.sessionId)

	// topics
	e.UVarint(uint64(len(fetchRequest.topics) + 1))
	for _, requestTopic := range fetchRequest.topics {
		matchedTopic := topicsById[requestTopic.topicId]

		// topic id
		e.UUID(requestTopic.topicId)

		e.UVarint(uint64(len(requestTopic.partitions) + 1))
		for _, partition := range requestTopic.partitions {
			// partition index
			e.Int32(partition.partition)
			// error code
			if matchedTopic == nil {
				e.Int16(UNKNOWN_TOPIC_ID_ERR)
			} else {
				e.Int16(0)
			}
			// high watermark
			e.Int64(-1)
			// last stable offset
			e.Int64(-1)
			// log start offset
			e.Int64(-1)
			// aborted transactions array length (0 for now)
			e.UVarint(1)
			// preferred read replica
			e.Int32(-1)
			// record set (empty for now)
			e.UVarint(1)
			// partition authorized operations
			// e.Int32(0)
			// tag buffer
			e.Int8(TAG_BUFFER)
		}
		// topic authorized operations
		// e.Int32(0)
		// tag buffer
		e.Int8(TAG_BUFFER)
	}

	// tag buffer
	e.Int8(TAG_BUFFER)
}
