package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

const CLUSTER_METADATA_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

type uuid [16]byte
type RecordType int8

const (
	RECORD_TYPE_UNKNOWN       RecordType = 0
	RECORD_TYPE_TOPIC         RecordType = 2
	RECORD_TYPE_PARTITION     RecordType = 3
	RECORD_TYPE_FEATURE_LEVEL RecordType = 12
)

type TopicsByName map[string]*Topic
type TopicsById map[uuid]*Topic

type Topic struct {
	name                 string
	id                   uuid
	isInternal           bool
	partitions           []*TopicPartition
	authorizedOperations int32
}

type TopicPartition struct {
	errorCode                int16
	index                    int32
	leaderId                 int32
	leaderEpoch              int32
	replicas                 []int32
	inSyncReplicas           []int32
	eligibleLeaderReplicas   []int32
	lastKnownEligibleLeaders []int32
	offlineReplicas          []int32
}

type MetadataRecordBatch struct {
	baseOffset           int64
	partitionLeaderEpoch int32
	magicByte            int8
	CRC                  int32
	attributes           int16
	lastOffsetDelta      int32
	baseTimestamp        int64
	maxTimestamp         int64
	producerID           int64
	producerEpoch        int16
	baseSequence         int32
	records              []any
}

type RecordBase struct {
	attributes     int8
	timestampDelta int64
	offsetDelta    int64
	key            []byte
	frameVersion   int8
	recordType     RecordType
	version        int8
	taggedFields   []byte
}

type FeatureLevelRecord struct {
	RecordBase
	name         string
	featureLevel int16
}

type TopicRecord struct {
	RecordBase
	name    string
	topicId uuid
}

type PartitionRecord struct {
	RecordBase
	partitionId      int32
	topicId          uuid
	replicas         []int32
	inSyncReplicas   []int32
	removingReplicas []int32
	addingReplicas   []int32
	leaderReplica    int32
	leaderEpoch      int32
	partitionEpoch   int32
	directories      []uuid
}

func retrieveClusterMetadata() ([]*MetadataRecordBatch, error) {
	file, err := os.Open(CLUSTER_METADATA_PATH)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	batches, err := parseClusterMetadata(NewDecoder(file))
	if err != nil {
		return nil, err
	}
	return batches, nil
}

func parseClusterMetadata(d *Decoder) ([]*MetadataRecordBatch, error) {
	// readBuf := make([]byte, 1024)
	// n, err := file.Read(readBuf)
	// if err != nil {
	// 	return nil, fmt.Errorf("error reading metadata file: %w", err)
	// }

	var batches []*MetadataRecordBatch
	// p := 0
	for {

		batch, err := parseMetadataRecordBatch(d)
		if errors.Is(err, io.EOF) {
			log.Println("EOF reached")
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error parsing metadata record batch: %w", err)
		}
		batches = append(batches, batch)
	}

	return batches, nil
}

func parseMetadataRecordBatch(d *Decoder) (*MetadataRecordBatch, error) {
	baseOffset, err := d.Int64()
	if err != nil {
		return nil, err
	}
	// batch length, unused for now but needed to advance the reader
	_, err = d.Int32()
	if err != nil {
		return nil, err
	}
	partitionLeaderEpoch, err := d.Int32()
	if err != nil {
		return nil, err
	}
	magicByte, err := d.Int8()
	if err != nil {
		return nil, err
	}
	CRC, err := d.Int32()
	if err != nil {
		return nil, err
	}
	attributes, err := d.Int16()
	if err != nil {
		return nil, err
	}
	lastOffsetDelta, err := d.Int32()
	if err != nil {
		return nil, err
	}
	baseTimestamp, err := d.Int64()
	if err != nil {
		return nil, err
	}
	maxTimestamp, err := d.Int64()
	if err != nil {
		return nil, err
	}
	producerID, err := d.Int64()
	if err != nil {
		return nil, err
	}
	producerEpoch, err := d.Int16()
	if err != nil {
		return nil, err
	}
	baseSequence, err := d.Int32()
	if err != nil {
		return nil, err
	}
	numRecords, err := d.Int32()
	if err != nil {
		return nil, err
	}

	records := make([]any, 0, numRecords)
	for i := 0; i < int(numRecords); i++ {
		record, err := parseMetadataRecord(d)
		if err != nil {
			return nil, fmt.Errorf("error parsing record: %w", err)
		}
		records = append(records, record)
	}

	recordBatch := &MetadataRecordBatch{
		baseOffset:           baseOffset,
		partitionLeaderEpoch: partitionLeaderEpoch,
		magicByte:            magicByte,
		CRC:                  CRC,
		attributes:           attributes,
		lastOffsetDelta:      lastOffsetDelta,
		baseTimestamp:        baseTimestamp,
		maxTimestamp:         maxTimestamp,
		producerID:           producerID,
		producerEpoch:        producerEpoch,
		baseSequence:         baseSequence,
		records:              records,
	}
	return recordBatch, nil
}

func parseMetadataRecord(d *Decoder) (any, error) {
	// record length is not used directly, but need to advance the reader
	_, err := d.Varint()
	if err != nil {
		return nil, err
	}
	attributes, err := d.Int8()
	if err != nil {
		return nil, err
	}
	timestampDelta, err := d.Varint()
	if err != nil {
		return nil, err
	}
	offsetDelta, err := d.Varint()
	if err != nil {
		return nil, err
	}
	keyLength, err := d.Varint()
	if err != nil {
		return nil, err
	}
	var key []byte
	if keyLength > 0 {
		key, err = d.ByteArray(int(keyLength))
		if err != nil {
			return nil, err
		}
	}
	// value length is not used directly, but need to advance the reader
	_, err = d.Varint()
	if err != nil {
		return nil, err
	}

	// parse value
	frameVersion, err := d.Int8()
	if err != nil {
		return nil, err
	}
	recordTypeByte, err := d.Int8()
	if err != nil {
		return nil, err
	}
	recordType := RecordType(recordTypeByte)
	if recordType == RECORD_TYPE_UNKNOWN {
		return nil, fmt.Errorf("unknown record type %q", recordTypeByte)
	}
	version, err := d.Int8()
	if err != nil {
		return nil, err
	}

	base := &RecordBase{
		attributes:     attributes,
		timestampDelta: timestampDelta,
		offsetDelta:    offsetDelta,
		key:            key,
		frameVersion:   frameVersion,
		recordType:     RecordType(recordType),
		version:        version,
	}

	var record any
	switch recordType {
	case RECORD_TYPE_FEATURE_LEVEL:
		record, err = parseFeatureLevelRecord(d, base)
	case RECORD_TYPE_TOPIC:
		record, err = parseTopicRecord(d, base)
	case RECORD_TYPE_PARTITION:
		record, err = parsePartitionRecord(d, base)
	default:
		return nil, fmt.Errorf("Record type not supported")
	}
	if err != nil {
		return nil, err
	}

	return record, nil
}

func parseFeatureLevelRecord(d *Decoder, base *RecordBase) (*FeatureLevelRecord, error) {
	nameLength, err := d.Varint()
	if err != nil {
		return nil, err
	}
	nameBytes, err := d.ByteArray(int(nameLength))
	if err != nil {
		return nil, err
	}
	featureLevel, err := d.Int16()
	if err != nil {
		return nil, err
	}
	taggedFields, err := parseTaggedFields(d)
	if err != nil {
		return nil, err
	}
	base.taggedFields = taggedFields

	featureLevelRecord := &FeatureLevelRecord{
		RecordBase:   *base,
		name:         string(nameBytes),
		featureLevel: featureLevel,
	}
	return featureLevelRecord, nil
}

func parseTopicRecord(d *Decoder, base *RecordBase) (*TopicRecord, error) {
	nameLength, err := d.Varint()
	if err != nil {
		return nil, err
	}
	nameBytes, err := d.ByteArray(int(nameLength))
	if err != nil {
		return nil, err
	}
	topicId, err := d.UUID()
	if err != nil {
		return nil, err
	}
	taggedFields, err := parseTaggedFields(d)
	if err != nil {
		return nil, err
	}
	base.taggedFields = taggedFields

	record := &TopicRecord{
		RecordBase: *base,
		name:       string(nameBytes),
		topicId:    topicId,
	}
	return record, nil
}

func parsePartitionRecord(d *Decoder, base *RecordBase) (*PartitionRecord, error) {
	partitionId, err := d.Int32()
	if err != nil {
		return nil, err
	}
	topicId, err := d.UUID()
	if err != nil {
		return nil, err
	}
	replicas, err := parseReplicaArray(d)
	if err != nil {
		return nil, err
	}
	inSyncReplicas, err := parseReplicaArray(d)
	if err != nil {
		return nil, err
	}
	removingReplicas, err := parseReplicaArray(d)
	if err != nil {
		return nil, err
	}
	addingReplicas, err := parseReplicaArray(d)
	if err != nil {
		return nil, err
	}
	leaderReplica, err := d.Int32()
	if err != nil {
		return nil, err
	}
	leaderEpoch, err := d.Int32()
	if err != nil {
		return nil, err
	}
	partitionEpoch, err := d.Int32()
	if err != nil {
		return nil, err
	}
	numDirectories, err := d.Varint()
	if err != nil {
		return nil, err
	}
	directories := make([]uuid, 0, numDirectories)
	for range numDirectories {
		dirId, err := d.UUID()
		if err != nil {
			return nil, err
		}
		directories = append(directories, dirId)
	}
	taggedFields, err := parseTaggedFields(d)
	if err != nil {
		return nil, err
	}
	base.taggedFields = taggedFields

	partitionRecord := &PartitionRecord{
		RecordBase:       *base,
		partitionId:      partitionId,
		topicId:          topicId,
		replicas:         replicas,
		inSyncReplicas:   inSyncReplicas,
		removingReplicas: removingReplicas,
		addingReplicas:   addingReplicas,
		leaderReplica:    leaderReplica,
		leaderEpoch:      leaderEpoch,
		partitionEpoch:   partitionEpoch,
		directories:      directories,
	}
	return partitionRecord, nil
}

func parseTaggedFields(d *Decoder) ([]byte, error) {
	numTaggedFields, err := d.Varint()
	if err != nil {
		return nil, fmt.Errorf("error parsing tagged fields: %w", err)
	}
	if numTaggedFields > 0 {
		return nil, fmt.Errorf("tagged fields not supported yet")
	}
	return nil, nil
}

// parseReplicaArray produces a slice of int32 replica ids from the decoder
func parseReplicaArray(d *Decoder) ([]int32, error) {
	numReplicas, err := d.Varint()
	if err != nil {
		return nil, err
	}
	replicas := make([]int32, 0, numReplicas)
	for range numReplicas {
		replica, err := d.Int32()
		if err != nil {
			return nil, err
		}
		replicas = append(replicas, replica)
	}
	return replicas, nil
}

func extractVarInt(data []byte) (value uint64, consumed int, err error) {
	const (
		continuationMask = 0x80
		dataMask         = 0x7f
		maxBytes         = 10
	)

	var shift uint
	for i, b := range data {
		if i == maxBytes {
			return 0, i, fmt.Errorf("varint too long")
		}
		// remove the continuation bit
		part := uint64(b & dataMask)
		// shift the
		value |= part << shift
		consumed = i + 1

		if b&continuationMask == 0 {
			// TODO: overflow sanity check on final byte if i==9
			return value, consumed, nil
		}
		shift += 7
	}

	return 0, consumed, fmt.Errorf("incomplete varint")
}

func extractSignedVarInt(data []byte) (value int64, consumed int, err error) {
	u, consumed, err := extractVarInt(data)
	if err != nil {
		return 0, consumed, err
	}
	// ZigZag decode
	value = int64((u >> 1) ^ uint64(-(u & 1)))
	return
}

func produceTopicMap() (TopicsByName, error) {
	batches, err := retrieveClusterMetadata()
	if err != nil {
		return nil, fmt.Errorf("error retrieving cluster metadata: %w", err)
	}

	topicIdMap := make(TopicsById)
	for _, batch := range batches {
		for _, record := range batch.records {
			switch r := record.(type) {
			case *TopicRecord:
				topic, exists := topicIdMap[r.topicId]
				if exists {
					if topic.name != "" {
						return nil, fmt.Errorf("duplicate topic ID found in metadata: %x", r.topicId)
					}
					topic.name = r.name
				} else {
					topicIdMap[r.topicId] = &Topic{
						name:       r.name,
						id:         r.topicId,
						isInternal: false,
					}
				}

			case *PartitionRecord:
				topic, exists := topicIdMap[r.topicId]
				if !exists {
					topic = &Topic{
						id: r.topicId,
					}
					topicIdMap[r.topicId] = topic
				}
				topic.partitions = append(topic.partitions, &TopicPartition{
					errorCode:                0,
					index:                    r.partitionId,
					leaderId:                 r.leaderReplica,
					leaderEpoch:              r.leaderEpoch,
					replicas:                 r.replicas,
					inSyncReplicas:           r.inSyncReplicas,
					eligibleLeaderReplicas:   nil,
					lastKnownEligibleLeaders: nil,
					offlineReplicas:          nil, // TODO: fix
				})
			case *FeatureLevelRecord:
				// ignore for now
			default:
				return nil, fmt.Errorf("unknown record type in metadata batch: %T", r)
			}
		}
	}

	topicNameMap := make(TopicsByName)
	for _, topic := range topicIdMap {
		if topic.name == "" {
			return nil, fmt.Errorf("topic with ID %x has no name", topic.id)
		}
		if _, exists := topicNameMap[topic.name]; exists {
			return nil, fmt.Errorf("duplicate topic name found in metadata: %s", topic.name)
		}
		topicNameMap[topic.name] = topic
	}

	return topicNameMap, nil
}
