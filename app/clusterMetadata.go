package main

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"os"
)

const CLUSTER_METADATA_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

type RecordType int8

const (
	RECORD_TYPE_UNKNOWN       RecordType = 0
	RECORD_TYPE_TOPIC         RecordType = 2
	RECORD_TYPE_PARTITION     RecordType = 3
	RECORD_TYPE_FEATURE_LEVEL RecordType = 12
)

type MetadataRecordBatch struct {
	baseOffset           uint64
	partitionLeaderEpoch uint32
	magicByte            byte
	CRC                  uint32
	attributes           uint16
	lastOffsetDelta      uint32
	baseTimestamp        uint64
	maxTimestamp         uint64
	producerID           uint64
	producerEpoch        uint16
	baseSequence         uint32
	records              []any
}

type RecordBase struct {
	attributes     byte
	timestampDelta int64
	offsetDelta    int64
	key            []byte
	frameVersion   int8
	recordType     RecordType
	version        int8
}

type FeatureLevelRecord struct {
	RecordBase
	name         []byte
	featureLevel uint16
}

type TopicRecord struct {
	RecordBase
	name    []byte
	topicID [16]byte
}

type PartitionRecord struct {
	RecordBase
	partitionID      int32
	topicID          [16]byte
	replicas         []int32
	inSyncReplicas   []int32
	removingReplicas []int32
	addingReplicas   []int32
	leaderReplica    int32
	leaderEpoch      int32
	partitionEpoch   int32
	directories      [][16]byte
}

func retrieveClusterMetadata() (*MetadataRecordBatch, error) {
	file, err := os.Open(CLUSTER_METADATA_PATH)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return nil, nil
}

func parseClusterMetadataFile(file os.File) ([]*MetadataRecordBatch, error) {
	readBuf := make([]byte, 1024)
	n, err := file.Read(readBuf)
	if err != nil {
		return nil, fmt.Errorf("Error reading metadata file: %w", err)
	}

	var batches []*MetadataRecordBatch
	p := 0
	for p < n {
		batch, n, err := parseMetadataRecordBatch(readBuf[p:])
		if err != nil {
			return nil, fmt.Errorf("Error parsing metadata record batch: %w", err)
		}
		p += n
		batches = append(batches, batch)
	}

	return batches, nil
}

func parseMetadataRecordBatch(data []byte) (*MetadataRecordBatch, int, error) {
	if len(data) < 12 {
		return nil, len(data), fmt.Errorf("Not enough bytes to parse metadata record batch, need 12 got %d", len(data))
	}
	p := 0
	baseOffset := binary.BigEndian.Uint64(data[p : p+8])
	p += 8
	batchLength := int(binary.BigEndian.Uint32(data[p:p+4])) + 12
	if batchLength > len(data) {
		return nil, len(data), fmt.Errorf("Declared metadata batch length %d is bigger than data length %d", batchLength, len(data))
	}
	p += 4
	partitionLeaderEpoch := binary.BigEndian.Uint32(data[p : p+4])
	p += 4
	magicByte := data[p]
	p += 1
	CRC := binary.BigEndian.Uint32(data[p : p+4])
	p += 4
	attributes := binary.BigEndian.Uint16(data[p : p+2])
	p += 2
	lastOffsetDelta := binary.BigEndian.Uint32(data[p : p+4])
	p += 4
	baseTimestamp := binary.BigEndian.Uint64(data[p : p+8])
	p += 8
	maxTimestamp := binary.BigEndian.Uint64(data[p : p+8])
	p += 8
	producerID := binary.BigEndian.Uint64(data[p : p+8])
	p += 8
	producerEpoch := binary.BigEndian.Uint16(data[p : p+2])
	p += 2
	baseSequence := binary.BigEndian.Uint32(data[p : p+4])
	p += 4
	numRecords := binary.BigEndian.Uint32(data[p : p+4])
	p += 4

	records := make([]any, 0, numRecords)
	// parse records in the batch
	for i := 0; i < int(numRecords); i++ {
		record, n, err := parseMetadataRecord(data[p:])
		if err != nil {
			return nil, batchLength, fmt.Errorf("Error parsing record: %w", err)
		}
		p += n
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
	return recordBatch, batchLength, nil
}

func parseMetadataRecord(data []byte) (any, int, error) {
	p := 0
	varInt, n, err1 := extractSignedVarInt(data[p:])
	recordLength := int(varInt) + n
	p += n
	attributes := data[p]
	p += 1
	timestampDelta, n, err2 := extractSignedVarInt(data[p:])
	p += n
	offsetDelta, n, err3 := extractSignedVarInt(data[p:])
	p += n
	keyLength, n, err4 := extractSignedVarInt(data[p:])
	p += n
	var key []byte
	if keyLength >= 0 {
		key = data[p : p+int(keyLength)]
		p += int(keyLength)
	} else {
		key = nil
	}
	// value length
	varInt, n, err5 := extractSignedVarInt(data[p:])
	valueLength := int(varInt)
	p += n
	if err := cmp.Or(err1, err2, err3, err4, err5); err != nil {
		return nil, recordLength, err
	}
	// get headers before parsing the value (no headers for now so leave)
	headersCount, n, err := extractVarInt(data[p+valueLength:])
	p += n
	if err != nil {
		return nil, recordLength, err
	}
	if headersCount > 0 {
		return nil, recordLength, fmt.Errorf("Headers not supported yet")
	}

	// parse the value
	frameVersion := int8(data[p])
	p += 1
	recordType := RecordType(data[p])
	if recordType == RECORD_TYPE_UNKNOWN {
		return nil, recordLength, fmt.Errorf("Unknown record type")
	}
	p += 1
	version := int8(data[p])
	p += 1

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
		record, _, err = parseFeatureLevelRecord(data[p:], base)
	default:
		return nil, recordLength, fmt.Errorf("Record type not supported")
	}
	return record, recordLength, err
}

func parseFeatureLevelRecord(data []byte, base *RecordBase) (*FeatureLevelRecord, int, error) {
	p := 0
	nameLength, n, err := extractVarInt(data[p:])
	p += n
	if err != nil {
		return nil, p, err
	}
	name := data[p : p+int(nameLength)]
	p += int(nameLength)
	featureLevel := binary.BigEndian.Uint16(data[p : p+2])
	p += 2

	taggedFieldsCount, p, err := extractVarInt(data[p:])
	p += n
	if err != nil {
		return nil, p, err
	}
	if taggedFieldsCount > 0 {
		return nil, p, fmt.Errorf("Tagged fields not supported yet")
	}

	featureLevelRecord := &FeatureLevelRecord{
		RecordBase:   *base,
		name:         name,
		featureLevel: featureLevel,
	}
	return featureLevelRecord, p, nil
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
			return 0, i, fmt.Errorf("Varint too long")
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

	return 0, consumed, fmt.Errorf("Incomplete varint")
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
