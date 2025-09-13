package main

import (
	"fmt"
	"os"
)

const CLUSTER_METADATA_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

type MetadataRecord struct {
	timestampDelta int
	offsetDelta    int
	key            []byte
	frameVersion   int
	recordType     int
	version        int
}

func retrieveClusterMetadata() (*MetadataRecord, error) {
	file, err := os.Open(CLUSTER_METADATA_PATH)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return nil, nil
}

// func parseClusterMetadataFile(file os.File) ([]byte, error) {
// 	readBuf := make([]byte, 1024)
// 	n, err := file.Read(readBuf)
// 	if err != nil {
// 		return nil, fmt.Errorf("Error reading metadata file: %w", err)
// 	}

// 	// current point in file contents
// 	p := 0
// 	for p < n {
// 		// parse a record batch
// 		baseOffset := binary.BigEndian.Uint64(readBuf[p : p+8])
// 		p += 8
// 		batchLength := binary.BigEndian.Uint32(readBuf[p : p+4])
// 		p += 4
// 		partitionLeaderEpoch := binary.BigEndian.Uint32(readBuf[p : p+4])
// 		p += 4
// 		magicByte := readBuf[p]
// 		p += 1
// 		CRC := binary.BigEndian.Uint32(readBuf[p : p+4])
// 		p += 4
// 		attributes := binary.BigEndian.Uint16(readBuf[p : p+2])
// 		p += 2
// 		lastOffsetDelta := binary.BigEndian.Uint32(readBuf[p : p+4])
// 		p += 4
// 		baseTimestamp := binary.BigEndian.Uint64(readBuf[p : p+8])
// 		p += 8
// 		maxTimestamp := binary.BigEndian.Uint64(readBuf[p : p+8])
// 		p += 8
// 		producerID := binary.BigEndian.Uint64(readBuf[p : p+8])
// 		p += 8
// 		producerEpoch := binary.BigEndian.Uint16(readBuf[p : p+2])
// 		p += 2
// 		baseSequence := binary.BigEndian.Uint32(readBuf[p : p+4])
// 		p += 4
// 		numRecords := binary.BigEndian.Uint32(readBuf[p : p+4])
// 		p += 4

// 		// parse records in the batch
// 		for i := uint32(0); i < numRecords; i++ {
// 			recordLength := binary.BigEndian.Uint32(readBuf[p : p+4])
// 			p += 4
// 			attributes := readBuf[p]
// 			p += 1
// 			timestampDelta := binary.BigEndian.Uint32(readBuf[p : p+4])
// 			p += 4
// 			offsetDelta := binary.BigEndian.Uint32(readBuf[p : p+4])
// 			p += 4
// 			keyLength := binary.BigEndian.Uint32(readBuf[p : p+4])
// 			p += 4
// 			var key []byte

// 		}
// 	}

// 	return nil, nil
// }

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
