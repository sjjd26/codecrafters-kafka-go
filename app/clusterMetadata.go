package main

import (
	"encoding/binary"
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

func parseClusterMetadataFile(file os.File) ([]byte, error) {
	readBuf := make([]byte, 1024)
	n, err := file.Read(readBuf)
	if err != nil {
		return nil, fmt.Errorf("Error reading metadata file: %w", err)
	}

	// current point in file contents
	p := 0
	for p < n {
		// parse a record batch
		baseOffset := binary.BigEndian.Uint64(readBuf[p : p+8])
		p += 8
		batchLength := binary.BigEndian.Uint32(readBuf[p : p+4])
		p += 4
		partitionLeaderEpoch := binary.BigEndian.Uint32(readBuf[p : p+4])
		p += 4
		magicByte := readBuf[p]
		p += 1
		CRC := binary.BigEndian.Uint32(readBuf[p : p+4])
		p += 4
		attributes := binary.BigEndian.Uint16(readBuf[p : p+2])
		p += 2
		lastOffsetDelta := binary.BigEndian.Uint32(readBuf[p : p+4])
		p += 4
		baseTimestamp := binary.BigEndian.Uint64(readBuf[p : p+8])
		p += 8
		maxTimestamp := binary.BigEndian.Uint64(readBuf[p : p+8])
		p += 8
		producerID := binary.BigEndian.Uint64(readBuf[p : p+8])
		p += 8
		producerEpoch := binary.BigEndian.Uint16(readBuf[p : p+2])
		p += 2
		baseSequence := binary.BigEndian.Uint32(readBuf[p : p+4])
		p += 4
		numRecords := binary.BigEndian.Uint32(readBuf[p : p+4])
		p += 4

		// parse records in the batch
		for i := uint32(0); i < numRecords; i++ {
			recordLength := binary.BigEndian.Uint32(readBuf[p : p+4])
			p += 4
			attributes := readBuf[p]
			p += 1
			timestampDelta := binary.BigEndian.Uint32(readBuf[p : p+4])
			p += 4
			offsetDelta := binary.BigEndian.Uint32(readBuf[p : p+4])
			p += 4
			keyLength := binary.BigEndian.Uint32(readBuf[p : p+4])
			p += 4
			var key []byte

		}
	}

	return nil, nil
}

func extractVarInt(data []byte) (int, error) {
	continuationMask := 0b1000_0000
	var bytes []byte
	for i := 0; i < len(data); i++ {
		bytes = append(bytes, data[i]<<1)
	}

	return 0, nil
}
