package main

import "os"

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
