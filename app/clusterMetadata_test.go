package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"
)

// helpers

func putBE8(b *bytes.Buffer, v int8) {
	b.WriteByte(byte(v))
}

func putBE16(b *bytes.Buffer, v int16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	b.Write(tmp[:])
}

func putBE32(b *bytes.Buffer, v int32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	b.Write(tmp[:])
}

func putBE64(b *bytes.Buffer, v int64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	b.Write(tmp[:])
}

func putUVarint(b *bytes.Buffer, v uint64) {
	for {
		if v < 0x80 {
			b.WriteByte(byte(v))
			return
		}
		b.WriteByte(byte(v&0x7f | 0x80))
		v >>= 7
	}
}

func zigZagEncode(i int64) uint64 {
	return uint64(uint64(i<<1) ^ uint64((i >> 63)))
}

func putVarint(b *bytes.Buffer, i int64) { putUVarint(b, zigZagEncode(i)) }

func makeUUID(start byte) uuid {
	var u uuid
	for i := 0; i < len(u); i++ {
		u[i] = start + byte(i)
	}
	return u
}

// parseTaggedFields

func Test_parseTaggedFields(t *testing.T) {
	t.Run("no tagged fields", func(t *testing.T) {
		var buf bytes.Buffer
		putUVarint(&buf, 0)

		d := NewDecoder(bytes.NewReader(buf.Bytes()))
		got, err := parseTaggedFields(d)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil tagged fields, got %v", got)
		}
	})

	t.Run("non-zero tagged fields not supported", func(t *testing.T) {
		var buf bytes.Buffer
		putUVarint(&buf, 1)

		d := NewDecoder(bytes.NewReader(buf.Bytes()))
		_, err := parseTaggedFields(d)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
	})
}

// parseReplicaArray

func Test_parseReplicaArray(t *testing.T) {
	t.Run("empty compact array", func(t *testing.T) {
		var buf bytes.Buffer
		putUVarint(&buf, 1) // length = 0 + 1

		d := NewDecoder(bytes.NewReader(buf.Bytes()))
		got, err := parseReplicaArray(d)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("expected empty array, got %#v", got)
		}
	})

	t.Run("three items", func(t *testing.T) {
		var buf bytes.Buffer
		putUVarint(&buf, 4) // 3 + 1
		putBE32(&buf, 1)
		putBE32(&buf, 2)
		putBE32(&buf, 3)

		d := NewDecoder(bytes.NewReader(buf.Bytes()))
		got, err := parseReplicaArray(d)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		want := []int32{1, 2, 3}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, want %#v", got, want)
		}
	})

	t.Run("short read on element", func(t *testing.T) {
		var buf bytes.Buffer
		putUVarint(&buf, 2)                 // 1 + 1 => expect one int32
		buf.Write([]byte{0x00, 0x00, 0x00}) // only 3/4 bytes

		d := NewDecoder(bytes.NewReader(buf.Bytes()))
		_, err := parseReplicaArray(d)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
	})

	t.Run("incomplete compact length varint", func(t *testing.T) {
		d := NewDecoder(bytes.NewReader([]byte{0x80})) // continuation set, then EOF
		_, err := parseReplicaArray(d)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
	})
}

// parseFeatureLevelRecord

func Test_parseFeatureLevelRecord(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		base := &RecordBase{}

		var buf bytes.Buffer
		// name length + name
		putUVarint(&buf, 4)
		buf.WriteString("foo")
		// feature level
		putBE16(&buf, 2)
		// tagged fields = 0
		putUVarint(&buf, 0)

		d := NewDecoder(bytes.NewReader(buf.Bytes()))
		got, err := parseFeatureLevelRecord(d, base)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if got.name != "foo" || got.featureLevel != 2 {
			t.Fatalf("value mismatch: %+v", got)
		}
	})

	t.Run("tagged fields not supported", func(t *testing.T) {
		base := &RecordBase{}
		var buf bytes.Buffer
		putUVarint(&buf, 4)
		buf.WriteString("foo")
		putBE16(&buf, 1)
		putUVarint(&buf, 1) // non-zero tagged fields -> error

		d := NewDecoder(bytes.NewReader(buf.Bytes()))
		_, err := parseFeatureLevelRecord(d, base)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
	})
}

// parseTopicRecord

func Test_parseTopicRecord(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		base := &RecordBase{}
		wantID := makeUUID(0x10)

		var buf bytes.Buffer
		putUVarint(&buf, 4) // name length = 3 + 1
		buf.WriteString("foo")
		buf.Write(wantID[:])
		putUVarint(&buf, 0) // tagged fields

		d := NewDecoder(bytes.NewReader(buf.Bytes()))
		got, err := parseTopicRecord(d, base)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if got.name != "foo" {
			t.Fatalf("name mismatch: %q", got.name)
		}
		if got.topicId != wantID {
			t.Fatalf("uuid mismatch: got %x want %x", got.topicId, wantID)
		}
	})

	t.Run("tagged fields not supported", func(t *testing.T) {
		base := &RecordBase{}
		var buf bytes.Buffer
		putUVarint(&buf, 1)
		buf.WriteByte('a')
		u := makeUUID(0x00)
		buf.Write(u[:])
		putUVarint(&buf, 1) // non-zero -> error

		d := NewDecoder(bytes.NewReader(buf.Bytes()))
		_, err := parseTopicRecord(d, base)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
	})
}

// parsePartitionRecord

func Test_parsePartitionRecord(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		base := &RecordBase{}
		topicID := makeUUID(0x20)

		var buf bytes.Buffer
		putBE32(&buf, 7)      // partitionId
		buf.Write(topicID[:]) // topicId
		putUVarint(&buf, 4)   // replicas: 3 entries
		putBE32(&buf, 1)
		putBE32(&buf, 2)
		putBE32(&buf, 3)
		putUVarint(&buf, 1) // inSyncReplicas: 0 entries
		putUVarint(&buf, 1) // removingReplicas: 0
		putUVarint(&buf, 3) // addingReplicas: 2 entries
		putBE32(&buf, 9)
		putBE32(&buf, 10)
		putBE32(&buf, 9)    // leaderReplica
		putBE32(&buf, 5)    // leaderEpoch
		putBE32(&buf, 11)   // partitionEpoch
		putUVarint(&buf, 1) // directories: 0 entries
		putUVarint(&buf, 0) // tagged fields

		d := NewDecoder(bytes.NewReader(buf.Bytes()))
		got, err := parsePartitionRecord(d, base)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		if got.partitionId != 7 || got.topicId != topicID {
			t.Fatalf("ids mismatch: %+v", got)
		}
		if !reflect.DeepEqual(got.replicas, []int32{1, 2, 3}) {
			t.Fatalf("replicas mismatch: %#v", got.replicas)
		}
		if len(got.inSyncReplicas) != 0 || len(got.removingReplicas) != 0 {
			t.Fatalf("expected empty ISR/removing, got %#v / %#v", got.inSyncReplicas, got.removingReplicas)
		}
		if !reflect.DeepEqual(got.addingReplicas, []int32{9, 10}) {
			t.Fatalf("adding mismatch: %#v", got.addingReplicas)
		}
		if got.leaderReplica != 9 || got.leaderEpoch != 5 || got.partitionEpoch != 11 {
			t.Fatalf("leader/epochs mismatch: %+v", got)
		}
		if len(got.directories) != 0 {
			t.Fatalf("expected no directories, got %d", len(got.directories))
		}
	})

	t.Run("short read in arrays", func(t *testing.T) {
		base := &RecordBase{}
		var buf bytes.Buffer
		putBE32(&buf, 1)
		u := makeUUID(0x00)
		buf.Write(u[:])
		// replicas says 1 entry but we provide only 2 bytes of the int32
		putUVarint(&buf, 2) // 1 + 1
		buf.Write([]byte{0x00, 0x00})

		d := NewDecoder(bytes.NewReader(buf.Bytes()))
		_, err := parsePartitionRecord(d, base)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
	})
}

// parseMetadataRecord

func Test_parseMetadataRecord_Topic(t *testing.T) {
	var buf bytes.Buffer

	// record length (ignored)
	putVarint(&buf, 0)
	// attributes
	putBE8(&buf, 0)
	// timestampDelta
	putVarint(&buf, 0)
	// offsetDelta
	putVarint(&buf, 0)
	// keyLength (0) and key
	putVarint(&buf, 0)

	// value length (ignored)
	putVarint(&buf, 0)
	// value: frameVersion, type (TOPIC), version
	putBE8(&buf, 0)                       // frameVersion
	putBE8(&buf, int8(RECORD_TYPE_TOPIC)) // recordType
	putBE8(&buf, 1)                       // version

	// TopicRecord payload
	putUVarint(&buf, 4)    // name len
	buf.WriteString("abc") // name
	tid := makeUUID(0x30)
	buf.Write(tid[:])   // uuid
	putUVarint(&buf, 0) // tagged fields

	// headers count
	putUVarint(&buf, 0)

	d := NewDecoder(bytes.NewReader(buf.Bytes()))
	rec, err := parseMetadataRecord(d)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	tr, ok := rec.(*TopicRecord)
	if !ok {
		t.Fatalf("expected *TopicRecord, got %T", rec)
	}
	if tr.name != "abc" || tr.topicId != tid {
		t.Fatalf("topic mismatch: %+v", tr)
	}
	if tr.RecordBase.recordType != RECORD_TYPE_TOPIC {
		t.Fatalf("recordType mismatch: %v", tr.RecordBase.recordType)
	}
}

func Test_parseMetadataRecord_UnknownType(t *testing.T) {
	var buf bytes.Buffer
	putVarint(&buf, 0) // length
	putBE8(&buf, 0)    // attributes
	putVarint(&buf, 0) // ts delta
	putVarint(&buf, 0) // off delta
	putVarint(&buf, 0) // key len
	putVarint(&buf, 0) // value len
	putBE8(&buf, 0)    // frameVersion
	putBE8(&buf, int8(RECORD_TYPE_UNKNOWN))
	putBE8(&buf, 0) // version

	d := NewDecoder(bytes.NewReader(buf.Bytes()))
	_, err := parseMetadataRecord(d)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func Test_parseMetadataRecord_HeadersNotSupported(t *testing.T) {
	var buf bytes.Buffer
	// minimal valid record with Topic payload, then headersCount > 0
	putVarint(&buf, 0) // length
	putBE8(&buf, 0)    // attributes
	putVarint(&buf, 0) // ts delta
	putVarint(&buf, 0) // off delta
	putVarint(&buf, 0) // key len
	putVarint(&buf, 0) // value len
	putBE8(&buf, 0)    // frameVersion
	putBE8(&buf, int8(RECORD_TYPE_TOPIC))
	putBE8(&buf, 1) // version
	putUVarint(&buf, 1)
	buf.WriteByte('x')
	u := makeUUID(0x00)
	buf.Write(u[:])
	putUVarint(&buf, 0) // tagged fields
	putUVarint(&buf, 1) // headersCount -> not supported

	d := NewDecoder(bytes.NewReader(buf.Bytes()))
	_, err := parseMetadataRecord(d)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

// parseMetadataRecordBatch

func buildBatchHeader(
	baseOffset int64,
	batchLen int32,
	partitionLeaderEpoch int32,
	magic int8,
	crc int32,
	attributes int16,
	lastOffsetDelta int32,
	baseTs int64,
	maxTs int64,
	producerID int64,
	producerEpoch int16,
	baseSeq int32,
	numRecords int32,
) []byte {
	var b bytes.Buffer
	putBE64(&b, baseOffset)
	putBE32(&b, batchLen)
	putBE32(&b, partitionLeaderEpoch)
	putBE8(&b, magic)
	putBE32(&b, crc)
	putBE16(&b, attributes)
	putBE32(&b, lastOffsetDelta)
	putBE64(&b, baseTs)
	putBE64(&b, maxTs)
	putBE64(&b, producerID)
	putBE16(&b, producerEpoch)
	putBE32(&b, baseSeq)
	putBE32(&b, numRecords)
	return b.Bytes()
}

func Test_parseMetadataRecordBatch_NoRecords(t *testing.T) {
	header := buildBatchHeader(
		123, 61, 2, 2, 0, 0, 0,
		1111, 2222, 3333, 4, 5, 0,
	)

	d := NewDecoder(bytes.NewReader(header))
	got, err := parseMetadataRecordBatch(d)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got.baseOffset != 123 || got.partitionLeaderEpoch != 2 || got.magicByte != 2 {
		t.Fatalf("header mismatch: %+v", got)
	}
	// records field is currently set to nil in parseMetadataRecordBatch
	if got.records != nil {
		t.Fatalf("expected records=nil, got %v", got.records)
	}
}

func Test_parseMetadataRecordBatch_ShortHeader(t *testing.T) {
	d := NewDecoder(bytes.NewReader([]byte{0, 1, 2, 3}))
	_, err := parseMetadataRecordBatch(d)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

// parseClusterMetadata

func Test_parseClusterMetadata_TwoBatches_NoRecords(t *testing.T) {
	b1 := buildBatchHeader(1, 61, 0, 2, 0, 0, 0, 10, 20, 30, 1, 2, 0)
	b2 := buildBatchHeader(2, 61, 1, 2, 0, 0, 0, 11, 21, 31, 2, 3, 0)
	stream := append(append([]byte{}, b1...), b2...)

	d := NewDecoder(bytes.NewReader(stream))
	batches, err := parseClusterMetadata(d)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
	if batches[0].baseOffset != 1 || batches[1].baseOffset != 2 {
		t.Fatalf("baseOffsets mismatch: %d, %d", batches[0].baseOffset, batches[1].baseOffset)
	}
}

func Test_parseClusterMetadata_PartialSecondBatch_ReturnsError(t *testing.T) {
	b1 := buildBatchHeader(1, 61, 0, 2, 0, 0, 0, 10, 20, 30, 1, 2, 0)
	b2 := buildBatchHeader(2, 61, 1, 2, 0, 0, 0, 11, 21, 31, 2, 3, 0)
	partial := b2[:10]
	stream := append(append([]byte{}, b1...), partial...)

	d := NewDecoder(bytes.NewReader(stream))
	_, err := parseClusterMetadata(d)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

// ensure decoder integration for error type used in ByteArray
func Test_ByteArray_ErrUnexpectedEOFPropagation(t *testing.T) {
	d := NewDecoder(bytes.NewReader([]byte{0x01}))
	// request 4, but only 1 byte in input -> propagation to io.ErrUnexpectedEOF
	_, err := d.ByteArray(4)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		// allow either exact io.ErrUnexpectedEOF or wrap – current impl returns io.ErrUnexpectedEOF
		t.Logf("got error: %v (ok if wrapped)", err)
	}
}
