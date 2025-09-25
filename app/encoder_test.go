package main

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestEncoder_Int8(t *testing.T) {
	tests := []struct {
		name string
		val  int8
		exp  []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"positive", 127, []byte{0x7f}},
		{"negative", -128, []byte{0x80}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEncoder(0)
			e.Int8(tt.val)
			if !bytes.Equal(e.Bytes(), tt.exp) {
				t.Fatalf("got %x, want %x", e.Bytes(), tt.exp)
			}
		})
	}
}

func TestEncoder_Int16(t *testing.T) {
	tests := []struct {
		name string
		val  int16
		exp  []byte
	}{
		{"zero", 0, []byte{0x00, 0x00}},
		{"one", 1, []byte{0x00, 0x01}},
		{"minus1", -1, []byte{0xff, 0xff}},
		{"min", math.MinInt16, []byte{0x80, 0x00}},
		{"max", math.MaxInt16, []byte{0x7f, 0xff}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEncoder(0)
			e.Int16(tt.val)
			if !bytes.Equal(e.Bytes(), tt.exp) {
				t.Fatalf("got %x, want %x", e.Bytes(), tt.exp)
			}
		})
	}
}

func TestEncoder_Int32(t *testing.T) {
	tests := []struct {
		name string
		val  int32
		exp  []byte
	}{
		{"zero", 0, []byte{0x00, 0x00, 0x00, 0x00}},
		{"one", 1, []byte{0x00, 0x00, 0x00, 0x01}},
		{"minus1", -1, []byte{0xff, 0xff, 0xff, 0xff}},
		{"min", math.MinInt32, []byte{0x80, 0x00, 0x00, 0x00}},
		{"max", math.MaxInt32, []byte{0x7f, 0xff, 0xff, 0xff}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEncoder(0)
			e.Int32(tt.val)
			if !bytes.Equal(e.Bytes(), tt.exp) {
				t.Fatalf("got %x, want %x", e.Bytes(), tt.exp)
			}
		})
	}
}

func TestEncoder_Int64(t *testing.T) {
	tests := []struct {
		name string
		val  int64
		exp  []byte
	}{
		{"zero", 0, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{"one", 1, []byte{0, 0, 0, 0, 0, 0, 0, 1}},
		{"minus1", -1, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{"min", math.MinInt64, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{"max", math.MaxInt64, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEncoder(0)
			e.Int64(tt.val)
			if !bytes.Equal(e.Bytes(), tt.exp) {
				t.Fatalf("got %x, want %x", e.Bytes(), tt.exp)
			}
		})
	}
}

func TestEncoder_UVarint(t *testing.T) {
	tests := []struct {
		name string
		val  uint64
		exp  []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"one", 1, []byte{0x01}},
		{"maxSingleByte", 127, []byte{0x7f}},
		{"128", 128, []byte{0x80, 0x01}},
		{"300", 300, []byte{0xac, 0x02}},
		{"150", 150, []byte{0x96, 0x01}},
		{"maxUint64", math.MaxUint64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewEncoder(0)
			e.UVarint(tt.val)
			if !bytes.Equal(e.Bytes(), tt.exp) {
				t.Fatalf("got %x, want %x", e.Bytes(), tt.exp)
			}
			// cross-check with binary.PutUvarint
			var buf [binary.MaxVarintLen64]byte
			n := binary.PutUvarint(buf[:], tt.val)
			if !bytes.Equal(e.Bytes(), buf[:n]) {
				t.Fatalf("mismatch with binary.PutUvarint: got %x want %x", e.Bytes(), buf[:n])
			}
		})
	}
}

func TestEncoder_UUID(t *testing.T) {
	u := makeUUID(0x10)
	e := NewEncoder(0)
	e.UUID(u)
	if !bytes.Equal(e.Bytes(), u[:]) {
		t.Fatalf("got %x, want %x", e.Bytes(), u[:])
	}
}

func TestEncoder_WriteBytes(t *testing.T) {
	e := NewEncoder(0)
	e.WriteBytes([]byte{1, 2, 3})
	e.WriteBytes([]byte{4})
	want := []byte{1, 2, 3, 4}
	if !bytes.Equal(e.Bytes(), want) {
		t.Fatalf("got %x, want %x", e.Bytes(), want)
	}
}

func TestEncoder_Reset(t *testing.T) {
	e := NewEncoder(0)
	e.Int32(0x01020304)
	if len(e.Bytes()) == 0 {
		t.Fatalf("expected bytes written")
	}
	e.Reset()
	if len(e.Bytes()) != 0 {
		t.Fatalf("expected buffer cleared, got %x", e.Bytes())
	}
	e.Int16(0x0a0b)
	want := []byte{0x0a, 0x0b}
	if !bytes.Equal(e.Bytes(), want) {
		t.Fatalf("got %x, want %x", e.Bytes(), want)
	}
}

func TestEncoder_ComposedSequence(t *testing.T) {
	e := NewEncoder(0)
	// Compose: Int16(0x1234), Int32(-1), UVarint(300), UUID(0x20..), Int8(-1)
	e.Int16(0x1234)
	e.Int32(-1)
	e.UVarint(300)
	u := makeUUID(0x20)
	e.UUID(u)
	e.Int8(-1)

	var want []byte
	want = binary.BigEndian.AppendUint16(want, 0x1234)
	want = binary.BigEndian.AppendUint32(want, uint32(^uint32(0)))
	want = append(want, 0xac, 0x02)
	want = append(want, u[:]...)
	want = append(want, 0xff)

	if !bytes.Equal(e.Bytes(), want) {
		t.Fatalf("composed mismatch:\n got  %x\n want %x", e.Bytes(), want)
	}
}
