package main

import (
	"math"
	"testing"
)

func TestExtractVarInt(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		wantVal  uint64
		wantCons int
		wantErr  bool
	}{
		{"zero", []byte{0x00}, 0, 1, false},
		{"one", []byte{0x01}, 1, 1, false},
		{"maxSingleByte", []byte{0x7f}, 127, 1, false},
		{"128", []byte{0x80, 0x01}, 128, 2, false},
		{"300", []byte{0xAC, 0x02}, 300, 2, false},
		{"150", []byte{0x96, 0x01}, 150, 2, false},
		{"incomplete", []byte{0x80}, 0, 1, true},
		{"maxUint64", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, math.MaxUint64, 10, false},
		// too long: 11 bytes with continuation still set
		{"tooLong", []byte{0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x00}, 0, 10, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, consumed, err := extractVarInt(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (val=%d consumed=%d)", val, consumed)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if consumed != tc.wantCons {
				t.Fatalf("consumed mismatch: got %d want %d", consumed, tc.wantCons)
			}
			if val != tc.wantVal {
				t.Fatalf("value mismatch: got %d want %d", val, tc.wantVal)
			}
		})
	}
}

func TestExtractSignedVarInt(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		wantVal  int64
		wantCons int
		wantErr  bool
	}{
		{"zero", []byte{0x00}, 0, 1, false},
		{"minus1", []byte{0x01}, -1, 1, false},
		{"plus1", []byte{0x02}, 1, 1, false},
		{"minus2", []byte{0x03}, -2, 1, false},
		{"plus2", []byte{0x04}, 2, 1, false},
		{"minus32", []byte{0x3F}, -32, 1, false},
		// -64 -> 127 (0x7F)
		{"minus64", []byte{0x7F}, -64, 1, false},
		// +32 -> 64 (0x40)
		{"plus32", []byte{0x40}, 32, 1, false},
		// +64 -> 128 -> varint 0x80 0x01
		{"plus64", []byte{0x80, 0x01}, 64, 2, false},
		// -65 -> 129 -> 0x81 0x01
		{"minus65", []byte{0x81, 0x01}, -65, 2, false},
		// +300 -> zigzag 600 -> bytes 0x98 0x25
		{"plus300", []byte{0xD8, 0x04}, 300, 2, false},
		// -300 -> zigzag 599 -> bytes 0x97 0x25
		{"minus300", []byte{0xD7, 0x04}, -300, 2, false},
		{"incomplete", []byte{0x80}, 0, 1, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			val, consumed, err := extractSignedVarInt(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (val=%d consumed=%d)", val, consumed)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if consumed != tc.wantCons {
				t.Fatalf("consumed mismatch: got %d want %d", consumed, tc.wantCons)
			}
			if val != tc.wantVal {
				t.Fatalf("value mismatch: got %d want %d", val, tc.wantVal)
			}
		})
	}
}
