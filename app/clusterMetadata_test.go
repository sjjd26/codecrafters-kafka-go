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
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil (val=%d consumed=%d)", val, consumed)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tc.wantErr {
				if val != tc.wantVal {
					t.Fatalf("value mismatch: got %d want %d", val, tc.wantVal)
				}
				if consumed != tc.wantCons {
					t.Fatalf("consumed mismatch: got %d want %d", consumed, tc.wantCons)
				}
			}
		})
	}
}
