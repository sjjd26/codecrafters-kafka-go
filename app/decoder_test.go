package main

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"
)

func TestDecoder_Int8(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    int8
		wantErr bool
	}{
		{name: "valid positive", input: []byte{0x7F}, want: 127, wantErr: false},
		{name: "valid negative", input: []byte{0x80}, want: -128, wantErr: false},
		{name: "empty", input: []byte{}, want: 0, wantErr: true},
		{name: "valid zero", input: []byte{0x00}, want: 0, wantErr: false},
		{name: "valid with extra", input: []byte{0x01, 0x02}, want: 1, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoder := NewDecoder(bytes.NewReader(tt.input))
			got, err := decoder.Int8()
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
			// ensure position is advanced by 1
			if decoder.pos != 1 {
				t.Errorf("expected position 1, got %d", decoder.pos)
			}
		})
	}
}

func TestDecoder_Int16(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    int16
		wantErr bool
	}{
		{name: "valid positive", input: []byte{0x7f, 0xff}, want: 32767, wantErr: false},
		{name: "valid negative", input: []byte{0x80, 0x00}, want: -32768, wantErr: false},
		{name: "zero", input: []byte{0x00, 0x00}, want: 0, wantErr: false},
		{name: "empty", input: []byte{}, want: 0, wantErr: true},
		{name: "short 1 byte", input: []byte{0x01}, want: 0, wantErr: true},
		{name: "with extra", input: []byte{0x00, 0x01, 0xFF}, want: 1, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(tt.input))
			got, err := d.Int16()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
			if d.pos != 2 {
				t.Fatalf("expected position 2, got %d", d.pos)
			}
		})
	}
}

func TestDecoder_Int32(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    int32
		wantErr bool
	}{
		{name: "valid positive", input: []byte{0x7f, 0xff, 0xff, 0xff}, want: 2147483647, wantErr: false},
		{name: "valid negative", input: []byte{0x80, 0x00, 0x00, 0x00}, want: -2147483648, wantErr: false},
		{name: "zero", input: []byte{0x00, 0x00, 0x00, 0x00}, want: 0, wantErr: false},
		{name: "empty", input: []byte{}, want: 0, wantErr: true},
		{name: "short 3 bytes", input: []byte{0x00, 0x00, 0x01}, want: 0, wantErr: true},
		{name: "with extra", input: []byte{0x00, 0x00, 0x00, 0x02, 0xAA}, want: 2, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(tt.input))
			got, err := d.Int32()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
			if d.pos != 4 {
				t.Fatalf("expected position 4, got %d", d.pos)
			}
		})
	}
}

func TestDecoder_Int64(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    int64
		wantErr bool
	}{
		{name: "valid positive", input: []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, want: math.MaxInt64, wantErr: false},
		{name: "valid negative", input: []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, want: math.MinInt64, wantErr: false},
		{name: "zero", input: []byte{0, 0, 0, 0, 0, 0, 0, 0}, want: 0, wantErr: false},
		{name: "empty", input: []byte{}, want: 0, wantErr: true},
		{name: "short 7 bytes", input: []byte{0, 0, 0, 0, 0, 0, 0}, want: 0, wantErr: true},
		{name: "with extra", input: []byte{0, 0, 0, 0, 0, 0, 0, 3, 0xAA}, want: 3, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(tt.input))
			got, err := d.Int64()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
			if d.pos != 8 {
				t.Fatalf("expected position 8, got %d", d.pos)
			}
		})
	}
}

func TestDecoder_UUID(t *testing.T) {
	tests := []struct {
		name       string
		input      []byte
		want       uuid
		wantErr    bool
		wantPos    int
		expectRead bool
	}{
		{
			name:  "valid",
			input: []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
			want: func() (u uuid) {
				copy(u[:], []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f})
				return
			}(),
			wantErr: false,
			wantPos: 16,
		},
		{
			name:    "empty",
			input:   []byte{},
			wantErr: true,
			wantPos: 0,
		},
		{
			name:    "short",
			input:   []byte{0x01, 0x02},
			wantErr: true,
			wantPos: 0,
		},
		{
			name:  "with extra",
			input: append(bytes.Repeat([]byte{0xAB}, 16), 0xCD),
			want: func() (u uuid) {
				copy(u[:], bytes.Repeat([]byte{0xAB}, 16))
				return
			}(),
			wantErr: false,
			wantPos: 16,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(tt.input))
			got, err := d.UUID()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("value mismatch: got %v, want %v", got, tt.want)
			}
			if d.pos != tt.wantPos {
				t.Fatalf("expected position %d, got %d", tt.wantPos, d.pos)
			}
		})
	}
}

func TestDecoder_UVarint(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantVal uint64
		wantPos int
		wantErr bool
	}{
		{"zero", []byte{0x00}, 0, 1, false},
		{"one", []byte{0x01}, 1, 1, false},
		{"maxSingleByte", []byte{0x7f}, 127, 1, false},
		{"128", []byte{0x80, 0x01}, 128, 2, false},
		{"300", []byte{0xac, 0x02}, 300, 2, false},
		{"150", []byte{0x96, 0x01}, 150, 2, false},
		{"incomplete", []byte{0x80}, 0, 1, true},
		{"maxUint64", []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, math.MaxUint64, 10, false},
		{"tooLong", []byte{0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x00}, 0, 10, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(tc.input))
			val, err := d.UVarint()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (val=%d pos=%d)", val, d.pos)
				}
				if d.pos != tc.wantPos {
					t.Fatalf("pos mismatch: got %d want %d", d.pos, tc.wantPos)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != tc.wantVal {
				t.Fatalf("value mismatch: got %d want %d", val, tc.wantVal)
			}
			if d.pos != tc.wantPos {
				t.Fatalf("pos mismatch: got %d want %d", d.pos, tc.wantPos)
			}
		})
	}
}

func TestDecoder_Varint(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantVal int64
		wantPos int
		wantErr bool
	}{
		{"zero", []byte{0x00}, 0, 1, false},
		{"minus1", []byte{0x01}, -1, 1, false},
		{"plus1", []byte{0x02}, 1, 1, false},
		{"minus2", []byte{0x03}, -2, 1, false},
		{"plus2", []byte{0x04}, 2, 1, false},
		{"minus32", []byte{0x3f}, -32, 1, false},
		{"minus64", []byte{0x7f}, -64, 1, false},
		{"plus32", []byte{0x40}, 32, 1, false},
		{"plus64", []byte{0x80, 0x01}, 64, 2, false},
		{"minus65", []byte{0x81, 0x01}, -65, 2, false},
		{"plus300", []byte{0xd8, 0x04}, 300, 2, false},
		{"minus300", []byte{0xd7, 0x04}, -300, 2, false},
		{"incomplete", []byte{0x80}, 0, 1, true},
		{"tooLong", []byte{0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x81, 0x00}, 0, 10, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(tc.input))
			val, err := d.Varint()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (val=%d pos=%d)", val, d.pos)
				}
				if d.pos != tc.wantPos {
					t.Fatalf("pos mismatch: got %d want %d", d.pos, tc.wantPos)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != tc.wantVal {
				t.Fatalf("value mismatch: got %d want %d", val, tc.wantVal)
			}
			if d.pos != tc.wantPos {
				t.Fatalf("pos mismatch: got %d want %d", d.pos, tc.wantPos)
			}
		})
	}
}

func TestDecoder_ByteArray(t *testing.T) {
	makePayload := func(n int) []byte {
		b := make([]byte, n)
		for i := range n {
			b[i] = byte(i % 256)
		}
		return b
	}

	tests := []struct {
		name     string
		lengthBE []byte // 4 bytes, big-endian int32 (raw before -1 inside method)
		payload  []byte
		want     []byte
		wantPos  int
		wantErr  bool
		checkEOF bool // if true, expect io.ErrUnexpectedEOF
	}{
		{
			name:     "length 1 -> effective 0, returns nil",
			lengthBE: []byte{0x00, 0x00, 0x00, 0x01},
			payload:  nil,
			want:     nil,
			wantPos:  4,
			wantErr:  false,
		},
		{
			name:     "length 0 -> effective -1, returns nil",
			lengthBE: []byte{0x00, 0x00, 0x00, 0x00},
			payload:  nil,
			want:     nil,
			wantPos:  4,
			wantErr:  false,
		},
		{
			name:     "negative length -> effective <=0, returns nil",
			lengthBE: []byte{0xff, 0xff, 0xff, 0xfb}, // -5
			payload:  nil,
			want:     nil,
			wantPos:  4,
			wantErr:  false,
		},
		{
			name:     "reads payload",
			lengthBE: []byte{0x00, 0x00, 0x00, 0x05}, // effective 4
			payload:  []byte{1, 2, 3, 4},
			want:     []byte{1, 2, 3, 4},
			wantPos:  8,
			wantErr:  false,
		},
		{
			name:     "insufficient header",
			lengthBE: []byte{0x00, 0x00, 0x00}, // only 3 bytes
			payload:  nil,
			want:     nil,
			wantPos:  3, // 3 bytes read before EOF
			wantErr:  true,
		},
		{
			name:     "insufficient payload",
			lengthBE: []byte{0x00, 0x00, 0x00, 0x05}, // effective 4
			payload:  []byte{1, 2},                   // only 2 bytes available
			want:     nil,
			wantPos:  6, // 4 (len) + 2 (read before EOF)
			wantErr:  true,
			checkEOF: true,
		},
		{
			name:     "grows internal buffer (effective > 1024)",
			lengthBE: []byte{0x00, 0x00, 0x04, 0x02}, // 1026 -> effective 1025
			payload:  makePayload(1025),
			want:     makePayload(1025),
			wantPos:  4 + 1025,
			wantErr:  false,
		},
		{
			name:     "reads payload with extra trailing bytes",
			lengthBE: []byte{0x00, 0x00, 0x00, 0x03}, // effective 2
			payload:  []byte{9, 8, 7},                // 1 extra
			want:     []byte{9, 8},
			wantPos:  6, // 4 + 2 consumed; extra 1 remains unread
			wantErr:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stream := append(append([]byte{}, tc.lengthBE...), tc.payload...)
			d := NewDecoder(bytes.NewReader(stream))
			got, err := d.ByteArray()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (pos=%d)", d.pos)
				}
				if tc.checkEOF && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("expected io.ErrUnexpectedEOF, got %s", err)
				}
				if d.pos != tc.wantPos {
					t.Fatalf("pos mismatch: got %d want %d", d.pos, tc.wantPos)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("payload mismatch")
			}
			if d.pos != tc.wantPos {
				t.Fatalf("pos mismatch: got %d want %d", d.pos, tc.wantPos)
			}
		})
	}
}

// small helper to avoid importing errors in multiple places
func errorsIs(err, target error) bool {
	return err != nil && target != nil && (err == target || (err.Error() == target.Error()))
}
