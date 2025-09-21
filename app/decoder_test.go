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
		for i := 0; i < n; i++ {
			b[i] = byte(i % 256)
		}
		return b
	}

	tests := []struct {
		name     string
		length   int
		input    []byte
		want     []byte
		wantPos  int
		wantErr  bool
		checkEOF bool // expect io.ErrUnexpectedEOF
	}{
		{
			name:    "length 0 returns nil without reading",
			length:  0,
			input:   []byte{1, 2, 3},
			want:    nil,
			wantPos: 0,
			wantErr: false,
		},
		{
			name:    "negative length returns nil without reading",
			length:  -5,
			input:   []byte{1, 2, 3},
			want:    nil,
			wantPos: 0,
			wantErr: false,
		},
		{
			name:    "exact read",
			length:  4,
			input:   []byte{1, 2, 3, 4},
			want:    []byte{1, 2, 3, 4},
			wantPos: 4,
			wantErr: false,
		},
		{
			name:     "empty input with positive length -> unexpected EOF",
			length:   1,
			input:    []byte{},
			want:     nil,
			wantPos:  0, // nothing read
			wantErr:  true,
			checkEOF: true,
		},
		{
			name:     "short read -> unexpected EOF (partial read)",
			length:   4,
			input:    []byte{9, 8},
			want:     nil,
			wantPos:  2, // advanced by bytes actually read
			wantErr:  true,
			checkEOF: true,
		},
		{
			name:    "read with extra trailing bytes (not consumed)",
			length:  2,
			input:   []byte{9, 8, 7, 6},
			want:    []byte{9, 8},
			wantPos: 2,
			wantErr: false,
		},
		{
			name:    "grows internal buffer (>1024)",
			length:  1025,
			input:   makePayload(1025),
			want:    makePayload(1025),
			wantPos: 1025,
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(tc.input))
			got, err := d.ByteArray(tc.length)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (pos=%d)", d.pos)
				}
				if tc.checkEOF && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
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

func TestDecoder_NullableBytes(t *testing.T) {
	makePayload := func(n int) []byte {
		b := make([]byte, n)
		for i := 0; i < n; i++ {
			b[i] = byte(i % 256)
		}
		return b
	}

	type tc struct {
		name     string
		length   int16  // used when raw == nil
		payload  []byte // appended after the 2-byte length
		raw      []byte // if set, use this exact stream (no length prefix built)
		want     []byte
		wantPos  int
		wantErr  bool
		checkEOF bool // specifically expect io.ErrUnexpectedEOF
	}

	tests := []tc{
		{
			name:    "empty input -> error",
			raw:     []byte{},
			want:    nil,
			wantPos: 0,
			wantErr: true,
		},
		{
			name:    "short length header (1 byte) -> error",
			raw:     []byte{0x00},
			want:    nil,
			wantPos: 1, // Int16 advanced by 1 then errored
			wantErr: true,
		},
		{
			name:    "negative length returns nil without reading payload",
			length:  -1,
			payload: []byte{1, 2, 3},
			want:    nil,
			wantPos: 2, // only length consumed
			wantErr: false,
		},
		{
			name:    "zero length returns nil without reading payload",
			length:  0,
			payload: []byte{1, 2, 3},
			want:    nil,
			wantPos: 2, // only length consumed
			wantErr: false,
		},
		{
			name:    "positive length exact read",
			length:  4,
			payload: []byte{9, 8, 7, 6},
			want:    []byte{9, 8, 7, 6},
			wantPos: 6, // 2 (len) + 4 (payload)
			wantErr: false,
		},
		{
			name:     "positive length but no payload -> unexpected EOF",
			length:   3,
			payload:  []byte{},
			want:     nil,
			wantPos:  2, // only the length was consumed
			wantErr:  true,
			checkEOF: true,
		},
		{
			name:     "short payload -> unexpected EOF with partial read",
			length:   5,
			payload:  []byte{1, 2},
			want:     nil,
			wantPos:  2 + 2, // 2 (len) + 2 bytes actually read before EOF
			wantErr:  true,
			checkEOF: true,
		},
		{
			name:    "positive length with extra trailing bytes (not consumed)",
			length:  2,
			payload: []byte{4, 5, 6, 7},
			want:    []byte{4, 5},
			wantPos: 4, // 2 (len) + 2 consumed
			wantErr: false,
		},
		{
			name:    "large payload grows internal buffer",
			length:  1025,
			payload: makePayload(1025),
			want:    makePayload(1025),
			wantPos: 2 + 1025,
			wantErr: false,
		},
	}

	build := func(length int16, payload []byte) []byte {
		h := []byte{byte(uint16(length) >> 8), byte(uint16(length))}
		return append(h, payload...)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := tt.raw
			if stream == nil {
				stream = build(tt.length, tt.payload)
			}
			d := NewDecoder(bytes.NewReader(stream))
			got, err := d.NullableBytes()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (pos=%d)", d.pos)
				}
				if tt.checkEOF && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
				}
				if d.pos != tt.wantPos {
					t.Fatalf("pos mismatch: got %d want %d", d.pos, tt.wantPos)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("value mismatch")
			}
			if d.pos != tt.wantPos {
				t.Fatalf("pos mismatch: got %d want %d", d.pos, tt.wantPos)
			}
		})
	}
}
