package main

import (
	"bytes"
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
		})
	}
}
