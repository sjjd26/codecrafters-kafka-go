package main

import "io"

type SeekReader interface {
	io.Reader
	io.Seeker
}

type Decoder struct {
	reader SeekReader
	pos    int
	buf    []byte
}

func NewDecoder(r SeekReader) *Decoder {
	return &Decoder{
		reader: r,
		buf:    make([]byte, 4096),
	}
}
