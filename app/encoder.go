package main

import (
	"bufio"
	"io"
)

type ByteWriteWriter interface {
	io.Writer
	io.ByteWriter
}

type Encoder struct {
	writer  ByteWriteWriter
	scratch []byte
	err     error
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		writer:  bufio.NewWriter(w),
		scratch: make([]byte, 8),
	}
}

func (e *Encoder) Int8(n int8) {
	if e.err != nil {
		return
	}
	_, e.err = e.writer.Write([]byte{byte(n)})
}

func (e *Encoder) Int16(n int16) {
	if e.err != nil {
		return
	}
	e.scratch[0] = byte(n)
	e.scratch[1] = byte(n >> 8)
	_, e.err = e.writer.Write(e.scratch[:2])
}
