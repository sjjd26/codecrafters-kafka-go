package main

import (
	"encoding/binary"
)

type Encoder struct {
	scratch []byte
	err     error
	buf     []byte
}

func NewEncoder(capacity int) *Encoder {
	return &Encoder{
		scratch: make([]byte, 8),
		buf:     make([]byte, 0, capacity),
	}
}

func (e *Encoder) Bytes() []byte { return e.buf }
func (e *Encoder) Reset()        { e.buf = e.buf[:0] }

func (e *Encoder) WriteBytes(b []byte) {
	e.buf = append(e.buf, b...)
}

func (e *Encoder) Int8(n int8) {
	e.buf = append(e.buf, byte(n))
}

func (e *Encoder) Int16(n int16) {
	e.buf = binary.BigEndian.AppendUint16(e.buf, uint16(n))
}

func (e *Encoder) Int32(n int32) {
	e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(n))
}

func (e *Encoder) Int64(n int64) {
	e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(n))
}

func (e *Encoder) UVarint(v uint64) {
	e.buf = binary.AppendUvarint(e.buf, v)
}

func (e *Encoder) UUID(u uuid) {
	e.buf = append(e.buf, u[:]...)
}
