package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type ByteReadReader interface {
	io.Reader
	io.ByteReader
}

type Decoder struct {
	reader ByteReadReader
	pos    int
	buf    []byte
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		reader: bufio.NewReader(r),
		buf:    make([]byte, 1024),
	}
}

func (d *Decoder) Int8() (int8, error) {
	n, err := d.reader.Read(d.buf[:1])
	d.pos += n
	if err != nil {
		return 0, err
	}
	if n < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	return int8(d.buf[0]), nil
}

func (d *Decoder) Int16() (int16, error) {
	n, err := d.reader.Read(d.buf[:2])
	d.pos += n
	if err != nil {
		return 0, err
	}
	if n < 2 {
		return 0, io.ErrUnexpectedEOF
	}
	return int16(binary.BigEndian.Uint16(d.buf[:2])), nil
}

func (d *Decoder) Int32() (int32, error) {
	n, err := d.reader.Read(d.buf[:4])
	d.pos += n
	if err != nil {
		return 0, err
	}
	if n < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	return int32(binary.BigEndian.Uint32(d.buf[:4])), nil
}

func (d *Decoder) Int64() (int64, error) {
	n, err := d.reader.Read(d.buf[:8])
	d.pos += n
	if err != nil {
		return 0, err
	}
	if n < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	return int64(binary.BigEndian.Uint64(d.buf[:8])), nil
}

func (d *Decoder) UUID() (uuid, error) {
	var u uuid
	n, err := d.reader.Read(u[:])
	d.pos += n
	if err != nil {
		return uuid{}, err
	}
	if n < len(u) {
		return uuid{}, io.ErrUnexpectedEOF
	}
	return u, nil
}

func (d *Decoder) UVarint() (uint64, error) {
	const (
		continuationMask = 0x80
		dataMask         = 0x7f
		maxBytes         = 10
	)

	var shift uint
	var b byte
	var value uint64
	for range maxBytes {
		b, err := d.reader.ReadByte()
		if err != nil {
			return 0, err
		}
		d.pos++
		// remove the continuation bit
		part := uint64(b & dataMask)
		// shift the
		value |= part << shift

		if b&continuationMask == 0 {
			// TODO: overflow sanity check on final byte if i==9
			return value, nil
		}
		shift += 7
	}

	if b&continuationMask != 0 {
		return 0, fmt.Errorf("varint too long")
	}
	return 0, fmt.Errorf("incomplete varint")
}

func (d *Decoder) Varint() (int64, error) {
	u, err := d.UVarint()
	if err != nil {
		return 0, err
	}
	// zigzag decode
	return int64((u >> 1) ^ uint64(-(u & 1))), nil
}

func (d *Decoder) ByteArray() ([]byte, error) {
	length, err := d.Int32()
	length--
	if err != nil {
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if length <= 0 {
		return nil, nil
	}
	if int(length) > len(d.buf) {
		d.buf = make([]byte, length)
	}
	n, err := d.reader.Read(d.buf[:length])
	d.pos += n
	if err != nil {
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if n < int(length) {
		return nil, io.ErrUnexpectedEOF
	}
	return d.buf[:length], nil
}
