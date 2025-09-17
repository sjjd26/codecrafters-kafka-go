package main

import "encoding/binary"

func appendVarint(slice []byte, value int64) []byte {
	var buf [10]byte
	n := binary.PutVarint(buf[:], value)
	return append(slice, buf[:n]...)
}

func appendUVarint(slice []byte, value uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], value)
	return append(slice, buf[:n]...)
}
