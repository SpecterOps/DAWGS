package pg

import (
	"encoding/binary"
	"math/bits"
)

const (
	xxh32Prime1 uint32 = 0x9e3779b1
	xxh32Prime2 uint32 = 0x85ebca77
	xxh32Prime3 uint32 = 0xc2b2ae3d
	xxh32Prime4 uint32 = 0x27d4eb2f
	xxh32Prime5 uint32 = 0x165667b1

	ingestNodeIdentityDomain = "dawgs:pg-ingest:node-id:v1"
	ingestEdgeIdentityDomain = "dawgs:pg-ingest:edge-id:v1"
)

func xxh32Sum(input []byte, seed uint32) uint32 {
	inputLength := len(input)
	index := 0

	var hash uint32
	if inputLength >= 16 {
		v1 := seed + xxh32Prime1 + xxh32Prime2
		v2 := seed + xxh32Prime2
		v3 := seed
		v4 := seed - xxh32Prime1

		for index <= inputLength-16 {
			v1 = xxh32Round(v1, binary.LittleEndian.Uint32(input[index:]))
			v2 = xxh32Round(v2, binary.LittleEndian.Uint32(input[index+4:]))
			v3 = xxh32Round(v3, binary.LittleEndian.Uint32(input[index+8:]))
			v4 = xxh32Round(v4, binary.LittleEndian.Uint32(input[index+12:]))
			index += 16
		}

		hash = bits.RotateLeft32(v1, 1) + bits.RotateLeft32(v2, 7) +
			bits.RotateLeft32(v3, 12) + bits.RotateLeft32(v4, 18)
	} else {
		hash = seed + xxh32Prime5
	}

	hash += uint32(inputLength)

	for index <= inputLength-4 {
		hash += binary.LittleEndian.Uint32(input[index:]) * xxh32Prime3
		hash = bits.RotateLeft32(hash, 17) * xxh32Prime4
		index += 4
	}

	for index < inputLength {
		hash += uint32(input[index]) * xxh32Prime5
		hash = bits.RotateLeft32(hash, 11) * xxh32Prime1
		index++
	}

	hash ^= hash >> 15
	hash *= xxh32Prime2
	hash ^= hash >> 13
	hash *= xxh32Prime3
	hash ^= hash >> 16

	return hash
}

func xxh32Round(accumulator, lane uint32) uint32 {
	accumulator += lane * xxh32Prime2
	accumulator = bits.RotateLeft32(accumulator, 13)

	return accumulator * xxh32Prime1
}

func hashIngestNodeIdentity(objectID string) uint32 {
	return xxh32Sum(frameIngestIdentity(ingestNodeIdentityDomain, objectID), 0)
}

func hashIngestEdgeIdentity(startObjectID, kind, endObjectID string) uint32 {
	return xxh32Sum(frameIngestIdentity(ingestEdgeIdentityDomain, startObjectID, kind, endObjectID), 0)
}

func frameIngestIdentity(domain string, fields ...string) []byte {
	length := len(domain) + len(fields)*8
	for _, field := range fields {
		length += len(field)
	}

	frame := make([]byte, length)
	index := copy(frame, domain)
	for _, field := range fields {
		binary.BigEndian.PutUint64(frame[index:], uint64(len(field)))
		index += 8
		index += copy(frame[index:], field)
	}

	return frame
}
