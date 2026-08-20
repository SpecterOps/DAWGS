package pg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestXXH32OfficialVectors(t *testing.T) {
	require.Equal(t, uint32(0x02cc5d05), xxh32Sum(nil, 0))
	require.Equal(t, uint32(0x550d7456), xxh32Sum([]byte("a"), 0))
	require.Equal(t, uint32(0x32d153ff), xxh32Sum([]byte("abc"), 0))
	require.Equal(t, uint32(0x42ae804d), xxh32Sum([]byte("abcdefghijklmnopqrstuvwxyz0123456789"), 0))
}

func TestIngestIdentityHashesAreDomainSeparated(t *testing.T) {
	nodeHash := hashIngestNodeIdentity("source")
	edgeHash := hashIngestEdgeIdentity("source", "", "")

	require.NotEqual(t, nodeHash, edgeHash)
}

func TestIngestNodeIdentityHashIsCaseSensitive(t *testing.T) {
	require.NotEqual(t, hashIngestNodeIdentity("ObjectID"), hashIngestNodeIdentity("objectid"))
}

func TestIngestEdgeIdentityHashPreservesDirection(t *testing.T) {
	forward := hashIngestEdgeIdentity("start", "MemberOf", "end")
	reverse := hashIngestEdgeIdentity("end", "MemberOf", "start")

	require.NotEqual(t, forward, reverse)
}

func TestIngestEdgeIdentityHashLengthFramesFields(t *testing.T) {
	first := hashIngestEdgeIdentity("ab", "c", "")
	second := hashIngestEdgeIdentity("a", "bc", "")

	require.NotEqual(t, first, second)
}

func TestIngestIdentityHashSignedConversionPreservesBits(t *testing.T) {
	for _, hash := range []uint32{0, 1, 0x7fffffff, 0x80000000, 0xffffffff} {
		require.Equal(t, hash, uint32(int32(hash)))
	}
}
