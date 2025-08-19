package graph

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/specterops/dawgs/util/size"
)

// String represents a database-safe code-to-symbol mapping that negotiates to a string.
type String interface {
	String() string
}

// Kind is an interface that represents a DAWGS Node's type. Simple constant enumerations are encouraged when satisfying
// the Kind contract. Kind implementations must implement all functions of the Kind contract.
type Kind interface {
	String

	// Is returns true if the other Kind matches the Kind represented by this interface.
	Is(other ...Kind) bool
}

// Kinds is a type alias for []Kind that adds some additional convenience receiver functions.
type Kinds []Kind

func (s Kinds) Copy() Kinds {
	var kindsCopy Kinds

	if s != nil {
		kindsCopy = make(Kinds, len(s))
		copy(kindsCopy, s)
	}

	return kindsCopy
}

func (s Kinds) ConcatenateAll(kindBags ...Kinds) Kinds {
	combined := s

	for _, kindBag := range kindBags {
		combined = combined.Concatenate(kindBag)
	}

	return combined
}

func (s Kinds) Concatenate(kinds Kinds) Kinds {
	combined := make(Kinds, len(s)+len(kinds))

	copy(combined, s)
	copy(combined[len(s):], kinds)

	return combined
}

func (s Kinds) Exclude(exclusions Kinds) Kinds {
	kinds := make(Kinds, 0, len(s))

	for _, kind := range s {
		if !exclusions.ContainsOneOf(kind) {
			kinds = append(kinds, kind)
		}
	}

	return kinds
}

func (s Kinds) Remove(kind Kind) Kinds {
	for idx, nodeKind := range s {
		if kind == nodeKind {
			return append(s[:idx], s[idx+1:]...)
		}
	}

	return s
}

func (s Kinds) Add(kinds ...Kind) Kinds {
	ref := s

	for _, kind := range kinds {
		if !ref.ContainsOneOf(kind) {
			ref = append(ref, kind)
		}
	}

	return ref
}

func (s Kinds) SizeOf() size.Size {
	byteSize := size.Of(s) * size.Size(cap(s))

	for idx := 0; idx < len(s); idx++ {
		byteSize += size.Of(s[idx])
	}

	return byteSize
}

func (s Kinds) Strings() []string {
	kindStrings := make([]string, len(s))
	for idx := 0; idx < len(s); idx++ {
		kindStrings[idx] = s[idx].String()
	}

	return kindStrings
}

func (s Kinds) Formatted() string {
	return strings.Join(s.Strings(), ",")
}

// ContainsOneOf returns true if the Kinds contains one of the given Kind types or false if it does not.
func (s Kinds) ContainsOneOf(others ...Kind) bool {
	for _, kind := range s {
		if kind == nil {
			continue
		}
		if kind.Is(others...) {
			return true
		}
	}

	return false
}

// Hash returns a deterministic hash of the Kinds in sorted order.
func (s Kinds) HashInto(h *xxhash.Digest) error {
	if len(s) == 0 {
		return nil
	}

	// sort names for deterministic order
	ks := s.Strings()
	sort.Strings(ks)

	// Reuse one stack-allocated buffer for all length prefixes
	var lenbuf [binary.MaxVarintLen64]byte

	for i := range ks {
		kind := ks[i]

		// frame kinds with a length-prefix to prevent collisions like: ["ab","c"] === ["a","bc"]
		n := binary.PutUvarint(lenbuf[:], uint64(len(kind)))
		if _, err := h.Write(lenbuf[:n]); err != nil {
			return fmt.Errorf("writing length prefix: %w", err)
		}

		// NOTE: converting string -> []byte allocates; see note below.
		if _, err := h.Write([]byte(kind)); err != nil {
			return fmt.Errorf("writing kind to hash: %w", err)
		}
	}

	return nil
}

var (
	kindCache = &sync.Map{}
	EmptyKind = StringKind("")
)

func StringKind(str string) Kind {
	var (
		kind          = stringKind(str)
		cachedKind, _ = kindCache.LoadOrStore(str, &kind)
	)

	return cachedKind.(Kind)
}

func StringsToKinds(strs []string) Kinds {
	kinds := make(Kinds, len(strs))

	for idx := 0; idx < len(strs); idx++ {
		kinds[idx] = StringKind(strs[idx])
	}

	return kinds
}

type stringKind string

func (s stringKind) String() string {
	return string(s)
}

func (s stringKind) SizeOf() int64 {
	return int64(unsafe.Sizeof(s))
}

func (s stringKind) Is(other ...Kind) bool {
	for idx := 0; idx < len(other); idx++ {
		if s.String() == other[idx].String() {
			return true
		}
	}

	return false
}
