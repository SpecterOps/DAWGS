package pg

import (
	"crypto/sha256"
	"hash"
	"math"
	"reflect"
	"sort"
	"strconv"

	"github.com/specterops/dawgs/cypher/models/cypher"
)

type builderCacheIdentity struct {
	digest [sha256.Size]byte
	valid  bool
}

// builderIdentity fingerprints the input tree, not its lossy Cypher rendering.
// Parameter names and values are rewritten before translation, so only their
// positions belong here; negotiated parameter types remain in the cache key.
// Unsupported values and bounded-work limits disable reuse, not compilation.
func builderIdentity(query *cypher.RegularQuery) builderCacheIdentity {
	encoder := builderIdentityEncoder{}
	if !encoder.encode(reflect.ValueOf(query), 0) {
		return builderCacheIdentity{}
	}
	return builderCacheIdentity{digest: encoder.sum(), valid: true}
}

type builderIdentityEncoder struct {
	hash   hash.Hash
	buffer [1024]byte
	used   int
	total  int
}

func (s *builderIdentityEncoder) flush() {
	if s.hash == nil {
		s.hash = sha256.New()
	}
	s.hash.Write(s.buffer[:s.used])
	s.used = 0
}

func (s *builderIdentityEncoder) write(value string) {
	for len(value) > 0 {
		written := copy(s.buffer[s.used:], value)
		s.used += written
		value = value[written:]
		if s.used == len(s.buffer) {
			s.flush()
		}
	}
}

func (s *builderIdentityEncoder) sum() [sha256.Size]byte {
	s.flush()
	var result [sha256.Size]byte
	s.hash.Sum(result[:0])
	return result
}

// Length framing keeps field, type, string and container boundaries unambiguous.
func (s *builderIdentityEncoder) token(value string) bool {
	length := strconv.Itoa(len(value))
	size := len(length) + 1 + len(value)
	if size > maxCachedCypherBytes-s.total {
		return false
	}
	s.total += size
	s.write(length)
	s.write(":")
	s.write(value)
	return true
}

func (s *builderIdentityEncoder) encode(value reflect.Value, depth int) bool {
	if depth > 128 {
		return false
	}
	if !value.IsValid() {
		return s.token("nil")
	}
	typeOf := value.Type()
	if !s.token(typeOf.PkgPath()) || !s.token(typeOf.String()) {
		return false
	}

	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if value.IsNil() {
			return s.token("nil")
		}
		if !s.token("present") {
			return false
		}
		if typeOf == reflect.TypeFor[*cypher.Parameter]() {
			return true
		}
		return s.encode(value.Elem(), depth+1)
	case reflect.Struct:
		// AST structs are owned here. External structs can hide state behind
		// methods (including custom Kind implementations); bypass those shapes.
		if typeOf.PkgPath() != reflect.TypeFor[cypher.RegularQuery]().PkgPath() {
			return false
		}
		for idx := 0; idx < value.NumField(); idx++ {
			if !s.encode(value.Field(idx), depth+1) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Map:
		if value.IsNil() {
			return s.token("nil")
		}
		if !s.token(strconv.Itoa(value.Len())) || value.Len() > maxCachedCypherBytes {
			return false
		}
		if value.Kind() == reflect.Map {
			if typeOf.Key().Kind() != reflect.String {
				return false
			}
			keys := value.MapKeys()
			sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
			for _, key := range keys {
				if !s.token(key.String()) || !s.encode(value.MapIndex(key), depth+1) {
					return false
				}
			}
			return true
		}
		for idx := 0; idx < value.Len(); idx++ {
			if !s.encode(value.Index(idx), depth+1) {
				return false
			}
		}
		return true
	case reflect.String:
		return s.token(value.String())
	case reflect.Bool:
		return s.token(strconv.FormatBool(value.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return s.token(strconv.FormatInt(value.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return s.token(strconv.FormatUint(value.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		return s.token(strconv.FormatUint(math.Float64bits(value.Float()), 16))
	default:
		return false
	}
}
