package pg

import (
	"crypto/sha256"
	"hash"
	"math"
	"reflect"
	"sort"
	"strconv"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

const (
	// builderIdentityBufferSize bounds writes to the structural identity hash.
	builderIdentityBufferSize = 1024

	// builderIdentityMaxDepth bounds recursion through caller-supplied query trees.
	builderIdentityMaxDepth = 128

	// builderIdentityTokenSeparator separates a token's byte length from its contents.
	builderIdentityTokenSeparator = ":"

	// builderIdentityNilToken distinguishes absent values from populated containers.
	builderIdentityNilToken = "nil"

	// builderIdentityPresentToken marks a non-nil interface or pointer before its value.
	builderIdentityPresentToken = "present"
)

// builderCacheIdentity identifies a query tree only when bounded encoding succeeds.
type builderCacheIdentity struct {
	// digest fingerprints typed structure while excluding runtime parameter values.
	digest [sha256.Size]byte
	// valid reports whether the entire query satisfied the identity contract.
	valid bool
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
	return builderCacheIdentity{
		digest: encoder.sum(),
		valid:  true,
	}
}

// builderIdentityEncoder streams length-framed query structure into a bounded hash.
type builderIdentityEncoder struct {
	// hash accumulates complete buffers and the final partial buffer.
	hash hash.Hash
	// buffer batches token writes without retaining the query tree.
	buffer [builderIdentityBufferSize]byte
	// used counts occupied bytes in buffer.
	used int
	// total counts encoded bytes against the cache identity work limit.
	total int
}

// flush hashes the occupied buffer and prepares it for subsequent token bytes.
func (s *builderIdentityEncoder) flush() {
	if s.hash == nil {
		s.hash = sha256.New()
	}

	s.hash.Write(s.buffer[:s.used])
	s.used = 0
}

// write streams value through the buffer without changing the input binding.
func (s *builderIdentityEncoder) write(value string) {
	remainingValue := value

	for len(remainingValue) > 0 {
		written := copy(s.buffer[s.used:], remainingValue)
		s.used += written
		remainingValue = remainingValue[written:]
		if s.used == len(s.buffer) {
			s.flush()
		}
	}
}

// sum flushes pending bytes and returns the fixed-size structural digest.
func (s *builderIdentityEncoder) sum() [sha256.Size]byte {
	s.flush()

	var digest [sha256.Size]byte
	s.hash.Sum(digest[:0])
	return digest
}

// token length-frames a value so field, type and container boundaries stay unambiguous.
func (s *builderIdentityEncoder) token(value string) bool {
	var (
		length = strconv.Itoa(len(value))
		size   = len(length) + len(builderIdentityTokenSeparator) + len(value)
	)

	if size > maxCachedCypherBytes-s.total {
		return false
	}
	s.total += size
	s.write(length)
	s.write(builderIdentityTokenSeparator)
	s.write(value)

	return true
}

// builderIdentityPlan caches only type metadata, never query trees or parameter
// values. The registry is fixed at initialization; arbitrary caller-defined types cannot
// grow it. Fields (including embedded private AST fields) are discovered rather
// than enumerated, so adding an AST field cannot silently remove it from identity.
type builderIdentityPlan struct {
	// frame contains the already length-framed package path and type name.
	frame string
	// kind selects the encoding rules without repeating reflection on the type.
	kind reflect.Kind
	// parameter marks pointers whose runtime name and value must be excluded.
	parameter bool
	// astStruct permits field traversal only for structures owned by the Cypher AST.
	astStruct bool
	// elem supplies static element metadata, or nil for dynamic fallback encoding.
	elem *builderIdentityPlan
	// fields follows reflection field order, including embedded private AST fields.
	fields []*builderIdentityPlan
}

// identityTypeFrame encodes the same package and type tokens used by uncached identity.
func identityTypeFrame(valueType reflect.Type) string {
	var (
		packagePath = valueType.PkgPath()
		typeName    = valueType.String()
	)

	return strconv.Itoa(len(packagePath)) + builderIdentityTokenSeparator + packagePath + strconv.Itoa(len(typeName)) + builderIdentityTokenSeparator + typeName
}

// builderIdentityPlans holds immutable metadata for a fixed set of AST and scalar types.
var builderIdentityPlans = newBuilderIdentityPlans()

// newBuilderIdentityPlans discovers static fields once and seeds common dynamic expressions.
func newBuilderIdentityPlans() map[reflect.Type]*builderIdentityPlan {
	var (
		plans   = make(map[reflect.Type]*builderIdentityPlan)
		addPlan func(reflect.Type) *builderIdentityPlan
	)

	addPlan = func(valueType reflect.Type) *builderIdentityPlan {
		if existingPlan := plans[valueType]; existingPlan != nil {
			return existingPlan
		}

		plan := &builderIdentityPlan{
			frame:     identityTypeFrame(valueType),
			kind:      valueType.Kind(),
			parameter: valueType == reflect.TypeFor[*cypher.Parameter](),
			astStruct: valueType.PkgPath() == reflect.TypeFor[cypher.RegularQuery]().PkgPath(),
		}
		plans[valueType] = plan

		switch valueType.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map:
			if !plan.parameter {
				plan.elem = addPlan(valueType.Elem())
			}

		case reflect.Struct:
			if plan.astStruct {
				plan.fields = make([]*builderIdentityPlan, valueType.NumField())
				for fieldIndex := range plan.fields {
					plan.fields[fieldIndex] = addPlan(valueType.Field(fieldIndex).Type)
				}
			}
		}

		return plan
	}

	// RegularQuery supplies the static tree. These seeds supply expressions held
	// behind interfaces; less common or caller-defined values use the same encoder
	// with uncached type metadata. Omitting a seed affects speed, not identity.
	for _, seed := range []any{
		(*cypher.RegularQuery)(nil), (*cypher.NodePattern)(nil), (*cypher.RelationshipPattern)(nil),
		(*cypher.KindMatcher)(nil), (*cypher.Literal)(nil), (*cypher.Parameter)(nil),
		(*cypher.Conjunction)(nil), (*cypher.Disjunction)(nil), (*cypher.ExclusiveDisjunction)(nil),
		(*cypher.Negation)(nil), (*cypher.Parenthetical)(nil), (*cypher.Comparison)(nil),
		(*cypher.PropertyLookup)(nil), (*cypher.FunctionInvocation)(nil),
		(*cypher.ProjectionItem)(nil), (*cypher.Properties)(nil),
		(*cypher.ArithmeticExpression)(nil), (*cypher.UnaryAddOrSubtractExpression)(nil),
		(*cypher.Quantifier)(nil), (*cypher.PatternPredicate)(nil),
		(*cypher.Create)(nil), (*cypher.Merge)(nil), (*cypher.Set)(nil),
		(*cypher.Remove)(nil), (*cypher.Delete)(nil), cypher.MapLiteral(nil), cypher.ListLiteral(nil),
		graph.StringKind(""), int(0), int8(0), int16(0), int32(0), int64(0),
		uint(0), uint8(0), uint16(0), uint32(0), uint64(0), float32(0), float64(0),
	} {
		addPlan(reflect.TypeOf(seed))
	}

	return plans
}

// encode resolves metadata for a reflected value without retaining unknown types.
func (s *builderIdentityEncoder) encode(value reflect.Value, depth int) bool {
	if !value.IsValid() {
		return depth <= builderIdentityMaxDepth && s.token(builderIdentityNilToken)
	}

	plan := builderIdentityPlans[value.Type()]
	if plan == nil {
		valueType := value.Type()
		plan = &builderIdentityPlan{
			frame:     identityTypeFrame(valueType),
			kind:      valueType.Kind(),
			parameter: valueType == reflect.TypeFor[*cypher.Parameter](),
			astStruct: valueType.PkgPath() == reflect.TypeFor[cypher.RegularQuery]().PkgPath(),
		}
	}

	return s.encodePlanned(value, depth, plan)
}

// encodePlanned preserves typed identity and bounded work while reusing static metadata.
func (s *builderIdentityEncoder) encodePlanned(value reflect.Value, depth int, plan *builderIdentityPlan) bool {
	if plan == nil {
		return s.encode(value, depth)
	}
	if depth > builderIdentityMaxDepth {
		return false
	}
	if len(plan.frame) > maxCachedCypherBytes-s.total {
		return false
	}
	s.total += len(plan.frame)
	s.write(plan.frame)

	switch plan.kind {
	case reflect.Interface, reflect.Pointer:
		if value.IsNil() {
			return s.token(builderIdentityNilToken)
		}
		if !s.token(builderIdentityPresentToken) {
			return false
		}
		if plan.parameter {
			return true
		}
		return s.encodePlanned(value.Elem(), depth+1, plan.elem)
	case reflect.Struct:
		// AST structs are owned here. External structs can hide state behind
		// methods (including custom Kind implementations); bypass those shapes.
		if !plan.astStruct {
			return false
		}
		for idx := 0; idx < value.NumField(); idx++ {
			var field *builderIdentityPlan
			if plan.fields != nil {
				field = plan.fields[idx]
			}
			if !s.encodePlanned(value.Field(idx), depth+1, field) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Map:
		if value.IsNil() {
			return s.token(builderIdentityNilToken)
		}
		if !s.token(strconv.Itoa(value.Len())) || value.Len() > maxCachedCypherBytes {
			return false
		}
		if plan.kind == reflect.Map {
			if value.Type().Key().Kind() != reflect.String {
				return false
			}
			keys := value.MapKeys()
			sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
			for _, key := range keys {
				if !s.token(key.String()) || !s.encodePlanned(value.MapIndex(key), depth+1, plan.elem) {
					return false
				}
			}
			return true
		}
		for idx := 0; idx < value.Len(); idx++ {
			if !s.encodePlanned(value.Index(idx), depth+1, plan.elem) {
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
