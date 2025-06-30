package graph

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/specterops/dawgs/util/size"
)

const (
	DirectionInbound  Direction = 0
	DirectionOutbound Direction = 1
	DirectionBoth     Direction = 2
)

var ErrInvalidDirection = errors.New("must be called with either an inbound or outbound direction")

// Direction describes the direction of a graph traversal. A Direction may be either Inbound or DirectionOutbound.
type Direction int

// Reverse returns the reverse of the current direction.
func (s Direction) Reverse() Direction {
	switch s {
	case DirectionInbound:
		return DirectionOutbound

	case DirectionOutbound:
		return DirectionInbound

	default:
		return DirectionBoth
	}
}

// PickID picks either the start or end Node ID from a Relationship depending on the direction of the receiver.
func (s Direction) PickID(start, end ID) (ID, error) {
	switch s {
	case DirectionInbound:
		return end, nil

	case DirectionOutbound:
		return start, nil

	default:
		return 0, ErrInvalidDirection
	}
}

// PickReverseID picks either the start or end Node ID from a Relationship depending on the direction of the receiver.
func (s Direction) PickReverseID(start, end ID) (ID, error) {
	switch s {
	case DirectionInbound:
		return start, nil

	case DirectionOutbound:
		return end, nil

	default:
		return 0, ErrInvalidDirection
	}
}

// Pick picks either the start or end Node ID from a Relationship depending on the direction of the receiver.
func (s Direction) Pick(relationship *Relationship) (ID, error) {
	return s.PickID(relationship.StartID, relationship.EndID)
}

// PickReverse picks either the start or end Node ID from a Relationship depending on the direction of the receiver.
func (s Direction) PickReverse(relationship *Relationship) (ID, error) {
	return s.PickReverseID(relationship.StartID, relationship.EndID)
}

func (s Direction) String() string {
	switch s {
	case DirectionInbound:
		return "inbound"
	case DirectionOutbound:
		return "outbound"
	case DirectionBoth:
		return "both"
	default:
		return "invalid"
	}
}

// ID is a 64-bit database Entity identifier type. Negative ID value associations in DAWGS drivers are not recommended
// and should not be considered during driver implementation.
type ID uint64

// Uint64 returns the ID typed as an uint64 and is shorthand for uint64(id).
func (s ID) Uint64() uint64 {
	return uint64(s)
}

// Uint32 returns the ID typed as an uint32 and is shorthand for uint32(id).
func (s ID) Uint32() uint32 {
	return uint32(s)
}

// Int64 returns the ID typed as an int64 and is shorthand for int64(id).
func (s ID) Int64() int64 {
	return int64(s)
}

func (s ID) Sizeof() size.Size {
	return size.Size(unsafe.Sizeof(s))
}

// String formats the int64 value of the ID as a string.
func (s ID) String() string {
	return strconv.FormatInt(s.Int64(), 10)
}

// PropertyValue is an interface that offers type negotiation for property values to reduce the boilerplate required
// handle property values.
type PropertyValue interface {
	// IsNil returns true if the property value is nil.
	IsNil() bool

	// Bool returns the property value as a bool along with any type negotiation error information.
	Bool() (bool, error)

	// Int returns the property value as an int along with any type negotiation error information.
	Int() (int, error)

	// Int64 returns the property value as an int64 along with any type negotiation error information.
	Int64() (int64, error)

	// Int64Slice returns the property value as an int64 slice along with any type negotiation error information.
	Int64Slice() ([]int64, error)

	// IDSlice returns the property value as a ID slice along with any type negotiation error information.
	IDSlice() ([]ID, error)

	//StringSlice returns the property value as a string slice along with any type negotiation error information.
	StringSlice() ([]string, error)

	// Uint64 returns the property value as an uint64 along with any type negotiation error information.
	Uint64() (uint64, error)

	// Float64 returns the property value as a float64 along with any type negotiation error information.
	Float64() (float64, error)

	// String returns the property value as a string along with any type negotiation error information.
	String() (string, error)

	// Time returns the property value as time.Time along with any type negotiation error information.
	Time() (time.Time, error)

	// Any returns the property value typed as any. This function may return a null reference.
	Any() any
}

type NodeUpdate struct {
	Node               *Node
	IdentityKind       Kind
	IdentityProperties []string
}

func (s NodeUpdate) Key() (string, error) {
	key := strings.Builder{}

	slices.Sort(s.IdentityProperties)

	for _, identityProperty := range s.IdentityProperties {
		if propertyValue, err := s.Node.Properties.Get(identityProperty).String(); err != nil {
			return "", err
		} else {
			key.WriteString(propertyValue)
		}
	}

	return key.String(), nil
}

type RelationshipUpdate struct {
	Relationship            *Relationship
	IdentityProperties      []string
	Start                   *Node
	StartIdentityKind       Kind
	StartIdentityProperties []string
	End                     *Node
	EndIdentityKind         Kind
	EndIdentityProperties   []string
}

func (s RelationshipUpdate) Key() (string, error) {
	var (
		key             = strings.Builder{}
		startNodeUpdate = NodeUpdate{
			Node:               s.Start,
			IdentityKind:       s.StartIdentityKind,
			IdentityProperties: s.StartIdentityProperties,
		}

		endNodeUpdate = NodeUpdate{
			Node:               s.End,
			IdentityKind:       s.EndIdentityKind,
			IdentityProperties: s.EndIdentityProperties,
		}
	)

	key.WriteString(s.Relationship.Kind.String())

	if startKey, err := startNodeUpdate.Key(); err != nil {
		return "", err
	} else if endKey, err := endNodeUpdate.Key(); err != nil {
		return "", err
	} else {
		key.WriteString(startKey)

		slices.Sort(s.IdentityProperties)

		for _, identityProperty := range s.IdentityProperties {
			if propertyValue, err := s.Relationship.Properties.Get(identityProperty).String(); err != nil {
				return "", err
			} else {
				key.WriteString(propertyValue)
			}
		}

		key.WriteString(endKey)
		return key.String(), nil
	}
}

func (s RelationshipUpdate) IdentityPropertiesMap() map[string]any {
	identityPropertiesMap := make(map[string]any, len(s.IdentityProperties))

	for _, identityPropertyKey := range s.IdentityProperties {
		identityPropertiesMap[identityPropertyKey] = s.Relationship.Properties.Get(identityPropertyKey).Any()
	}

	return identityPropertiesMap
}

func (s RelationshipUpdate) StartIdentityPropertiesMap() map[string]any {
	identityPropertiesMap := make(map[string]any, len(s.StartIdentityProperties))

	for _, identityPropertyKey := range s.StartIdentityProperties {
		identityPropertiesMap[identityPropertyKey] = s.Start.Properties.Get(identityPropertyKey).Any()
	}

	return identityPropertiesMap
}

func (s RelationshipUpdate) EndIdentityPropertiesMap() map[string]any {
	identityPropertiesMap := make(map[string]any, len(s.EndIdentityProperties))

	for _, identityPropertyKey := range s.EndIdentityProperties {
		identityPropertiesMap[identityPropertyKey] = s.End.Properties.Get(identityPropertyKey).Any()
	}

	return identityPropertiesMap
}
