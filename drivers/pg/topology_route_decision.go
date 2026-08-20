package pg

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
)

const (
	topologyRouteDecisionMaximumEntries = 64
	topologyRouteDecisionMaximumBytes   = 64 * 1024
	topologyRouteDecisionMaximumEntry   = 4 * 1024
)

var topologyRouteDecisionOwner uint64

// TraversalRouteDecision is query-text-free shadow telemetry for a
// transaction-owned topology decision. Shadow decisions never alter emitted
// SQL and never retain caller values.
type TraversalRouteDecision struct {
	Mode   string `json:"mode"`
	Reason string `json:"reason"`
}

// TraversalRouteDecisionCollector optionally records aggregate decision
// states. Implementations must not retain the per-transaction cache key.
type TraversalRouteDecisionCollector interface {
	RecordTraversalRouteDecision(TraversalRouteDecision)
}

type topologyRouteDecisionCache struct {
	owner      uint64
	generation uint64
	disabled   bool
	entries    map[string]struct{}
	bytes      int
}

func newTopologyRouteDecisionCache() *topologyRouteDecisionCache {
	return &topologyRouteDecisionCache{
		owner:   atomic.AddUint64(&topologyRouteDecisionOwner, 1),
		entries: map[string]struct{}{},
	}
}

func topologyRouteParameterFingerprint(parameters map[string]any) (string, bool) {
	keys := make([]string, 0, len(parameters))
	for key := range parameters {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var canonical strings.Builder
	for _, key := range keys {
		encoded, err := json.Marshal(parameters[key])
		if err != nil {
			return "", false
		}
		fmt.Fprintf(&canonical, "%d:%s:%T:%d:%s|", len(key), key, parameters[key], len(encoded), encoded)
	}
	digest := sha256.Sum256([]byte(canonical.String()))
	return hex.EncodeToString(digest[:]), true
}

func (s *transaction) invalidateTopologyRouteDecisions() {
	if s.topologyRouteDecisions == nil {
		return
	}
	s.topologyRouteDecisions.entries = map[string]struct{}{}
	s.topologyRouteDecisions.bytes = 0
	s.topologyRouteDecisions.generation++
	s.topologyRouteDecisions.disabled = true
}

func (s *transaction) recordTopologyRouteDecision(decision TraversalRouteDecision) {
	if collector, found := s.schemaManager.translationCacheProvider.(TraversalRouteDecisionCollector); found {
		collector.RecordTraversalRouteDecision(decision)
	}
}

// topologyRouteDecision selects a snapshot-bound fixed-suffix candidate only
// after an incumbent miss has populated this transaction-owned cache. Any
// failure, a first observation, or an unavailable synopsis remains incumbent.
// The returned instruction is valid only for the current transaction.
func (s *transaction) topologyRouteDecision(graphID int32, shape TraversalShape, parameters map[string]any, policyIdentity string, candidateAuthorized bool) bool {
	if shape.Version != TraversalFixedSuffixShapeVersion {
		return false
	}
	cache := s.topologyRouteDecisions
	if cache == nil || cache.disabled || s.tx == nil || !stableSnapshotIsolation(s.isolation) {
		s.recordTopologyRouteDecision(TraversalRouteDecision{Mode: "incumbent", Reason: "topology_route_disabled"})
		return false
	}
	parametersFingerprint, valid := topologyRouteParameterFingerprint(parameters)
	if !valid {
		s.recordTopologyRouteDecision(TraversalRouteDecision{Mode: "incumbent", Reason: "topology_route_parameters_unverifiable"})
		return false
	}
	synopsis, err := s.traversalTopologySynopsis(graphID)
	if err != nil || !synopsis.Available() || synopsis.SchemaVersion != "topology-synopsis-schema-v2" || synopsis.NodeCount == 0 || synopsis.EdgeCount == 0 {
		s.recordTopologyRouteDecision(TraversalRouteDecision{Mode: "incumbent", Reason: "topology_synopsis_unavailable"})
		return false
	}
	keyMaterial := fmt.Sprintf("%d|%d|%d|%s|%s|%s|%d|%d", cache.owner, graphID, cache.generation, shape.Fingerprint, parametersFingerprint, policyIdentity, synopsis.Epoch, synopsis.CurrentMutationEpoch)
	digest := sha256.Sum256([]byte(keyMaterial))
	key := hex.EncodeToString(digest[:])
	if _, found := cache.entries[key]; found {
		if candidateAuthorized {
			s.recordTopologyRouteDecision(TraversalRouteDecision{Mode: "candidate", Reason: "topology_route_candidate_hit"})
			return true
		}
		s.recordTopologyRouteDecision(TraversalRouteDecision{Mode: "incumbent", Reason: "topology_route_shadow_hit"})
		return false
	}
	entryBytes := len(key) + len(shape.Fingerprint) + len(policyIdentity) + 64
	if entryBytes > topologyRouteDecisionMaximumEntry || len(cache.entries) == topologyRouteDecisionMaximumEntries || cache.bytes+entryBytes > topologyRouteDecisionMaximumBytes {
		s.recordTopologyRouteDecision(TraversalRouteDecision{Mode: "incumbent", Reason: "topology_route_capacity"})
		return false
	}
	cache.entries[key] = struct{}{}
	cache.bytes += entryBytes
	s.recordTopologyRouteDecision(TraversalRouteDecision{Mode: "incumbent", Reason: "topology_route_shadow_miss"})
	return false
}
