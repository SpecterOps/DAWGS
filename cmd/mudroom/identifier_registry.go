package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/graph"
)

const (
	minFakeRID = uint64(1000)
	maxFakeRID = uint64(4294967295)
)

type graphIdentifierNode struct {
	databaseID graph.ID
	kinds      []string
	objectID   string
	domainName string
}

type graphIdentifierRegistry struct {
	objectIDReplacements   map[string]string
	domainSIDReplacements  map[string]string
	domainNameReplacements map[string]string
	usedDomainRIDs         map[string]map[uint64]struct{}
}

func newGraphIdentifierRegistry() *graphIdentifierRegistry {
	return &graphIdentifierRegistry{
		objectIDReplacements:   map[string]string{},
		domainSIDReplacements:  map[string]string{},
		domainNameReplacements: map[string]string{},
		usedDomainRIDs:         map[string]map[uint64]struct{}{},
	}
}

func (s Scrubber) collectGraphIdentifierDomain(node graphIdentifierNode, registry *graphIdentifierRegistry) {
	if !s.isDomainKind(node.kinds) {
		return
	}

	if domainSIDPattern.MatchString(node.objectID) {
		if _, exists := registry.domainSIDReplacements[node.objectID]; !exists {
			registry.domainSIDReplacements[node.objectID] = s.fakeDomainSID(node.objectID)
		}
	}

	if node.domainName != "" {
		replacement, _ := s.ScrubProperty(s.config.GraphRules.DomainNameKey, node.domainName)
		if replacementString, ok := replacement.(string); ok && replacementString != "" {
			registry.domainNameReplacements[s.domainNameRegistryKey(node.domainName)] = replacementString
		}
	}
}

func (s Scrubber) collectGraphIdentifierObjectID(node graphIdentifierNode, registry *graphIdentifierRegistry, liveDatabase bool) {
	if node.objectID == "" {
		return
	}
	if _, exists := registry.objectIDReplacements[node.objectID]; exists {
		return
	}

	if replacement, found := registry.domainSIDReplacements[node.objectID]; found {
		registry.objectIDReplacements[node.objectID] = replacement
		return
	}

	if s.config.GraphRules.PreserveADSIDDomainPrefixes {
		if matches := adObjectSIDPattern.FindStringSubmatch(node.objectID); len(matches) == 3 {
			if domainSIDReplacement, found := registry.domainSIDReplacements[matches[1]]; found {
				registry.objectIDReplacements[node.objectID] = s.fakeObjectSIDWithDomainPrefix(registry, domainSIDReplacement, node.objectID)
				return
			}
			registry.objectIDReplacements[node.objectID] = s.fakeSID(node.objectID)
			return
		}
	}

	if liveDatabase {
		registry.objectIDReplacements[node.objectID] = fmt.Sprintf("id-%d", node.databaseID.Uint64())
		return
	}

	replacement, _ := s.ScrubProperty(s.config.GraphRules.ObjectIDKey, node.objectID)
	if replacementString, ok := replacement.(string); ok && replacementString != "" {
		registry.objectIDReplacements[node.objectID] = replacementString
	}
}

func (s Scrubber) applyGraphIdentifierRewrites(originalProperties, scrubbedProperties map[string]any, registry *graphIdentifierRegistry) {
	if registry == nil {
		return
	}

	if replacement, found := registry.replacementForObjectIDValue(originalProperties[s.config.GraphRules.ObjectIDKey]); found {
		scrubbedProperties[s.config.GraphRules.ObjectIDKey] = replacement
	}

	for _, key := range s.config.GraphRules.DomainSIDReferenceKeys {
		if replacement, found := registry.replacementForDomainSIDValue(originalProperties[key]); found {
			scrubbedProperties[key] = replacement
		}
	}

	for _, key := range s.config.GraphRules.ObjectIDReferenceKeys {
		if replacement, found := registry.replacementForObjectIDValue(originalProperties[key]); found {
			scrubbedProperties[key] = replacement
		}
	}

	if objectID, ok := stringPropertyValue(originalProperties[s.config.GraphRules.ObjectIDKey]); ok {
		if replacement, found := registry.objectIDReplacements[objectID]; found {
			for _, key := range s.config.GraphRules.SelfObjectIDAliasKeys {
				if alias, ok := stringPropertyValue(originalProperties[key]); ok && alias == objectID {
					scrubbedProperties[key] = replacement
				}
			}
		}
	}

	for _, key := range s.config.GraphRules.DomainNameReferenceKeys {
		if domainName, ok := stringPropertyValue(originalProperties[key]); ok {
			if replacement, found := registry.domainNameReplacements[s.domainNameRegistryKey(domainName)]; found {
				scrubbedProperties[key] = replacement
			}
		}
	}
}

func (s Scrubber) fakeObjectSIDWithDomainPrefix(registry *graphIdentifierRegistry, fakeDomainSID, originalObjectSID string) string {
	usedRIDs := registry.usedDomainRIDs[fakeDomainSID]
	if usedRIDs == nil {
		usedRIDs = map[uint64]struct{}{}
		registry.usedDomainRIDs[fakeDomainSID] = usedRIDs
	}

	ridRange := maxFakeRID - minFakeRID + 1
	ridSeed, _ := strconv.ParseUint(s.decimalToken("sid-rid", originalObjectSID, 10), 10, 64)
	rid := minFakeRID + (ridSeed % ridRange)
	for {
		if _, exists := usedRIDs[rid]; !exists {
			usedRIDs[rid] = struct{}{}
			return fakeDomainSID + "-" + strconv.FormatUint(rid, 10)
		}
		rid++
		if rid > maxFakeRID {
			rid = minFakeRID
		}
	}
}

func (s Scrubber) domainNameRegistryKey(value string) string {
	trimmed := strings.TrimSpace(value)
	if s.config.GraphRules.CaseInsensitiveDomainNames {
		return strings.ToLower(trimmed)
	}
	return trimmed
}

func (s Scrubber) isDomainKind(kinds []string) bool {
	for _, kind := range kinds {
		if strings.EqualFold(kind, s.config.GraphRules.DomainKind) {
			return true
		}
	}
	return false
}

func (s *graphIdentifierRegistry) replacementForObjectIDValue(value any) (string, bool) {
	objectID, ok := stringPropertyValue(value)
	if !ok {
		return "", false
	}
	replacement, found := s.objectIDReplacements[objectID]
	return replacement, found
}

func (s *graphIdentifierRegistry) replacementForDomainSIDValue(value any) (string, bool) {
	domainSID, ok := stringPropertyValue(value)
	if !ok {
		return "", false
	}
	replacement, found := s.domainSIDReplacements[domainSID]
	return replacement, found
}

func graphKindStrings(kinds graph.Kinds) []string {
	kindStrings := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		if kind != nil {
			kindStrings = append(kindStrings, kind.String())
		}
	}
	return kindStrings
}

func stringPropertyValue(value any) (string, bool) {
	stringValue, ok := value.(string)
	if !ok {
		return "", false
	}
	trimmed := strings.TrimSpace(stringValue)
	if trimmed == "" {
		return "", false
	}
	return trimmed, true
}
