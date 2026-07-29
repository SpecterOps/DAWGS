// Package entity defines canonical graph-export entity values.
package entity

import "errors"

type Node struct {
	SourceID   string
	Kinds      []string
	Properties map[string]any
}

type Relationship struct {
	SourceID   string
	StartID    string
	EndID      string
	Kind       string
	Properties map[string]any
}

func CloneKinds(kinds []string) []string {
	return append([]string(nil), kinds...)
}

func CloneProperties(properties map[string]any) map[string]any {
	if properties == nil {
		return nil
	}

	cloned := make(map[string]any, len(properties))
	for key, value := range properties {
		cloned[key] = value
	}

	return cloned
}

func (s Node) Validate() error {
	if s.SourceID == "" {
		return errors.New("node source ID is required")
	}

	return nil
}

func (s Relationship) Validate() error {
	if s.StartID == "" {
		return errors.New("relationship start ID is required")
	}
	if s.EndID == "" {
		return errors.New("relationship end ID is required")
	}
	if s.Kind == "" {
		return errors.New("relationship kind is required")
	}

	return nil
}
