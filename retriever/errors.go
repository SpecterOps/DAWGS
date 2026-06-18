package retriever

import (
	"fmt"
)

type ChecksumMismatchError struct {
	Path           string
	ExpectedSHA256 string
	ActualSHA256   string
}

func (s ChecksumMismatchError) Error() string {
	return fmt.Sprintf("sha256 mismatch for %s: manifest has %s, file has %s", s.Path, s.ExpectedSHA256, s.ActualSHA256)
}

type ByteCountMismatchError struct {
	Path          string
	ExpectedBytes int64
	ActualBytes   int64
}

func (s ByteCountMismatchError) Error() string {
	return fmt.Sprintf("compressed byte mismatch for %s: manifest has %d, file has %d", s.Path, s.ExpectedBytes, s.ActualBytes)
}

type IncompatibleDriverError struct {
	Operation string
	Driver    string
	Reason    string
}

func (s IncompatibleDriverError) Error() string {
	if s.Reason != "" {
		return s.Reason
	}
	if s.Operation != "" && s.Driver != "" {
		return fmt.Sprintf("driver %q is incompatible with retriever %s", s.Driver, s.Operation)
	}
	if s.Driver != "" {
		return fmt.Sprintf("driver %q is incompatible with retriever", s.Driver)
	}
	return "driver is incompatible with retriever"
}

type NonEmptyTargetGraphError struct {
	GraphName string
	NodeCount int64
	EdgeCount int64
}

func (s NonEmptyTargetGraphError) Error() string {
	return fmt.Sprintf("target graph %q is not empty; found %d nodes and %d relationships; clear the graph before loading", s.GraphName, s.NodeCount, s.EdgeCount)
}

type MissingMetricsError struct {
	Operation string
}

func (s MissingMetricsError) Error() string {
	return "manifest does not contain graph metrics; create a new dump with metrics support before verifying"
}

type EntityCountMismatchError struct {
	Operation string
	Graph     string
	Phase     Phase
	Expected  int64
	Actual    int64
	Message   string
}

func (s EntityCountMismatchError) Error() string {
	if s.Message != "" {
		return s.Message
	}
	phase := string(s.Phase)
	if phase == "" {
		phase = "entities"
	}
	if s.Operation == "" {
		return fmt.Sprintf("%s count mismatch for graph %q: expected %d, actual %d", phase, s.Graph, s.Expected, s.Actual)
	}
	return fmt.Sprintf("%s %s count mismatch for graph %q: expected %d, actual %d", s.Operation, phase, s.Graph, s.Expected, s.Actual)
}
