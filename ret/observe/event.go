package observe

import "time"

// Event is an observation emitted during a graph export operation.
type Event interface {
	isEvent()
}

// OperationStarted indicates an operation has begun.
type OperationStarted struct{ Operation string }

func (OperationStarted) isEvent() {}

// OperationCompleted indicates an operation has completed.
type OperationCompleted struct {
	Operation string
	Duration  time.Duration
	Err       error
}

func (OperationCompleted) isEvent() {}

// GraphStarted indicates processing has begun for a graph.
type GraphStarted struct{ Operation, Graph string }

func (GraphStarted) isEvent() {}

// GraphCompleted indicates processing has completed for a graph.
type GraphCompleted struct {
	Operation, Graph     string
	Nodes, Relationships int64
	Duration             time.Duration
}

func (GraphCompleted) isEvent() {}

// PhaseStarted indicates a graph-processing phase has begun.
type PhaseStarted struct {
	Operation, Graph, Phase string
	Completed, Total        int64
}

func (PhaseStarted) isEvent() {}

// PhaseProgress reports progress through a graph-processing phase.
type PhaseProgress struct {
	Operation, Graph, Phase string
	Completed, Total        int64
}

func (PhaseProgress) isEvent() {}

// PhaseCompleted indicates a graph-processing phase has completed.
type PhaseCompleted struct {
	Operation, Graph, Phase string
	Completed               int64
	Duration                time.Duration
}

func (PhaseCompleted) isEvent() {}

// ShardCommitted indicates output files for a shard have been committed.
type ShardCommitted struct {
	Graph, EntityType, JSONLPath, ParquetPath string
	Index                                     int
	Count, JSONLBytes, ParquetBytes           int64
}

func (ShardCommitted) isEvent() {}

// ArtifactVerified indicates an output artifact has been verified.
type ArtifactVerified struct {
	Graph, EntityType, Format, Path string
	Count, Bytes                    int64
}

func (ArtifactVerified) isEvent() {}

// ArchiveEntryProcessed indicates an archive entry has been processed.
type ArchiveEntryProcessed struct {
	Operation, Path string
	Size            int64
}

func (ArchiveEntryProcessed) isEvent() {}
