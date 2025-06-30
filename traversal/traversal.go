package traversal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util"
	"github.com/specterops/dawgs/util/atomics"
	"github.com/specterops/dawgs/util/channels"
	"github.com/specterops/dawgs/util/size"
)

// Logic represents callable logic that drives sending queries to the graph
type Logic = func(ctx context.Context, tx database.Driver, segment *graph.PathSegment) ([]*graph.PathSegment, error)

type PatternMatchDelegate = func(terminal *graph.PathSegment) error

// PatternContinuation is an openCypher inspired fluent pattern for defining parallel chained expansions. After
// building the pattern the user may call the Do(...) function and pass it a delegate for handling paths that match
// the pattern.
//
// The return value of the Do(...) function may be passed directly to a Instance via a Plan as the Plan.Driver field.
type PatternContinuation interface {
	Outbound(criteria ...cypher.SyntaxNode) PatternContinuation
	OutboundWithDepth(min, max int, criteria ...cypher.SyntaxNode) PatternContinuation
	Inbound(criteria ...cypher.SyntaxNode) PatternContinuation
	InboundWithDepth(min, max int, criteria ...cypher.SyntaxNode) PatternContinuation
	Do(delegate PatternMatchDelegate) Logic
}

// expansion is an internal representation of a path expansion step.
type expansion struct {
	criteria  []cypher.SyntaxNode
	direction graph.Direction
	minDepth  int
	maxDepth  int
}

type patternTag struct {
	patternIdx int
	depth      int
}

func popSegmentPatternTag(segment *graph.PathSegment) *patternTag {
	var tag *patternTag

	if typedTag, typeOK := segment.Tag.(*patternTag); typeOK && typedTag != nil {
		tag = typedTag
		segment.Tag = nil
	} else {
		tag = &patternTag{
			patternIdx: 0,
			depth:      0,
		}
	}

	return tag
}

type pattern struct {
	expansions []expansion
	delegate   PatternMatchDelegate
}

// Do assigns the PatterMatchDelegate internally before returning a function pointer to the Driver receiver function.
func (s *pattern) Do(delegate PatternMatchDelegate) Logic {
	s.delegate = delegate
	return s.Driver
}

// OutboundWithDepth specifies the next outbound expansion step for this pattern with depth parameters.
func (s *pattern) OutboundWithDepth(min, max int, criteria ...cypher.SyntaxNode) PatternContinuation {
	if min < 0 {
		min = 1
		slog.Warn("Negative mindepth not allowed. Setting min depth for expansion to 1")
	}

	if max < 0 {
		max = 0
		slog.Warn("Negative maxdepth not allowed. Setting max depth for expansion to 0")
	}

	s.expansions = append(s.expansions, expansion{
		criteria:  criteria,
		direction: graph.DirectionOutbound,
		minDepth:  min,
		maxDepth:  max,
	})

	return s
}

// Outbound specifies the next outbound expansion step for this pattern. By default, this expansion will use a minimum
// depth of 1 to make the expansion required and a maximum depth of 0 to expand indefinitely.
func (s *pattern) Outbound(criteria ...cypher.SyntaxNode) PatternContinuation {
	return s.OutboundWithDepth(1, 0, criteria...)
}

// InboundWithDepth specifies the next inbound expansion step for this pattern with depth parameters.
func (s *pattern) InboundWithDepth(min, max int, criteria ...cypher.SyntaxNode) PatternContinuation {
	if min < 0 {
		min = 1
		slog.Warn("Negative mindepth not allowed. Setting min depth for expansion to 1")
	}

	if max < 0 {
		max = 0
		slog.Warn("Negative maxdepth not allowed. Setting max depth for expansion to 0")
	}

	s.expansions = append(s.expansions, expansion{
		criteria:  criteria,
		direction: graph.DirectionInbound,
		minDepth:  min,
		maxDepth:  max,
	})

	return s
}

// Inbound specifies the next inbound expansion step for this pattern. By default, this expansion will use a minimum
// depth of 1 to make the expansion required and a maximum depth of 0 to expand indefinitely.
func (s *pattern) Inbound(criteria ...cypher.SyntaxNode) PatternContinuation {
	return s.InboundWithDepth(1, 0, criteria...)
}

// NewPattern returns a new PatternContinuation for building a new pattern.
func NewPattern() PatternContinuation {
	return &pattern{}
}

func (s *pattern) Driver(ctx context.Context, dbDriver database.Driver, segment *graph.PathSegment) ([]*graph.PathSegment, error) {
	var (
		nextSegments []*graph.PathSegment

		// The patternTag lives on the current terminal segment of each path. Once popped the pointer reference for
		// this segment is set to nil.
		tag              = popSegmentPatternTag(segment)
		currentExpansion = s.expansions[tag.patternIdx]

		// fetchFunc handles directional results from the graph database and is called twice to fetch segment
		// expansions.
		fetchFunc = func(criteria cypher.SyntaxNode, direction graph.Direction) error {
			var (
				queryBuilder = query.New()
				allCriteria  = []cypher.SyntaxNode{criteria}
			)

			switch direction {
			case graph.DirectionInbound:
				queryBuilder.Where(append(allCriteria, query.Start().ID().Equals(segment.Node.ID))...).Return(
					query.Relationship(),
					query.End(),
				)

			case graph.DirectionOutbound:
				queryBuilder.Where(append(allCriteria, query.End().ID().Equals(segment.Node.ID))...).Return(
					query.Relationship(),
					query.Start(),
				)

			default:
				return fmt.Errorf("unsupported direction %v", direction)
			}

			if preparedQuery, err := queryBuilder.Build(); err != nil {
				return err
			} else {
				result := dbDriver.Exec(ctx, preparedQuery.Query, preparedQuery.Parameters)
				defer result.Close(ctx)

				for result.HasNext(ctx) {
					var (
						nextNode         graph.Node
						nextRelationship graph.Relationship
					)

					if err := result.Scan(&nextNode, &nextRelationship); err != nil {
						return err
					}

					nextSegment := segment.Descend(&nextNode, &nextRelationship)

					// Don't emit cycles out of the fetch
					if !nextSegment.IsCycle() {
						nextSegment.Tag = &patternTag{
							// Use the tag's patternIdx and depth since this is a continuation of the expansions
							patternIdx: tag.patternIdx,
							depth:      tag.depth + 1,
						}

						nextSegments = append(nextSegments, nextSegment)
					}
				}

				return result.Error()
			}
		}
	)

	// The fetch direction is the reverse intent of the expansion direction
	fetchDirection := currentExpansion.direction.Reverse()

	// If no max depth was set or if a max depth was set expand the current step further
	if currentExpansion.maxDepth == 0 || tag.depth < currentExpansion.maxDepth {
		// Perform the current expansion.
		if err := fetchFunc(currentExpansion.criteria, fetchDirection); err != nil {
			return nil, err
		}

		// Check first if this current segment was fetched using the current expansion (i.e. non-optional)
		if (tag.depth > 0 && currentExpansion.minDepth == 0) || tag.depth >= currentExpansion.minDepth {
			// No further expansions means this pattern segment is complete. Increment the pattern index to select the
			// next pattern expansion. Additionally, set the depth back to zero for the tag since we are leaving the
			// current expansion.
			tag.patternIdx++
			tag.depth = 0

			// Perform the next expansion if there is one.
			if tag.patternIdx < len(s.expansions) {
				nextExpansion := s.expansions[tag.patternIdx]

				// Expand the next segments
				if err := fetchFunc(nextExpansion.criteria, fetchDirection); err != nil {
					return nil, err
				}

				// If the next expansion is optional, make sure to preserve the current traversal branch
				if nextExpansion.minDepth == 0 {
					// Reattach the tag to the segment before adding it to the returned segments for the next expansion
					segment.Tag = tag
					nextSegments = append(nextSegments, segment)
				}
			} else if len(nextSegments) == 0 {
				// If there are no expanded segments and there are no remaining expansions, this is a terminal segment.
				// Hand it off to the delegate and handle any returned error.
				if err := s.delegate(segment); err != nil {
					return nil, err
				}
			}
		}
	}

	// If the above condition does not match then this current expansion is non-terminal and non-continuable
	return nextSegments, nil
}

type Plan struct {
	Root        *graph.Node
	RootSegment *graph.PathSegment
	Logic       Logic
}

type Instance struct {
	db                 database.Instance
	numParallelWorkers int
	memoryLimit        size.Size
}

func New(db database.Instance, numParallelWorkers int) Instance {
	return Instance{
		db:                 db,
		numParallelWorkers: numParallelWorkers,
	}
}

func (s Instance) BreadthFirst(ctx context.Context, plan Plan) error {
	var (
		// workerWG keeps count of background workers launched in goroutines
		workerWG = &sync.WaitGroup{}

		// descentWG keeps count of in-flight traversal work. When this wait group reaches a count of 0 the traversal
		// is considered complete.
		completionC                    = make(chan struct{}, s.numParallelWorkers)
		descentCount                   = &atomic.Int64{}
		errorCollector                 = util.NewErrorCollector()
		traversalCtx, doneFunc         = context.WithCancel(ctx)
		segmentWriterC, segmentReaderC = channels.BufferedPipe[*graph.PathSegment](traversalCtx)
		pathTree                       graph.Tree
	)

	// Defer calling the cancellation function of the context to ensure that all workers join, no matter what
	defer doneFunc()

	// Close the writer channel to the buffered pipe
	defer close(segmentWriterC)

	if plan.Root != nil {
		pathTree = graph.NewTree(plan.Root)
	} else if plan.RootSegment != nil {
		pathTree = graph.Tree{
			Root: plan.RootSegment,
		}
	} else {
		return fmt.Errorf("no root specified")
	}

	// Launch the background traversal workers
	for workerID := 0; workerID < s.numParallelWorkers; workerID++ {
		workerWG.Add(1)

		go func(workerID int) {
			defer workerWG.Done()

			if err := s.db.Session(ctx, func(ctx context.Context, driver database.Driver) error {
				for {
					if nextDescent, ok := channels.Receive(traversalCtx, segmentReaderC); !ok {
						return nil
					} else if s.memoryLimit > 0 && s.memoryLimit <= pathTree.SizeOf() {
						return fmt.Errorf("traversal memory limit reached - Limit: %.2f MB - Memory In-Use: %.2f MB", s.memoryLimit.Mebibytes(), pathTree.SizeOf().Mebibytes())
					} else {
						// Traverse the descending relationships of the current segment
						if descendingSegments, err := plan.Logic(traversalCtx, driver, nextDescent); err != nil {
							return err
						} else {
							for _, descendingSegment := range descendingSegments {
								// Add to the descent count before submitting to the channel
								descentCount.Add(1)
								channels.Submit(traversalCtx, segmentWriterC, descendingSegment)
							}
						}
					}

					// Mark descent for this segment as complete
					descentCount.Add(-1)

					if !channels.Submit(traversalCtx, completionC, struct{}{}) {
						return nil
					}
				}
			}); err != nil && !errors.Is(err, graph.ErrContextTimedOut) && !errors.Is(err, context.Canceled) {
				// A worker encountered a fatal error, kill the traversal context
				doneFunc()

				errorCollector.Add(fmt.Errorf("reader %d failed: %w", workerID, err))
			}
		}(workerID)
	}

	// Add to the descent wait group and then queue the root of the path tree for traversal
	descentCount.Add(1)
	if channels.Submit(traversalCtx, segmentWriterC, pathTree.Root) {
		for {
			if _, ok := channels.Receive(traversalCtx, completionC); !ok || descentCount.Load() == 0 {
				break
			}
		}
	}

	// Actively cancel the traversal context to force any idle workers to join and exit
	doneFunc()

	// Wait for all workers to exit
	workerWG.Wait()

	return errorCollector.Combined()
}

// SegmentFilter is a function type that takes a given path segment and returns true if further descent into the path
// is allowed.
type SegmentFilter = func(next *graph.PathSegment) bool

// SegmentVisitor is a function that receives a path segment as part of certain traversal strategies.
type SegmentVisitor = func(next *graph.PathSegment)

// UniquePathSegmentFilter is a SegmentFilter constructor that will allow a traversal to all unique paths. This is done
// by tracking edge IDs traversed in a bitmap.
func UniquePathSegmentFilter(delegate SegmentFilter) SegmentFilter {
	traversalBitmap := cardinality.ThreadSafeDuplex(cardinality.NewBitmap64())

	return func(next *graph.PathSegment) bool {
		// Bail on cycles
		if next.IsCycle() {
			return false
		}

		// Return if we've seen this edge before
		if !traversalBitmap.CheckedAdd(next.Edge.ID.Uint64()) {
			return false
		}

		// Pass this segment to the delegate if we've never seen it before
		return delegate(next)
	}
}

// AcyclicNodeFilter is a SegmentFilter constructor that will allow traversal to a node only once. It will ignore all
// but the first inbound or outbound edge that traverses to it.
func AcyclicNodeFilter(filter SegmentFilter) SegmentFilter {
	return func(next *graph.PathSegment) bool {
		// Bail on counting ourselves
		if next.IsCycle() {
			return false
		}

		// Descend only if we've never seen this node before.
		return filter(next)
	}
}

// A SkipLimitFilter is a function that represents a collection and descent filter for PathSegments. This function must
// return two boolean values:
//
// The first boolean value in the return tuple communicates to the FilteredSkipLimit SegmentFilter if the given
// PathSegment is eligible for collection and therefore should be counted when considering the traversal's skip and
// limit parameters.
//
// The second boolean value in the return tuple communicates to the FilteredSkipLimit SegmentFilter if the given
// PathSegment is eligible for further descent. When this value is true the path will be expanded further during
// traversal.
type SkipLimitFilter = func(next *graph.PathSegment) (bool, bool)

// FilteredSkipLimit is a SegmentFilter constructor that allows a caller to inform the skip-limit algorithm when a
// result was collected and if the traversal should continue to descend further during traversal.
func FilteredSkipLimit(filter SkipLimitFilter, visitorFilter SegmentVisitor, skip, limit int) SegmentFilter {
	var (
		shouldCollect = atomics.NewCounter(uint64(skip))
		atLimit       = atomics.NewCounter(uint64(limit))
	)

	return func(next *graph.PathSegment) bool {
		canCollect, shouldDescend := filter(next)

		if canCollect {
			// Check to see if this result should be skipped
			if skip == 0 || shouldCollect() {
				// If we should collect this result, check to see if we're already at a limit for the number of results
				if limit > 0 && atLimit() {
					slog.Debug(fmt.Sprintf("At collection limit, rejecting path: %s", graph.FormatPathSegment(next)))
					return false
				}

				slog.Debug(fmt.Sprintf("Collected path: %s", graph.FormatPathSegment(next)))
				visitorFilter(next)
			} else {
				slog.Debug(fmt.Sprintf("Skipping path visit: %s", graph.FormatPathSegment(next)))
			}
		}

		if shouldDescend {
			slog.Debug(fmt.Sprintf("Descending into path: %s", graph.FormatPathSegment(next)))
		} else {
			slog.Debug(fmt.Sprintf("Rejecting further descent into path: %s", graph.FormatPathSegment(next)))
		}

		return shouldDescend
	}
}
