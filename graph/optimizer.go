package graph

import "context"

// Optimizer is an optional capability that drivers may implement to perform
// backend-specific maintenance work, such as rebuilding fragmented indexes.
//
// Optimize is intended to be called periodically by the consumer (e.g. after
// an analysis cycle completes). Implementations must:
//
//   - Be safe to call repeatedly; consecutive calls on a healthy database
//     should be inexpensive (or no-ops).
//   - Honor ctx cancellation. Long-running maintenance must abort promptly
//     when the context is done.
//   - Avoid taking exclusive locks that would block normal read or write
//     traffic against the database for any meaningful duration.
//
// Drivers that do not have a meaningful notion of optimization should simply
// not implement this interface; consumers must type-assert before calling.
type Optimizer interface {
	Optimize(ctx context.Context) error
}
