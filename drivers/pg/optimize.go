package pg

import "context"

// Optimize satisfies the graph.Optimizer interface. The body is currently a
// no-op stub; the actual index maintenance logic (assessment via pgstattuple
// followed by REINDEX CONCURRENTLY of bloated indexes) is implemented in a
// later phase.
func (s *Driver) Optimize(ctx context.Context) error {
	return nil
}
