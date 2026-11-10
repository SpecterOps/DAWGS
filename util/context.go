package util

import "context"

// IsContextLive reports whether ctx has neither been canceled nor reached its deadline.
func IsContextLive(ctx context.Context) bool {
	return ctx.Err() == nil
}
