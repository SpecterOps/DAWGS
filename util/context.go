package util

import "context"

func IsContextLive(ctx context.Context) bool {
	return ctx.Err() == nil
}
