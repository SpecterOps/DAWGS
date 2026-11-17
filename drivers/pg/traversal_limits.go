package pg

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

const (
	// traversalStateLimitSetting names PostgreSQL's candidate trail admission budget.
	traversalStateLimitSetting = "dawgs.traversal_state_limit"

	// traversalByteLimitSetting names PostgreSQL's cumulative estimated trail byte budget.
	traversalByteLimitSetting = "dawgs.traversal_byte_limit"

	// traversalLimitCleanupTimeout bounds restoration after the operation context is canceled.
	traversalLimitCleanupTimeout = 5 * time.Second

	// readTraversalLimitsSQL snapshots session settings, preserving an unset custom value as empty text.
	readTraversalLimitsSQL = `select
		coalesce(current_setting($1, true), ''),
		coalesce(current_setting($2, true), '')`

	// writeTraversalLimitsSQL changes only supplied settings at the requested transaction or session scope.
	writeTraversalLimitsSQL = `select
		case when $2::text is not null then set_config($1, $2, $5) end,
		case when $4::text is not null then set_config($3, $4, $5) end`
)

// traversalLimits stores independent PostgreSQL constructor overrides; zero inherits the session value.
type traversalLimits struct {
	// stateLimit overrides the number of admitted candidate trail states.
	stateLimit int64

	// byteLimit overrides the cumulative estimated trail bytes admitted by the query.
	byteLimit int64
}

// traversalLimitSettings preserves exact text values for configuring and restoring a leased session.
type traversalLimitSettings struct {
	// stateLimit holds the PostgreSQL state setting, including empty text for an unset value.
	stateLimit string

	// byteLimit holds the PostgreSQL byte setting, including empty text for an unset value.
	byteLimit string
}

// traversalLimitScope owns constructor overrides and the session values changed for one operation.
type traversalLimitScope struct {
	// limits selects the settings overridden by the driver constructor.
	limits traversalLimits

	// previous retains autocommit session values until the operation closes.
	previous *traversalLimitSettings
}

// validate rejects negative constructor overrides while reserving zero for inheritance.
func (s traversalLimits) validate() error {
	if s.stateLimit < 0 {
		return fmt.Errorf("TraversalStateLimit must not be negative: %d", s.stateLimit)
	} else if s.byteLimit < 0 {
		return fmt.Errorf("TraversalByteLimit must not be negative: %d", s.byteLimit)
	}

	return nil
}

// write changes only configured overrides, allowing restoration to an empty custom setting.
func (s traversalLimits) write(ctx context.Context, executor driver, settings traversalLimitSettings, local bool) error {
	var (
		stateValue any
		byteValue  any
	)

	if s.stateLimit > 0 {
		stateValue = settings.stateLimit
	}
	if s.byteLimit > 0 {
		byteValue = settings.byteLimit
	}

	_, err := executor.Exec(ctx, writeTraversalLimitsSQL,
		traversalStateLimitSetting, stateValue,
		traversalByteLimitSetting, byteValue, local)
	return err
}

// apply initializes operation defaults, saving session settings when there is no explicit transaction.
func (s *traversalLimitScope) apply(ctx context.Context, executor driver, local bool) error {
	if s.limits.stateLimit == 0 && s.limits.byteLimit == 0 {
		return nil
	}

	if !local {
		previous := &traversalLimitSettings{}
		if err := executor.QueryRow(ctx, readTraversalLimitsSQL,
			traversalStateLimitSetting, traversalByteLimitSetting).Scan(&previous.stateLimit, &previous.byteLimit); err != nil {
			return fmt.Errorf("read PostgreSQL traversal limits: %w", err)
		}

		// Retain the snapshot before writing so a failed setup also runs cleanup.
		s.previous = previous
	}

	settings := traversalLimitSettings{
		stateLimit: strconv.FormatInt(s.limits.stateLimit, 10),
		byteLimit:  strconv.FormatInt(s.limits.byteLimit, 10),
	}

	if err := s.limits.write(ctx, executor, settings, local); err != nil {
		return fmt.Errorf("set PostgreSQL traversal limits: %w", err)
	}

	return nil
}

// restore reinstates autocommit settings with a bounded context independent of operation cancellation.
func (s *traversalLimitScope) restore(ctx context.Context, executor driver) error {
	if s.previous == nil {
		return nil
	}

	var (
		previous           = *s.previous
		cleanupCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), traversalLimitCleanupTimeout)
	)
	defer cancel()
	s.previous = nil

	if err := s.limits.write(cleanupCtx, executor, previous, false); err != nil {
		return fmt.Errorf("restore PostgreSQL traversal limits: %w", err)
	}

	return nil
}
