//go:build manual_integration

package integration

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestPostgreSQLNodeUpsertKindRemovalFailureRollsBackFlush(t *testing.T) {
	session := Open(t, Options{RequireDriver: pg.DriverName, SkipIfDriverMismatch: true, Schema: directWriteSchema(), CleanupMode: CleanupGraph})
	db, ctx := session.DB, session.Ctx
	target := directWriteCreateNode(t, ctx, db, directWriteProperties("objectid", "removal-failure", "preserved", true), directWriteUpsertNodeKind, directWriteUpsertNodeKindA)
	var graphID int32
	require.NoError(t, session.PGPool.QueryRow(ctx, "select id from graph where name = $1", "integration_test").Scan(&graphID))
	table := pgx.Identifier{fmt.Sprintf("node_%d", graphID)}.Sanitize()
	_, err := session.PGPool.Exec(ctx, fmt.Sprintf(`
create function variance_reject_kind_removal() returns trigger language plpgsql as $$
begin
    if new.id = %d and cardinality(new.kind_ids) < cardinality(old.kind_ids) then
        raise exception 'variance forced kind removal failure';
    end if;
    return new;
end $$`, target.ID))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := session.PGPool.Exec(ctx, "drop function variance_reject_kind_removal() cascade")
		require.NoError(t, err)
	})
	_, err = session.PGPool.Exec(ctx, "create trigger variance_kind_removal_guard before update on "+table+" for each row execute function variance_reject_kind_removal()")
	require.NoError(t, err)

	err = db.BatchOperation(ctx, func(batch graph.Batch) error {
		update := directWriteNodeUpdate("removal-failure", directWriteUpsertNodeKindB, directWriteProperties("changed", true))
		update.Node.DeleteKinds(directWriteUpsertNodeKindA)
		return batch.UpdateNodeBy(update)
	})
	require.ErrorContains(t, err, "variance forced kind removal failure")
	stored := directWriteFetchNodeByID(t, ctx, db, target.ID)
	require.True(t, stored.Kinds.ContainsOneOf(directWriteUpsertNodeKindA))
	require.False(t, stored.Kinds.ContainsOneOf(directWriteUpsertNodeKindB))
	require.False(t, stored.Properties.Exists("changed"))
	require.Equal(t, true, stored.Properties.Get("preserved").Any())
}
