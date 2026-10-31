//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestResultAliasSpelling(t *testing.T) {
	session := Open(t, Options{SkipIfNoConnection: true, CleanupMode: CleanupGraph, ExtraNodeKinds: graph.Kinds{graph.StringKind("AliasInput")}})
	ClearGraph(t, session.DB, session.Ctx)
	modes := []bool{true}
	if _, ok := session.DB.(*pg.Driver); ok {
		modes = append(modes, false)
	}
	for _, optimized := range modes {
		previous := pg.SetOptimizedTranslation(optimized)
		for _, tc := range []struct{ token, key string }{
			{"objectId", "objectId"},
			{"`object id`", "object id"},
			{"`object``id`", "object`id"},
			{"`object\"id`", "object\"id"},
			{"`select`", "select"},
			{"`étiquette`", "étiquette"},
			{"`*`", "*"},
		} {
			for _, query := range []string{
				"RETURN 7 AS " + tc.token,
				"WITH 7 AS " + tc.token + " RETURN " + tc.token + " AS " + tc.token + " ORDER BY " + tc.token,
			} {
				t.Run(query+map[bool]string{true: "/optimized", false: "/generic"}[optimized], func(t *testing.T) {
					require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
						result := tx.Query(query, nil)
						defer result.Close()
						require.True(t, result.Next(), "%v", result.Error())
						require.Equal(t, []string{tc.key}, result.Keys())
						require.Len(t, result.Values(), 1)
						require.EqualValues(t, 7, result.Values()[0])
						require.False(t, result.Next())
						return result.Error()
					}))
				})
			}
		}

		for _, tc := range []struct {
			query  string
			keys   []string
			values []any
		}{
			{"RETURN 1 AS objectId, 2 AS objectid", []string{"objectId", "objectid"}, []any{int64(1), int64(2)}},
			{"WITH 1 AS `a`, 2 AS `b` RETURN `a` + `b`", []string{"`a` + `b`"}, []any{int64(3)}},
		} {
			t.Run(tc.query+map[bool]string{true: "/optimized", false: "/generic"}[optimized], func(t *testing.T) {
				require.NoError(t, session.DB.ReadTransaction(session.Ctx, func(tx graph.Transaction) error {
					result := tx.Query(tc.query, nil)
					defer result.Close()
					require.True(t, result.Next(), "%v", result.Error())
					require.Equal(t, tc.keys, result.Keys())
					require.Len(t, result.Values(), len(tc.values))
					for idx, value := range tc.values {
						require.EqualValues(t, value, result.Values()[idx])
					}
					require.False(t, result.Next())
					return result.Error()
				}))
			})
		}
		pg.SetOptimizedTranslation(previous)
	}
}

func TestPostgreSQLResultAliasCompilerModes(t *testing.T) {
	runPostgreSQLTemplateCompilerModes(t, "result_alias_shapes")
}
