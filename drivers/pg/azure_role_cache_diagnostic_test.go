package pg

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// These shapes mirror BHCE's UserRoleAssignments reads without importing BHCE.
func azureRoleDiagnosticBuilder(shape string, id graph.ID) *query.Builder {
	var (
		builder    = query.NewBuilder(nil)
		criteria   graph.Criteria
		projection = query.Start()
	)
	switch shape {
	case "group_members":
		criteria = query.And(
			query.And(
				query.Kind(query.Relationship(), graph.StringKind("AZMemberOf")),
				query.Kind(query.Start(), graph.StringKind("AZUser")),
				query.Equals(query.EndProperty("isassignabletorole"), "true"),
			),
			query.InIDs(query.EndID(), id),
		)
	case "role_members":
		criteria = query.And(
			query.And(query.KindIn(query.Relationship(), graph.StringKind("AZMemberOf"), graph.StringKind("AZHasRole"))),
			query.InIDs(query.EndID(), id),
		)
	case "tenant_roles":
		projection = query.End()
		criteria = query.And(
			query.Equals(query.StartID(), id),
			query.Kind(query.Relationship(), graph.StringKind("AZContains")),
			query.Kind(query.End(), graph.StringKind("AZRole")),
			query.In(query.EndProperty("roletemplateid"), []string{"role-template"}),
		)
	case "tracker":
		criteria = query.And(
			query.Not(query.KindIn(query.Start(), graph.StringKind("Meta"), graph.StringKind("MetaDetail"))),
			query.KindIn(query.Relationship(), graph.StringKind("AZResetPassword"), graph.StringKind("AZGlobalAdmin"), graph.StringKind("AZPrivilegedRoleAdmin"), graph.StringKind("AZPrivilegedAuthAdmin"), graph.StringKind("AZAddMembers")),
			query.Not(query.KindIn(query.End(), graph.StringKind("Meta"), graph.StringKind("MetaDetail"))),
		)
		builder.Apply(query.Where(criteria), query.Returning(query.Relationship()))
		return builder
	default:
		panic("unknown diagnostic shape")
	}
	builder.Apply(query.Where(criteria), query.Returning(query.Relationship(), projection))
	return builder
}

func azureRoleDiagnosticManager() *SchemaManager {
	manager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{TranslationCacheEntries: 256})
	kinds := map[graph.Kind]int16{}
	for idx, name := range []string{"AZMemberOf", "AZHasRole", "AZContains", "AZUser", "AZRole", "Meta", "MetaDetail", "AZResetPassword", "AZGlobalAdmin", "AZPrivilegedRoleAdmin", "AZPrivilegedAuthAdmin", "AZAddMembers"} {
		kinds[graph.StringKind(name)] = int16(idx + 1)
	}
	manager.kindCache.merge(kinds, 0)
	return manager
}

func TestAzureRoleDiagnosticCacheReuse(t *testing.T) {
	previous := SetOptimizedTranslation(true)
	t.Cleanup(func() { SetOptimizedTranslation(previous) })
	for _, shape := range []string{"group_members", "role_members", "tenant_roles", "tracker"} {
		t.Run(shape, func(t *testing.T) {
			manager := azureRoleDiagnosticManager()
			for id := graph.ID(1); id <= 100; id++ {
				regular, err := azureRoleDiagnosticBuilder(shape, id).Build(false)
				require.NoError(t, err)
				prepared, err := prepareRegularQuery(regular)
				require.NoError(t, err)
				require.True(t, prepared.identity.valid)
				sql, _, err := manager.compileRegularQuery(context.Background(), prepared, 1)
				require.NoError(t, err)
				if id == 1 {
					t.Logf("Cypher: %s\nSQL: %s", prepared.source, sql)
				}
			}
			stats := manager.compilationCache.Stats()
			t.Logf("cache: %+v", stats)
			require.Equal(t, int64(1), stats.Misses)
			require.Equal(t, int64(99), stats.Hits)
			require.Zero(t, stats.Bypasses)
		})
	}
}

func BenchmarkAzureRoleDiagnostic(b *testing.B) {
	for _, shape := range []string{"group_members", "role_members", "tenant_roles", "tracker"} {
		for _, mode := range []string{"cached", "uncached"} {
			b.Run(shape+"/"+mode, func(b *testing.B) {
				manager := azureRoleDiagnosticManager()
				if mode == "uncached" {
					manager.compilationCache = newTranslationCache(0)
					manager.compilationCacheProvider = sharedTranslationCacheProvider{cache: manager.compilationCache}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for idx := 0; idx < b.N; idx++ {
					regular, err := azureRoleDiagnosticBuilder(shape, graph.ID(idx+1)).Build(false)
					if err != nil {
						b.Fatal(err)
					}
					prepared, err := prepareRegularQuery(regular)
					if err != nil {
						b.Fatal(err)
					}
					if _, _, err := manager.compileRegularQuery(context.Background(), prepared, 1); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestAzureRoleDiagnosticMissingKindPreventsReuse(t *testing.T) {
	var (
		manager = azureRoleDiagnosticManager()
		mapper  = pgutil.NewInMemoryKindMapper()
	)
	// AZMemberOf still matches even when the other relationship kind is absent.
	mapper.Put(graph.StringKind("AZMemberOf"))
	for id := graph.ID(1); id <= 100; id++ {
		regular, err := azureRoleDiagnosticBuilder("role_members", id).Build(false)
		require.NoError(t, err)
		prepared, err := prepareRegularQuery(regular)
		require.NoError(t, err)
		_, _, err = manager.compileWithMapper(context.Background(), prepared.source, prepared.parameters, 1, &prepared.identity, prepared.translationQuery, mapper)
		require.NoError(t, err)
	}
	stats := manager.compilationCache.Stats()
	t.Logf("missing AZHasRole cache: %+v", stats)
	require.Equal(t, int64(100), stats.Misses)
	require.Equal(t, int64(100), stats.Bypasses)
	require.Zero(t, stats.Hits)
	require.Zero(t, stats.Insertions)
}

func BenchmarkAzureRoleDiagnosticIdentity(b *testing.B) {
	regular, err := azureRoleDiagnosticBuilder("group_members", 1).Build(false)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if !builderIdentity(regular).valid {
			b.Fatal("uncacheable identity")
		}
	}
}

func BenchmarkAzureRoleDiagnosticJSON(b *testing.B) {
	for _, decoder := range []struct {
		name      string
		unmarshal func([]byte, any) error
	}{
		{"standard", json.Unmarshal},
		{"exact_numbers", unmarshalJSONNumbers},
	} {
		b.Run(decoder.name, func(b *testing.B) {
			data := []byte(`{"objectid":"user-guid","name":"USER@TENANT","isassignabletorole":true,"enabled":true,"lastseen":1700000000,"values":[1,2,3]}`)
			b.ReportAllocs()
			for b.Loop() {
				var value any
				if err := decoder.unmarshal(data, &value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
