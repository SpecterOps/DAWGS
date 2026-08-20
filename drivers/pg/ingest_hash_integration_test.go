//go:build manual_integration

package pg

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

const ingestHashDifferentialSeed int64 = 8675309

func TestPostgresIngestHashEquivalence(t *testing.T) {
	testDB := newPostgresIngestTestDatabase(t)
	ctx := testDB.ctx
	pool := testDB.pool

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	kindNames := []string{"DAWGSIngestHashKind", "DAWGSIngestHashÉquipe"}
	kindIDs := make([]int16, len(kindNames))
	for index, kindName := range kindNames {
		err := tx.QueryRow(ctx, `
			with inserted as (
			  insert into kind (name)
			  values ($1)
			  on conflict (name) do nothing
			  returning id
			)
			select id from inserted
			union all
			select id from kind where name = $1
			limit 1
		`, kindName).Scan(&kindIDs[index])
		require.NoError(t, err)
		require.Positive(t, kindIDs[index])
	}
	require.NotEqual(t, kindIDs[0], kindIDs[1])

	rows, err := tx.Query(ctx, `
		select name
		from kind
		where id = any($1::smallint[])
		order by convert_to(name, 'UTF8')
	`, kindIDs)
	require.NoError(t, err)
	actualKindNames, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	expectedKindNames := append([]string(nil), kindNames...)
	sort.Strings(expectedKindNames)
	require.Equal(t, expectedKindNames, actualKindNames)

	kinds := graph.Kinds{
		graph.StringKind(kindNames[0]),
		graph.StringKind(kindNames[1]),
	}
	for _, test := range ingestCanonicalDifferentialGoldens() {
		t.Run(test.name, func(t *testing.T) {
			value := decodeIngestDifferentialJSON(t, test.document)
			var goCanonical bytes.Buffer
			require.NoError(t, writeCanonicalIngestValue(&goCanonical, value))
			require.Equal(t, test.canonicalHex, hex.EncodeToString(goCanonical.Bytes()))

			var sqlCanonical []byte
			require.NoError(t, tx.QueryRow(ctx,
				"select dawgs_ingest_canonical_jsonb($1::jsonb)", test.document,
			).Scan(&sqlCanonical))
			require.Equal(t, goCanonical.Bytes(), sqlCanonical)

			properties := map[string]any{"value": value}
			comparePostgresIngestHashes(ctx, t, tx, kindIDs, kinds, properties,
				fmt.Sprintf("golden=%s json=%s", test.name, test.document))
		})
	}
	for name, properties := range map[string]map[string]any{
		"empty properties": {},
		"objectid exclusion": {
			"objectid": "S-1-5-21",
		},
		"objectid is the only node exclusion": {
			"name":     "alice",
			"objectid": "S-1-5-21",
		},
	} {
		comparePostgresIngestHashes(ctx, t, tx, kindIDs, kinds, properties, "hash golden="+name)
	}

	for _, number := range []string{"9e131071", "1e-16383", "0e1073741823"} {
		var goCanonical bytes.Buffer
		require.NoError(t, writeCanonicalIngestValue(&goCanonical, json.Number(number)))

		var sqlCanonical []byte
		require.NoError(t,
			tx.QueryRow(ctx, "select dawgs_ingest_canonical_number($1)", number).Scan(&sqlCanonical),
			number,
		)
		require.Equal(t, goCanonical.Bytes(), sqlCanonical, number)
	}

	random := rand.New(rand.NewSource(ingestHashDifferentialSeed))
	for index := range 10_000 {
		properties := randomNestedIngestDocument(random, index)
		encoded, err := json.Marshal(properties)
		require.NoError(t, err)
		comparePostgresIngestHashes(ctx, t, tx, kindIDs, kinds, properties,
			fmt.Sprintf("seed=%d case=%d json=%s", ingestHashDifferentialSeed, index, encoded))
	}

	require.NoError(t, tx.Rollback(ctx))

	var missingKindID int16
	require.NoError(t, pool.QueryRow(ctx, `
		select candidate::smallint
		from generate_series(1, 32767) as candidate
		left join kind on kind.id = candidate
		where kind.id is null
		limit 1
	`).Scan(&missingKindID))
	var hash []byte
	err = pool.QueryRow(ctx,
		"select dawgs_ingest_node_content_hash($1::smallint[], '{}'::jsonb)",
		[]int16{missingKindID},
	).Scan(&hash)
	require.Error(t, err, "missing kind mappings must be rejected")

	for _, number := range []string{
		"",
		"+1",
		"01",
		".1",
		"1.",
		"1e",
		"1e131072",
		"1e-16384",
		"0e-16384",
		"0e1073741824",
		"1." + strings.Repeat("0", 16384),
	} {
		err := pool.QueryRow(ctx, "select dawgs_ingest_canonical_number($1)", number).Scan(&hash)
		require.Error(t, err, "noncanonical or unrepresentable number %q must be rejected", number)
	}

	err = pool.QueryRow(ctx,
		"select dawgs_ingest_canonical_jsonb($1::jsonb)", `"before\u0000after"`,
	).Scan(&hash)
	require.Error(t, err, "PostgreSQL must reject JSON strings containing U+0000")

	for _, function := range []string{"dawgs_ingest_node_content_hash('{}'::smallint[], '[]'::jsonb)", "dawgs_ingest_edge_content_hash('[]'::jsonb)"} {
		err := pool.QueryRow(ctx, "select "+function).Scan(&hash)
		require.Error(t, err, "non-object properties must be rejected by %s", function)
	}
}

func comparePostgresIngestHashes(
	ctx context.Context,
	t *testing.T,
	tx pgx.Tx,
	kindIDs []int16,
	kinds graph.Kinds,
	properties map[string]any,
	detail string,
) {
	t.Helper()

	encoded, err := json.Marshal(properties)
	require.NoError(t, err, detail)

	goEdgeHash, err := hashIngestEdgeContent(properties)
	require.NoError(t, err, detail)
	goNodeHash, err := hashIngestNodeContent(kinds, properties)
	require.NoError(t, err, detail)

	var (
		sqlEdgeHash []byte
		sqlNodeHash []byte
	)
	err = tx.QueryRow(ctx, `
		select dawgs_ingest_edge_content_hash($1::jsonb),
		       dawgs_ingest_node_content_hash($2::smallint[], $1::jsonb)
	`, string(encoded), kindIDs).Scan(&sqlEdgeHash, &sqlNodeHash)
	require.NoError(t, err, detail)

	if !bytes.Equal(goEdgeHash[:], sqlEdgeHash) {
		t.Fatalf("edge content hash mismatch: %s\nGo:  %x\nSQL: %x", detail, goEdgeHash, sqlEdgeHash)
	}
	if !bytes.Equal(goNodeHash[:], sqlNodeHash) {
		t.Fatalf("node content hash mismatch: %s\nGo:  %x\nSQL: %x", detail, goNodeHash, sqlNodeHash)
	}
}

func ingestCanonicalDifferentialGoldens() []struct {
	name         string
	document     string
	canonicalHex string
} {
	return []struct {
		name         string
		document     string
		canonicalHex string
	}{
		{name: "null", document: `null`, canonicalHex: "00"},
		{name: "false", document: `false`, canonicalHex: "01"},
		{name: "true", document: `true`, canonicalHex: "02"},
		{name: "UTF-8 string", document: `"é"`, canonicalHex: "030000000000000002c3a9"},
		{name: "string bytes are not JSON escaped", document: `"\n\"\\"`, canonicalHex: "0300000000000000030a225c"},
		{name: "positive number", document: `1`, canonicalHex: "040000000000000000013100"},
		{name: "negative normalized number", document: `-12.300e2`, canonicalHex: "0401000000000000000331323302"},
		{name: "signed zero", document: `-0.00e+99`, canonicalHex: "040000000000000000013000"},
		{name: "large positive exponent", document: `1e131071`, canonicalHex: "0400000000000000000131feff0f"},
		{name: "large negative exponent", document: `1e-16383`, canonicalHex: "0400000000000000000131fdff01"},
		{name: "array preserves order", document: `[null,true]`, canonicalHex: "0500000000000000020002"},
		{
			name:     "nested object sorts keys by raw UTF-8 bytes",
			document: `{"é":[2,false],"z":"v"}`,
			canonicalHex: "060000000000000002" +
				"00000000000000017a03000000000000000176" +
				"0000000000000002c3a9050000000000000002" +
				"04000000000000000001320001",
		},
	}
}

func decodeIngestDifferentialJSON(t *testing.T, document string) any {
	t.Helper()

	decoder := json.NewDecoder(strings.NewReader(document))
	decoder.UseNumber()

	var value any
	require.NoError(t, decoder.Decode(&value))
	return value
}

func randomNestedIngestDocument(random *rand.Rand, index int) map[string]any {
	return map[string]any{
		"case": json.Number(strconv.Itoa(index)),
		"nested": []any{
			randomIngestJSONValue(random, 0),
			map[string]any{
				"left":  randomIngestJSONValue(random, 1),
				"right": randomIngestJSONValue(random, 1),
			},
		},
	}
}

func randomIngestJSONValue(random *rand.Rand, depth int) any {
	if depth >= 4 {
		return randomIngestJSONScalar(random)
	}

	switch random.Intn(8) {
	case 0, 1:
		return randomIngestJSONScalar(random)
	case 2, 3, 4:
		length := random.Intn(4)
		values := make([]any, length)
		for index := range values {
			values[index] = randomIngestJSONValue(random, depth+1)
		}
		return values
	default:
		valueCount := random.Intn(4)
		values := make(map[string]any, valueCount)
		for index := range valueCount {
			key := []string{"a", "z", "é", "東京", "escaped\nkey"}[random.Intn(5)] + strconv.Itoa(index)
			values[key] = randomIngestJSONValue(random, depth+1)
		}
		return values
	}
}

func randomIngestJSONScalar(random *rand.Rand) any {
	switch random.Intn(6) {
	case 0:
		return nil
	case 1:
		return random.Intn(2) == 1
	case 2:
		return []string{"", "plain", "é", "東京", "line\nquote\"slash\\"}[random.Intn(5)]
	case 3:
		return json.Number(strconv.FormatInt(random.Int63n(2_000_001)-1_000_000, 10))
	case 4:
		return json.Number(fmt.Sprintf("%d.%03d", random.Intn(2001)-1000, random.Intn(1000)))
	default:
		return json.Number(fmt.Sprintf("%de%d", random.Intn(1999)-999, random.Intn(41)-20))
	}
}
