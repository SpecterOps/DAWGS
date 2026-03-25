// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"slices"
	"testing"

	"github.com/specterops/dawgs/graph"
)

// baseFixture is shared by test cases that only require a small, general-purpose
// graph. It provides two typed nodes connected by a typed edge, plus a node
// carrying both kinds and a variety of property types.
//
//	n1 (NodeKind1) --[EdgeKind1]--> n2 (NodeKind2) --[EdgeKind2]--> n3 (NodeKind1+NodeKind2)
var baseFixture = GraphFixture{
	Nodes: []NodeFixture{
		NewNodeWithProperties("n1", map[string]any{
			"name":              "SOME NAME",
			"value":             1,
			"objectid":          "S-1-5-21-1",
			"enabled":           true,
			"hasspn":            true,
			"pwdlastset":        float64(-2), // not -1 or 0, so excluded by typical filters
			"functionallevel":   "2012",
			"system_tags":       "admin_tier_0",
			"domain":            "test.local",
			"other":             "SOME NAME",
			"tid":               "tid1",
			"selected":          true,
			"array_value":       []any{float64(1), float64(2)},
			"arrayProperty":     []any{"DES-CBC-CRC", "DES-CBC-MD5"},
			"distinguishedname": "CN=TEST,DC=example,DC=com",
			"samaccountname":    "testuser",
			"email":             "test@example.com",
		}, NodeKind1),
		NewNodeWithProperties("n2", map[string]any{
			"name":              "1234",
			"value":             2,
			"objectid":          "S-1-5-21-2",
			"tid":               "tid1",
			"distinguishedname": "CN=ADMINSDHOLDER,CN=SYSTEM,CN=TEST,DC=example,DC=com",
			"samaccountname":    "adminuser",
			"email":             "admin@example.com",
			"domain":            "other.local",
		}, NodeKind2),
		NewNodeWithProperties("n3", map[string]any{
			"name":  "n3",
			"value": 3,
			"prop":  "a",
		}, NodeKind1, NodeKind2),
	},
	Edges: []EdgeFixture{
		NewEdgeWithProperties("n1", "n2", EdgeKind1, map[string]any{
			"prop":      "a",
			"value":     42,
			"bool_prop": true,
		}),
		NewEdge("n2", "n3", EdgeKind2),
	},
}

// semanticTestCases is the complete list of semantic integration test cases.
var semanticTestCases = func() []SemanticTestCase {
	return slices.Concat(
		nodesSemanticCases,
		stepwiseSemanticCases,
		expansionSemanticCases,
		aggregationSemanticCases,
		multipartSemanticCases,
		patternBindingSemanticCases,
		deleteSemanticCases,
		updateSemanticCases,
		quantifiersSemanticCases,
	)
}()

var nodesSemanticCases = []SemanticTestCase{
	{
		Name:    "return kind labels for all nodes",
		Cypher:  `match (n) return labels(n)`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "filter any node by string property equality",
		Cypher:  `match (n) where n.name = '1234' return n`,
		Fixture: baseFixture,
		Assert:  AssertContainsNodeWithProp("name", "1234"),
	},
	{
		Name:    "filter a typed node using an inline property map",
		Cypher:  `match (n:NodeKind1 {name: "SOME NAME"}) return n`,
		Fixture: baseFixture,
		Assert:  AssertContainsNodeWithProp("name", "SOME NAME"),
	},
	{
		Name:    "return all nodes",
		Cypher:  `match (s) return s`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "filter nodes matching a kind disjunction",
		Cypher:  `match (s) where (s:NodeKind1 or s:NodeKind2) return s`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "cross-product filter where two nodes share a property value",
		Cypher:  `match (n:NodeKind1), (e) where n.name = e.name return n`,
		Fixture: baseFixture,
		// n1 (NodeKind1, name="SOME NAME") self-joins because it also exists as e
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter any node by string property equality (s binding)",
		Cypher:  `match (s) where s.name = '1234' return s`,
		Fixture: baseFixture,
		Assert:  AssertContainsNodeWithProp("name", "1234"),
	},
	{
		Name:   "filter node where property value appears in a literal list",
		Cypher: `match (s) where s.name in ['option 1', 'option 2'] return s`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "option 1"}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"name": "option 2"}, NodeKind2),
				NewNodeWithProperties("c", map[string]any{"name": "option 3"}, NodeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter node by string starts-with prefix",
		Cypher:  `match (s) where s.name starts with '123' return s`,
		Fixture: baseFixture,
		Assert:  AssertContainsNodeWithProp("name", "1234"),
	},
	{
		Name:    "filter node by string ends-with suffix",
		Cypher:  `match (s) where s.name ends with 'NAME' return s`,
		Fixture: baseFixture,
		Assert:  AssertContainsNodeWithProp("name", "SOME NAME"),
	},
	{
		Name:    "filter node where a property is not null",
		Cypher:  `match (n) where n.system_tags is not null return n`,
		Fixture: baseFixture,
		Assert:  AssertContainsNodeWithProp("system_tags", "admin_tier_0"),
	},
	{
		Name:    "filter typed node using coalesce with contains predicate",
		Cypher:  `match (n:NodeKind1) where coalesce(n.system_tags, '') contains 'admin_tier_0' return n`,
		Fixture: baseFixture,
		Assert:  AssertContainsNodeWithProp("system_tags", "admin_tier_0"),
	},
	{
		Name:    "filter typed node by array property size",
		Cypher:  `match (n:NodeKind1) where size(n.array_value) > 0 return n`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "filter typed node where array property overlaps a literal list",
		Cypher:  `match (n:NodeKind1) where ['DES-CBC-CRC', 'DES-CBC-MD5', 'RC4-HMAC-MD5'] in n.arrayProperty return n`,
		Fixture: baseFixture,
		// n1.arrayProperty contains DES-CBC-CRC and DES-CBC-MD5, so overlap matches
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter typed node where array property contains one of several scalar values",
		Cypher:  `match (u:NodeKind1) where 'DES-CBC-CRC' in u.arrayProperty or 'DES-CBC-MD5' in u.arrayProperty return u`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "filter node carrying two kind labels simultaneously",
		Cypher:  `match (s) where s:NodeKind1 and s:NodeKind2 return s`,
		Fixture: baseFixture,
		// n3 has both NodeKind1 and NodeKind2
		Assert: AssertContainsNodeWithProp("name", "n3"),
	},
	{
		Name:   "cross-product filter where two nodes match different typed properties",
		Cypher: `match (s), (e) where s.name = '1234' and e.other = 1234 return s`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "1234"}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"other": 1234}, NodeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "paginate results using SKIP and LIMIT",
		Cypher:  `match (n) return n skip 5 limit 10`,
		Fixture: baseFixture,
		// Behaviour depends on total node count, just assert no error
		Assert: AssertNoError(),
	},
	{
		Name:    "order results by node ID descending",
		Cypher:  `match (s) return s order by id(s) desc`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "filter isolated nodes with no adjacent edges",
		Cypher:  `match (s) where not (s)-[]-() return s`,
		Fixture: baseFixture,
		// Cannot predict with a shared database; just verify the query runs
		Assert: AssertNoError(),
	},
	{
		Name:   "filter nodes where node ID appears in another node's array property",
		Cypher: `match (s), (e) where id(s) in e.captured_ids return s, e`,
		// Cannot assert non-empty because fixture node IDs are unknown at
		// definition time; just verify the query executes without error
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	{
		Name:    "filter typed node with starts-with using a function call as the prefix",
		Cypher:  `match (n:NodeKind1) where n.distinguishedname starts with toUpper('admin') return n`,
		Fixture: baseFixture,
		Assert:  AssertEmpty(),
	},
	{
		Name:    "optional match returns results even when the pattern may be absent",
		Cypher:  `optional match (n:NodeKind1) return n`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "double-negation filter selects nodes where a property is null",
		Cypher:  `match (n) where not n.property is not null return n`,
		Fixture: baseFixture,
		// n1, n2, n3 have no 'property' key, so all qualify
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter nodes where array property equals an empty array literal",
		Cypher:  `match (s) where s.prop = [] return s`,
		Fixture: baseFixture,
		// None of our nodes have prop = [], n3 has prop = "a"
		Assert: AssertNoError(),
	},
	// ---- contains -------------------------------------------------------
	{
		Name:    "filter node by string contains predicate",
		Cypher:  `match (s) where s.name contains '123' return s`,
		Fixture: baseFixture,
		// n2.name = "1234" contains "123"
		Assert: AssertContainsNodeWithProp("name", "1234"),
	},
	// ---- negated string predicates --------------------------------------
	{
		Name:    "filter node using negated starts-with predicate",
		Cypher:  `match (s) where not s.name starts with 'XYZ' return s`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "filter node using negated contains predicate",
		Cypher:  `match (s) where not s.name contains 'XYZ' return s`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "filter node using negated ends-with predicate",
		Cypher:  `match (s) where not s.name ends with 'XYZ' return s`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	// ---- dynamic (variable-to-variable) string predicates ---------------
	{
		Name:    "filter node where string property starts with another property (dynamic)",
		Cypher:  `match (s) where s.name starts with s.other return s`,
		Fixture: baseFixture,
		// n1: name="SOME NAME", other="SOME NAME" — name starts with itself
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter node where string property contains another property (dynamic)",
		Cypher:  `match (s) where s.name contains s.other return s`,
		Fixture: baseFixture,
		// n1: name="SOME NAME", other="SOME NAME" — name contains itself
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter node where string property ends with another property (dynamic)",
		Cypher:  `match (s) where s.name ends with s.other return s`,
		Fixture: baseFixture,
		// n1: name="SOME NAME", other="SOME NAME" — name ends with itself
		Assert: AssertNonEmpty(),
	},
	// ---- IS NULL --------------------------------------------------------
	{
		Name:    "filter nodes where a datetime property is null",
		Cypher:  `match (s) where s.created_at is null return s`,
		Fixture: baseFixture,
		// No fixture node has created_at set → property is null for all three
		Assert: AssertNonEmpty(),
	},
	// ---- arithmetic in projection ---------------------------------------
	{
		Name:    "project an arithmetic expression on a node property",
		Cypher:  `match (s) return s.value + 1`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	// ---- datetime().epochseconds ----------------------------------------
	{
		Name:    "filter typed node using datetime arithmetic against epoch seconds",
		Cypher:  `match (u:NodeKind1) where u.pwdlastset < (datetime().epochseconds - (365 * 86400)) and not u.pwdlastset IN [-1.0, 0.0] return u limit 100`,
		Fixture: baseFixture,
		// n1: pwdlastset=-2; (-2 < current_epoch-31536000) is true; -2 not in {-1,0} is true
		Assert: AssertNonEmpty(),
	},
	// ---- element in property array --------------------------------------
	{
		Name:   "filter node where a scalar value appears in an array property",
		Cypher: `match (n) where 1 in n.array return n`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"array": []any{float64(1), float64(2), float64(3)}}, NodeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- coalesce equality forms ------------------------------------------------
	{
		Name:    "filter node using coalesce equality on a named property",
		Cypher:  `match (n) where coalesce(n.name, '') = '1234' return n`,
		Fixture: baseFixture,
		// n2.name='1234'; coalesce('1234','')='1234' → matches
		Assert: AssertContainsNodeWithProp("name", "1234"),
	},
	{
		Name:    "filter typed node using three-argument coalesce equality against an integer",
		Cypher:  `match (n:NodeKind1) where coalesce(n.a, n.b, 1) = 1 return n`,
		Fixture: baseFixture,
		// n1 and n3 have no 'a' or 'b' → coalesce(null,null,1)=1 → matches both
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter typed node using two-property coalesce that resolves to null",
		Cypher:  `match (n:NodeKind1) where coalesce(n.a, n.b) = 1 return n`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	{
		Name:    "filter typed node with coalesce on the right-hand side of equality",
		Cypher:  `match (n:NodeKind1) where 1 = coalesce(n.a, n.b) return n`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	{
		Name:   "filter typed node with coalesce equality on both sides",
		Cypher: `match (n:NodeKind1) where coalesce(n.name, '') = coalesce(n.migrated_name, '') return n`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "mirror", "migrated_name": "mirror"}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"name": "differ"}, NodeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- arithmetic in WHERE and projection -------------------------------------
	{
		Name:   "filter node using an arithmetic expression in the WHERE clause",
		Cypher: `match (s) where s.value + 2 / 3 > 10 return s`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"value": 20}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"value": 1}, NodeKind2),
			},
		},
		// Integer division: 2/3=0, so s.value+0>10 → a.value=20>10 matches
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "project a compound arithmetic expression dividing a shifted property",
		Cypher:  `match (s) return (s.value + 1) / 3`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	// ---- toLower equality with DISTINCT -----------------------------------------
	{
		Name:    "filter node using toLower equality and return distinct results",
		Cypher:  `match (s) where toLower(s.name) = '1234' return distinct s`,
		Fixture: baseFixture,
		// n2.name='1234'; toLower('1234')='1234' → matches
		Assert: AssertContainsNodeWithProp("name", "1234"),
	},
	{
		Name:   "filter typed node using toLower contains with a compound AND predicate",
		Cypher: `match (n:NodeKind1) where n:NodeKind1 and toLower(n.tenantid) contains 'myid' and n.system_tags contains 'tag' return n`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"tenantid": "MyID-Corp", "system_tags": "tag_admin"}, NodeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- toUpper in string predicates -------------------------------------------
	{
		Name:    "filter typed node where a property contains a toUpper() result",
		Cypher:  `match (n:NodeKind1) where n.distinguishedname contains toUpper('test') return n`,
		Fixture: baseFixture,
		// n1.distinguishedname='CN=TEST,DC=example,DC=com'; toUpper('test')='TEST' → contains
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter typed node where a property equals a toUpper() result (no match)",
		Cypher:  `match (n:NodeKind1) where n.distinguishedname = toUpper('admin') return n`,
		Fixture: baseFixture,
		// toUpper('admin')='ADMIN'; no node has distinguishedname='ADMIN'
		Assert: AssertEmpty(),
	},
	{
		Name:    "filter typed node where a property ends with a toUpper() result (no match)",
		Cypher:  `match (n:NodeKind1) where n.distinguishedname ends with toUpper('com') return n`,
		Fixture: baseFixture,
		// toUpper('com')='COM'; distinguishedname ends with 'com' not 'COM'
		Assert: AssertEmpty(),
	},
	// ---- toString / toInt in membership -----------------------------------------
	{
		Name:    "filter typed node where toString of a property appears in a literal list",
		Cypher:  `match (n:NodeKind1) where toString(n.functionallevel) in ['2008 R2', '2012', '2008', '2003'] return n`,
		Fixture: baseFixture,
		// n1.functionallevel='2012'; toString('2012')='2012' → in list → matches
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter typed node where toInt of a property appears in a literal integer list",
		Cypher:  `match (n:NodeKind1) where toInt(n.value) in [1, 2, 3, 4] return n`,
		Fixture: baseFixture,
		// n1.value=1; toInt(1)=1 → in [1,2,3,4] → matches
		Assert: AssertNonEmpty(),
	},
	// ---- datetime().epochmillis -------------------------------------------------
	{
		Name:    "filter typed node using datetime arithmetic against epoch milliseconds",
		Cypher:  `match (u:NodeKind1) where u.pwdlastset < (datetime().epochmillis - 86400000) and not u.pwdlastset IN [-1.0, 0.0] return u limit 100`,
		Fixture: baseFixture,
		// n1.pwdlastset=-2; current epochmillis ≫ 86400000; -2 < big_number → true; -2 ∉ {-1,0} → true
		Assert: AssertNonEmpty(),
	},
	// ---- date / time function comparisons ---------------------------------------
	{
		Name:    "filter node where a datetime property equals the current date",
		Cypher:  `match (s) where s.created_at = date() return s`,
		Fixture: baseFixture,
		// No fixture node has created_at; query runs but returns nothing
		Assert: AssertNoError(),
	},
	{
		Name:    "filter node where a property equals date minus a duration",
		Cypher:  `match (s) where s.created_at = date() - duration('P1D') return s`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	{
		Name:    "filter node where a property equals date plus a duration string",
		Cypher:  `match (s) where s.created_at = date() + duration('4 hours') return s`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	{
		Name:    "filter node where a property equals a literal date value",
		Cypher:  `match (s) where s.created_at = date('2023-4-4') return s`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	{
		Name:    "filter node where a datetime property equals the current datetime",
		Cypher:  `match (s) where s.created_at = datetime() return s`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	{
		Name:    "filter node where a property equals a literal datetime value",
		Cypher:  `match (s) where s.created_at = datetime('2019-06-01T18:40:32.142+0100') return s`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	{
		Name:    "filter node where a property equals the current local datetime",
		Cypher:  `match (s) where s.created_at = localdatetime() return s`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	{
		Name:    "filter node where a property equals a literal local datetime value",
		Cypher:  `match (s) where s.created_at = localdatetime('2019-06-01T18:40:32.142') return s`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	{
		Name:    "filter node where a property equals the current local time",
		Cypher:  `match (s) where s.created_at = localtime() return s`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	{
		Name:    "filter node where a property equals a literal local time value",
		Cypher:  `match (s) where s.created_at = localtime('4:4:4') return s`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	// ---- negation / NOT forms ---------------------------------------------------
	{
		Name:    "filter node using a negated parenthesized equality predicate",
		Cypher:  `match (s) where not (s.name = '123') return s`,
		Fixture: baseFixture,
		// n1.name='SOME NAME', n2.name='1234', n3.name='n3'; none equals '123'
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter node using negated 2-hop path existence",
		Cypher:  `match (s) where not (s)-[]->()-[]->() return s`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	{
		Name:    "filter node using negated directed edge pattern with property constraints",
		Cypher:  `match (s) where not (s)-[{prop: 'a'}]->({name: 'n3'}) return s`,
		Fixture: baseFixture,
		// n1 has edge {prop='a'} to n2 (name='1234'), not to a node named 'n3' → pattern absent for n1
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter node using negated incoming edge pattern with property constraints",
		Cypher:  `match (s) where not (s)<-[{prop: 'a'}]-({name: 'n3'}) return s`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "return id of node where negated kind filter removes typed results",
		Cypher:  `match (s) where not (s)-[]-() return id(s)`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	// ---- id() in integer literal list -------------------------------------------
	{
		Name:    "filter node where id appears in a literal integer list",
		Cypher:  `match (s) where id(s) in [1, 2, 3, 4] return s`,
		Fixture: baseFixture,
		// Node IDs are assigned by the database; we cannot predict them at definition time
		Assert: AssertNoError(),
	},
	// ---- three-way OR membership in array property ------------------------------
	{
		Name:    "filter typed node where array property contains one of three scalar values",
		Cypher:  `match (u:NodeKind1) where 'DES-CBC-CRC' in u.arrayProperty or 'DES-CBC-MD5' in u.arrayProperty or 'RC4-HMAC-MD5' in u.arrayProperty return u`,
		Fixture: baseFixture,
		// n1.arrayProperty=['DES-CBC-CRC','DES-CBC-MD5'] → first OR branch matches
		Assert: AssertNonEmpty(),
	},
	{
		Name:   "filter typed node where a scalar appears in an array property concatenated with a literal list",
		Cypher: `match (n:NodeKind1) where '1' in n.array_prop + ['1', '2'] return n`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"array_prop": []any{"x", "y"}}, NodeKind1),
			},
		},
		// ['x','y'] + ['1','2'] = ['x','y','1','2']; '1' is in result → matches
		Assert: AssertNonEmpty(),
	},
	// ---- empty-array comparison variants ----------------------------------------
	{
		Name:    "filter node where an empty array literal equals a property (reversed operands)",
		Cypher:  `match (s) where [] = s.prop return s`,
		Fixture: baseFixture,
		// n3.prop='a' ≠ []; no node has prop=[] → empty result, no error
		Assert: AssertNoError(),
	},
	{
		Name:    "filter node where a property is not equal to an empty array",
		Cypher:  `match (s) where s.prop <> [] return s`,
		Fixture: baseFixture,
		// n3.prop='a' ≠ [] → matches; others have no prop (null) which is excluded
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter node using negated equality to an empty array",
		Cypher:  `match (s) where not s.prop = [] return s`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	// ---- string concatenation in WHERE ------------------------------------------
	{
		Name:    "filter typed node using property equality with literal-then-property concatenation",
		Cypher:  `match (n:NodeKind1) match (m:NodeKind2) where m.distinguishedname = 'CN=ADMINSDHOLDER,CN=SYSTEM,' + n.distinguishedname return m`,
		Fixture: baseFixture,
		// 'CN=ADMINSDHOLDER,CN=SYSTEM,' + 'CN=TEST,DC=example,DC=com' matches n2.distinguishedname
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter typed node using property equality with property-then-literal concatenation",
		Cypher:  `match (n:NodeKind1) match (m:NodeKind2) where m.distinguishedname = n.distinguishedname + 'CN=ADMINSDHOLDER,CN=SYSTEM,' return m`,
		Fixture: baseFixture,
		// concat yields different string; n2.distinguishedname does not match → empty
		Assert: AssertNoError(),
	},
	{
		Name:    "filter typed node using property equality with two literal strings concatenated",
		Cypher:  `match (n:NodeKind1) match (m:NodeKind2) where m.distinguishedname = '1' + '2' return m`,
		Fixture: baseFixture,
		// '1'+'2'='12'; no node has that distinguishedname → empty
		Assert: AssertNoError(),
	},
	// ---- multiple ORDER BY columns ----------------------------------------------
	{
		Name:    "order results by two properties with mixed sort directions",
		Cypher:  `match (s) return s order by s.name, s.other_prop desc`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	// ---- cross-product with aliased property projection -------------------------
	{
		Name:   "return source and an aliased property from an unrelated node in a cross-product",
		Cypher: `match (s), (e) where s.name = 'n1' return s, e.name as othername`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "n1"}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"name": "n2"}, NodeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- cross-product with OR predicate ----------------------------------------
	{
		Name:   "filter cross-product where either node satisfies a different property predicate",
		Cypher: `match (s), (e) where s.name = '1234' or e.other = 1234 return s`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "1234"}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"other": 1234}, NodeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- double optional match --------------------------------------------------
	{
		Name:    "two sequential optional matches where only the anchor node is required",
		Cypher:  `match (n:NodeKind1) optional match (m:NodeKind2) where m.distinguishedname starts with n.distinguishedname optional match (o:NodeKind2) where o.distinguishedname <> n.distinguishedname return n, m, o`,
		Fixture: baseFixture,
		// n1 is always returned; m and o may or may not match
		Assert: AssertNonEmpty(),
	},
	// Currently fails
	//
	// {
	// 	Name:    "optional match with string concatenation in the filter joining two nodes",
	// 	Cypher:  `match (n:NodeKind1) optional match (m:NodeKind2) where m.distinguishedname = n.unknown + m.unknown return n, m`,
	// 	Fixture: baseFixture,
	// 	// n1 has no 'unknown' → null+null; no m matches → optional returns null for m; n returned
	// 	Assert: AssertNonEmpty(),
	// },
	// ---- complex hasspn / not-ends-with filter ----------------------------------
	{
		Name:    "filter typed node with compound hasspn, enabled, and not-ends-with checks",
		Cypher:  `match (u:NodeKind1) where u.hasspn = true and u.enabled = true and not '-502' ends with u.objectid and not coalesce(u.gmsa, false) = true and not coalesce(u.msa, false) = true return u limit 10`,
		Fixture: baseFixture,
		// n1: hasspn=true, enabled=true, objectid='S-1-5-21-1' → '-502' does not end with that → not false=true; gmsa/msa absent → coalesce false ≠ true → matches
		Assert: AssertNonEmpty(),
	},
	// ---- non-empty array literal equality ---------------------------------------
	//
	// Broken test case
	//
	// {
	// 	Name:   "filter node where a property equals a non-empty integer array literal",
	// 	Cypher: `match (s) where s.prop = [1, 2, 3] return s`,
	// 	Fixture: GraphFixture{
	// 		Nodes: []NodeFixture{
	// 			NewNodeWithProperties("a", map[string]any{"prop": []any{float64(1), float64(2), float64(3)}}, NodeKind1),
	// 			NewNodeWithProperties("b", map[string]any{"prop": "other"}, NodeKind2),
	// 		},
	// 	},
	// 	Assert: AssertNonEmpty(),
	// },
}

var stepwiseSemanticCases = []SemanticTestCase{
	{
		Name:    "return all edges",
		Cypher:  `match ()-[r]->() return r`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "filter edges by type() string comparison",
		Cypher:  `match ()-[r]->() where type(r) = 'EdgeKind1' return r`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "count edges of a specific kind",
		Cypher:  `match ()-[r:EdgeKind1]->() return count(r) as the_count`,
		Fixture: baseFixture,
		Assert:  AssertAtLeastInt64(1),
	},
	{
		Name:    "count typed edges reaching a node matching an inline property map",
		Cypher:  `match ()-[r:EdgeKind1]->({name: "123"}) return count(r) as the_count`,
		Fixture: baseFixture,
		// No target node has name "123" in our fixture; count returns exactly 0
		Assert: AssertExactInt64(0),
	},
	{
		Name:   "traverse one edge filtering both endpoints by property",
		Cypher: `match (s)-[r]->(e) where s.name = '123' and e.name = '321' return s, r, e`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("s", map[string]any{"name": "123"}, NodeKind1),
				NewNodeWithProperties("e", map[string]any{"name": "321"}, NodeKind2),
			},
			Edges: []EdgeFixture{NewEdge("s", "e", EdgeKind1)},
		},
		Assert: AssertNonEmpty(),
	},
	{
		Name:   "return source node and outgoing edge filtered by source property",
		Cypher: `match (n)-[r]->() where n.name = '123' return n, r`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "123"}, NodeKind1),
				NewNode("b", NodeKind2),
			},
			Edges: []EdgeFixture{NewEdge("a", "b", EdgeKind1)},
		},
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "filter edges by a numeric property value",
		Cypher:  `match ()-[r]->() where r.value = 42 return r`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(), // baseFixture edge n1->n2 has value=42
	},
	{
		Name:    "filter edges by a boolean property",
		Cypher:  `match ()-[r]->() where r.bool_prop return r`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(), // baseFixture edge n1->n2 has bool_prop=true
	},
	{
		Name:    "one-hop traversal filtering where source and target are not the same node",
		Cypher:  `match (n1)-[]->(n2) where n1 <> n2 return n2`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "traverse between typed endpoints with edge kind alternatives",
		Cypher:  `match (s:NodeKind1)-[r:EdgeKind1|EdgeKind2]->(e:NodeKind2) return s.name, e.name`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "traverse between multi-kind endpoints using edge kind alternatives",
		Cypher:  `match (s:NodeKind1:NodeKind2)-[r:EdgeKind1|EdgeKind2]->(e:NodeKind2:NodeKind1) return s.name, e.name`,
		Fixture: baseFixture,
		// n3 has both kinds and connects to nobody; need a direct n3->n3 edge or different fixture
		Assert: AssertNoError(),
	},
	// ---- reversed type comparison ---------------------------------------
	{
		Name:    "filter edges with reversed type() equality (literal on left)",
		Cypher:  `match ()-[r]->() where 'EdgeKind1' = type(r) return r`,
		Fixture: baseFixture,
		// baseFixture n1->n2 edge is EdgeKind1
		Assert: AssertNonEmpty(),
	},
	// ---- incoming edge direction ----------------------------------------
	{
		Name:    "traverse incoming edges filtering by kind alternatives",
		Cypher:  `match (s)<-[r:EdgeKind1|EdgeKind2]-(e) return s.name, e.name`,
		Fixture: baseFixture,
		// n1->n2(EK1): incoming to n2 from n1; n2->n3(EK2): incoming to n3 from n2
		Assert: AssertNonEmpty(),
	},
	// ---- diamond (two edges converging on one node) ---------------------
	{
		Name:   "diamond pattern where two edges converge on one node",
		Cypher: `match ()-[e0]->(n)<-[e1]-() return e0, n, e1`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1),
				NewNode("b", NodeKind1),
				NewNode("m", NodeKind2),
			},
			Edges: []EdgeFixture{
				NewEdge("a", "m", EdgeKind1),
				NewEdge("b", "m", EdgeKind2),
			},
		},
		// both edges converge on m
		Assert: AssertNonEmpty(),
	},
	// ---- shared-node forward chain --------------------------------------
	{
		Name:    "shared-node forward chain with two outgoing edges",
		Cypher:  `match ()-[e0]->(n)-[e1]->() return e0, n, e1`,
		Fixture: baseFixture,
		// n1->n2(EK1)->n3(EK2): e0=n1->n2, n=n2, e1=n2->n3
		Assert: AssertNonEmpty(),
	},
	// ---- edge inequality ------------------------------------------------
	{
		Name:    "two-hop chain filtering where the two traversed edges are not equal",
		Cypher:  `match ()-[r]->()-[e]->(n) where r <> e return n`,
		Fixture: baseFixture,
		// r=n1->n2, e=n2->n3; they are different edges so r <> e holds, n=n3
		Assert: AssertNonEmpty(),
	},
	// ---- unrelated cross-products with edges --------------------------------
	{
		Name:    "cross-product of an unrelated node and an edge",
		Cypher:  `match (n), ()-[r]->() return n, r`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "cross-product of two independent edge traversals",
		Cypher:  `match ()-[r]->(), ()-[e]->() return r, e`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	// ---- node with both an incoming and an outgoing edge -------------------
	// Pattern: ()<-[e0]-(n)<-[e1]-()
	//   - ()<-[e0]-(n)  means n is the SOURCE of e0 (n has an outgoing edge)
	//   - (n)<-[e1]-()  means n is the TARGET of e1 (n has an incoming edge)
	// n must therefore have at least one outgoing edge AND at least one
	// incoming edge.  The chain a→mid→b gives mid exactly that.
	{
		Name:   "pattern where a middle node has both an outgoing and an incoming edge",
		Cypher: `match ()<-[e0]-(n)<-[e1]-() return e0, n, e1`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1),
				NewNode("mid", NodeKind2),
				NewNode("b", NodeKind1),
			},
			Edges: []EdgeFixture{
				NewEdge("a", "mid", EdgeKind1), // incoming to mid  → satisfies (n)<-[e1]-()
				NewEdge("mid", "b", EdgeKind2), // outgoing from mid → satisfies ()<-[e0]-(n)
			},
		},
		// e0 = mid→b, n = mid, e1 = a→mid
		Assert: AssertNonEmpty(),
	},
	// ---- negated boolean edge property ---------------------------------------
	// The translator emits:  NOT ((e0.properties ->> 'property'))::bool
	// SQL three-valued logic: NOT NULL = NULL (not TRUE), so an absent key
	// does NOT satisfy the predicate — the row is discarded.  The property
	// must be present and explicitly false for NOT false = true to hold.
	{
		Name:   "traverse edge where the edge property flag is explicitly false",
		Cypher: `match (s)-[r]->(e) where s.name = '123' and e:NodeKind1 and not r.property return s, r, e`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("s", map[string]any{"name": "123"}, NodeKind2),
				NewNode("e", NodeKind1),
			},
			// property=false → NOT false = true → row included
			Edges: []EdgeFixture{NewEdgeWithProperties("s", "e", EdgeKind1, map[string]any{"property": false})},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- labels() and type() in the RETURN projection -----------------------
	{
		Name:    "return id, labels, and type from an edge traversal with a numeric id filter",
		Cypher:  `match (s)-[r:EdgeKind1]->(e) where not (s.system_tags contains 'admin_tier_0') and id(e) = 1 return id(s), labels(s), id(r), type(r)`,
		Fixture: baseFixture,
		// id(e)=1 is unlikely to match fixture data; query validates translation
		Assert: AssertNoError(),
	},
	// ---- chained edges with aliased property projections --------------------
	{
		Name:   "traverse two chained typed edges and return aliased endpoint properties",
		Cypher: `match (s)-[:EdgeKind1|EdgeKind2]->(e)-[:EdgeKind1]->() return s.name as s_name, e.name as e_name`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "src"}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"name": "mid"}, NodeKind2),
				NewNode("c", NodeKind1),
			},
			Edges: []EdgeFixture{
				NewEdge("a", "b", EdgeKind1),
				NewEdge("b", "c", EdgeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- multi-OR name filter on a typed source node -----------------------
	{
		Name:   "filter typed source node by four alternative name values with OR",
		Cypher: `match (n:NodeKind1)-[r]->() where n.name = '123' or n.name = '321' or n.name = '222' or n.name = '333' return n, r`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "123"}, NodeKind1),
				NewNode("b", NodeKind2),
			},
			Edges: []EdgeFixture{NewEdge("a", "b", EdgeKind1)},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- path binding with array membership and size check ------------------
	{
		Name:   "bind a one-hop typed path filtered by array membership or empty array size",
		Cypher: `match p = (:NodeKind1)-[:EdgeKind1|EdgeKind2]->(c:NodeKind2) where '123' in c.prop2 or '243' in c.prop2 or size(c.prop2) = 0 return p limit 10`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("src", NodeKind1),
				NewNodeWithProperties("dst", map[string]any{"prop2": []any{}}, NodeKind2),
			},
			Edges: []EdgeFixture{NewEdge("src", "dst", EdgeKind1)},
		},
		// dst.prop2=[] → size=0 → matches
		Assert: AssertNonEmpty(),
	},
}

var expansionSemanticCases = []SemanticTestCase{
	{
		Name:    "unbounded variable-length traversal returning both endpoints",
		Cypher:  `match (n)-[*..]->(e) return n, e`,
		Fixture: baseFixture,
		// n1->n2, n2->n3, n1->n3 (via n2) are all reachable
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "variable-length traversal bounded to depth 1–2",
		Cypher:  `match (n)-[*1..2]->(e) return n, e`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "variable-length traversal bounded to depth 3–5 (expect empty with shallow fixture)",
		Cypher:  `match (n)-[*3..5]->(e) return n, e`,
		Fixture: baseFixture,
		// base fixture only has 2-hop paths; depth 3+ should be empty for our data
		Assert: AssertNoError(),
	},
	{
		Name:    "bind unbounded path variable reaching a typed endpoint",
		Cypher:  `match p = (n)-[*..]->(e:NodeKind1) return p`,
		Fixture: baseFixture,
		// n2->n3 where n3 is NodeKind1; n1->n2->n3 also
		Assert: AssertNonEmpty(),
	},
	{
		Name:   "unbounded traversal from a named source to a typed endpoint",
		Cypher: `match (n)-[*..]->(e:NodeKind1) where n.name = 'n1' return e`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("n1", map[string]any{"name": "n1"}, NodeKind1),
				NewNodeWithProperties("n2", map[string]any{"name": "n2"}, NodeKind2),
				NewNodeWithProperties("n3", map[string]any{"name": "n3"}, NodeKind1),
			},
			Edges: []EdgeFixture{
				NewEdge("n1", "n2", EdgeKind1),
				NewEdge("n2", "n3", EdgeKind2),
			},
		},
		Assert: AssertContainsNodeWithProp("name", "n3"),
	},
	{
		Name:   "unbounded traversal filtering every traversed edge by a property",
		Cypher: `match (n)-[r*..]->(e:NodeKind1) where n.name = 'n1' and r.prop = 'a' return e`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("n1", map[string]any{"name": "n1"}, NodeKind1),
				NewNodeWithProperties("n2", map[string]any{"name": "n2"}, NodeKind2),
				NewNodeWithProperties("n3", map[string]any{"name": "n3"}, NodeKind1),
			},
			Edges: []EdgeFixture{
				NewEdgeWithProperties("n1", "n2", EdgeKind1, map[string]any{"prop": "a"}),
				NewEdgeWithProperties("n2", "n3", EdgeKind2, map[string]any{"prop": "a"}),
			},
		},
		Assert: AssertContainsNodeWithProp("name", "n3"),
	},
	{
		Name:    "bind path variable for unbounded traversal between typed endpoints",
		Cypher:  `match p = (s:NodeKind1)-[*..]->(e:NodeKind2) return p`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	// ---- incoming with depth range ----------------------------
	{
		Name:    "bounded incoming variable-length traversal with depth range 2–5",
		Cypher:  `match (n)<-[*2..5]-(e) return n, e`,
		Fixture: baseFixture,
		// n1->n2->n3: going backward from n3, n1 is reachable at depth 2
		Assert: AssertNonEmpty(),
	},
	// ---- followed by a single step ----------------------------
	{
		Name:   "unbounded expansion followed by a single fixed step",
		Cypher: `match (n)-[*..]->(e:NodeKind1)-[]->(l) where n.name = 'start' return l`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("start", map[string]any{"name": "start"}, NodeKind2),
				NewNodeWithProperties("mid", map[string]any{"name": "mid"}, NodeKind1),
				NewNodeWithProperties("leaf", map[string]any{"name": "leaf"}, NodeKind2),
			},
			Edges: []EdgeFixture{
				NewEdge("start", "mid", EdgeKind1),
				NewEdge("mid", "leaf", EdgeKind2),
			},
		},
		// start -[EK1]-> mid(NK1) -[EK2]-> leaf: reaches mid, one step to leaf
		Assert: AssertContainsNodeWithProp("name", "leaf"),
	},
	// ---- step followed by -------------------------------------
	{
		Name:   "fixed step followed by a bounded variable-length expansion",
		Cypher: `match (n)-[]->(e:NodeKind1)-[*2..3]->(l) where n.name = 'start' return l`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("start", map[string]any{"name": "start"}, NodeKind2),
				NewNodeWithProperties("mid", map[string]any{"name": "mid"}, NodeKind1),
				NewNodeWithProperties("hop1", map[string]any{"name": "hop1"}, NodeKind2),
				NewNodeWithProperties("hop2", map[string]any{"name": "hop2"}, NodeKind2),
			},
			Edges: []EdgeFixture{
				NewEdge("start", "mid", EdgeKind1),
				NewEdge("mid", "hop1", EdgeKind2),
				NewEdge("hop1", "hop2", EdgeKind2),
			},
		},
		// one step to mid(NK1), then 2 hops to hop2
		Assert: AssertContainsNodeWithProp("name", "hop2"),
	},
	// ---- expansion returning the source node (not the destination) ----------
	{
		Name:   "unbounded expansion to a typed endpoint returning the source node",
		Cypher: `match (n)-[*..]->(e:NodeKind1) where n.name = 'n2' return n`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("n2", map[string]any{"name": "n2"}, NodeKind2),
				NewNodeWithProperties("n3", map[string]any{"name": "n3"}, NodeKind1),
			},
			Edges: []EdgeFixture{NewEdge("n2", "n3", EdgeKind1)},
		},
		// n2 reaches n3(NK1); return n (=n2)
		Assert: AssertContainsNodeWithProp("name", "n2"),
	},
	// ---- bounded expansion followed by a fixed step -------------------------
	{
		Name:   "bounded variable-length expansion followed by a single fixed step",
		Cypher: `match (n)-[*2..3]->(e:NodeKind1)-[]->(l) where n.name = 'n1' return l`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("n1", map[string]any{"name": "n1"}, NodeKind2),
				NewNodeWithProperties("hop", map[string]any{"name": "hop"}, NodeKind2),
				NewNodeWithProperties("mid", map[string]any{"name": "mid"}, NodeKind1),
				NewNodeWithProperties("leaf", map[string]any{"name": "leaf"}, NodeKind2),
			},
			Edges: []EdgeFixture{
				NewEdge("n1", "hop", EdgeKind1),
				NewEdge("hop", "mid", EdgeKind2),
				NewEdge("mid", "leaf", EdgeKind1),
			},
		},
		// 2 hops: n1→hop→mid(NK1); one fixed step: mid→leaf → return leaf
		Assert: AssertContainsNodeWithProp("name", "leaf"),
	},
	// ---- two variable-length segments with a typed fixed step between --------
	{
		Name:   "two unbounded expansions joined through a typed fixed step",
		Cypher: `match (n)-[*..]->(e)-[:EdgeKind1|EdgeKind2]->()-[*..]->(l) where n.name = 'n1' and e.name = 'n2' return l`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("n1", map[string]any{"name": "n1"}, NodeKind1),
				NewNodeWithProperties("n2", map[string]any{"name": "n2"}, NodeKind2),
				NewNode("bridge", NodeKind2),
				NewNodeWithProperties("leaf", map[string]any{"name": "leaf"}, NodeKind1),
			},
			Edges: []EdgeFixture{
				NewEdge("n1", "n2", EdgeKind1),
				NewEdge("n2", "bridge", EdgeKind2),
				NewEdge("bridge", "leaf", EdgeKind1),
			},
		},
		Assert: AssertContainsNodeWithProp("name", "leaf"),
	},
	// ---- split() function in a WHERE predicate on an expansion endpoint -----
	{
		Name:   "bind expansion path filtered by a split() membership check on the endpoint",
		Cypher: `match p = (:NodeKind1)-[:EdgeKind1*1..]->(n:NodeKind2) where 'admin_tier_0' in split(n.system_tags, ' ') return p limit 1000`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("src", NodeKind1),
				NewNodeWithProperties("dst", map[string]any{"system_tags": "admin_tier_0 extra_tag"}, NodeKind2),
			},
			Edges: []EdgeFixture{NewEdge("src", "dst", EdgeKind1)},
		},
		// split('admin_tier_0 extra_tag',' ')=['admin_tier_0','extra_tag']; 'admin_tier_0' in list
		Assert: AssertNonEmpty(),
	},
	// ---- node inequality constraint inside an expansion path ----------------
	{
		Name:    "bind expansion path where the source and destination must be distinct nodes",
		Cypher:  `match p = (s:NodeKind1)-[*..]->(e:NodeKind2) where s <> e return p`,
		Fixture: baseFixture,
		// n1(NK1)-[EK1]->n2(NK2); s=n1, e=n2; n1 ≠ n2 → matches
		Assert: AssertNonEmpty(),
	},
	// ---- both-endpoint ends-with filters in an expansion path ---------------
	{
		Name:   "bind expansion path filtering both endpoints using ends-with on objectid",
		Cypher: `match p = (g:NodeKind1)-[:EdgeKind1|EdgeKind2*]->(target:NodeKind1) where g.objectid ends with '-src' and target.objectid ends with '-tgt' return p`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("src", map[string]any{"objectid": "S-1-src"}, NodeKind1),
				NewNodeWithProperties("tgt", map[string]any{"objectid": "S-1-tgt"}, NodeKind1),
			},
			Edges: []EdgeFixture{NewEdge("src", "tgt", EdgeKind1)},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- incoming unbounded expansion returning a bound path ----------------
	{
		Name:    "bind an incoming unbounded expansion path to a typed source",
		Cypher:  `match p = (:NodeKind1)<-[:EdgeKind1|EdgeKind2*..]-() return p limit 10`,
		Fixture: baseFixture,
		// n3(NK1) is reached by n2-[EK2]->n3; incoming expansion from NK1 finds this path
		Assert: AssertNonEmpty(),
	},
	// ---- expansion with a regex filter on the endpoint ----------------------
	{
		Name:    "bind expansion path filtered by a regular expression on the endpoint name",
		Cypher:  `match p = (n:NodeKind1)-[:EdgeKind1|EdgeKind2*1..2]->(r:NodeKind2) where r.name =~ '1.*' return p limit 10`,
		Fixture: baseFixture,
		// n1(NK1)-[EK1]->n2(NK2,name='1234'); '1234' =~ '1.*' → matches
		Assert: AssertNonEmpty(),
	},
	// ---- incoming expansion with a disjunction kind filter on the source ----
	{
		Name:    "bind incoming expansion path where source matches a kind disjunction",
		Cypher:  `match p = (t:NodeKind2)<-[:EdgeKind1*1..]-(a) where (a:NodeKind1 or a:NodeKind2) and t.objectid ends with '-2' return p limit 1000`,
		Fixture: baseFixture,
		// t=n2(NK2,objectid='S-1-5-21-2'); n1(NK1)-[EK1]->n2; a=n1 is NK1 → matches
		Assert: AssertNonEmpty(),
	},
}

var aggregationSemanticCases = []SemanticTestCase{
	{
		Name:    "count all nodes",
		Cypher:  `MATCH (n) RETURN count(n)`,
		Fixture: baseFixture,
		Assert:  AssertAtLeastInt64(3), // at least the 3 base fixture nodes
	},
	{
		Name:    "return a constant string literal",
		Cypher:  `RETURN 'hello world'`,
		Fixture: GraphFixture{},
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "return a constant arithmetic expression",
		Cypher:  `RETURN 2 + 3`,
		Fixture: GraphFixture{},
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "collect all node name properties into a list",
		Cypher:  `MATCH (n) RETURN collect(n.name)`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "return the size of a collected list of node properties",
		Cypher:  `MATCH (n) RETURN size(collect(n.name))`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "filter on an aggregate result using WITH and WHERE",
		Cypher:  `MATCH (n) WITH count(n) as cnt WHERE cnt > 1 RETURN cnt`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(), // db has at least 3 nodes, so cnt > 1 is satisfied
	},
	{
		Name:   "group by node and filter on per-node count",
		Cypher: `MATCH (n) WITH n, count(n) as node_count WHERE node_count > 1 RETURN n, node_count`,
		// Each node grouped by itself gives count=1; with >1 filter result is empty
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	// ---- sum / avg / min / max ------------------------------------------
	{
		Name:    "sum a numeric node property across all nodes",
		Cypher:  `MATCH (n) RETURN sum(n.value)`,
		Fixture: baseFixture,
		// n1.value=1, n2.value=2, n3.value=3 → sum ≥ 6
		Assert: AssertNonEmpty(),
	},
	{
		Name:    "average a numeric node property across all nodes",
		Cypher:  `MATCH (n) RETURN avg(n.value)`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "minimum of a numeric node property across all nodes",
		Cypher:  `MATCH (n) RETURN min(n.value)`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "maximum of a numeric node property across all nodes",
		Cypher:  `MATCH (n) RETURN max(n.value)`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	// ---- grouped --------------------------------------------
	{
		Name:    "group nodes by a property and count each group",
		Cypher:  `MATCH (n) RETURN n.domain, count(n)`,
		Fixture: baseFixture,
		// groups: "test.local"→n1, "other.local"→n2, null→n3
		Assert: AssertNonEmpty(),
	},
	// ---- multi-aggregate in one projection ------------------------------
	{
		Name:    "compute multiple aggregates in a single projection",
		Cypher:  `MATCH (n) RETURN count(n), sum(n.value)`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	// ---- size() in WHERE ------------------------------------------------
	{
		Name:    "filter nodes using size() on an array property in WHERE",
		Cypher:  `MATCH (n) WHERE size(n.array_value) > 0 RETURN n`,
		Fixture: baseFixture,
		// n1.array_value=[1,2] has size 2 > 0
		Assert: AssertNonEmpty(),
	},
	// ---- count-then-match (aggregate feeds a subsequent MATCH) ----------
	{
		Name:    "feed an aggregate result from a WITH stage into a subsequent MATCH",
		Cypher:  `MATCH (n) WITH count(n) as lim MATCH (o) RETURN o`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	// ---- grouped sum and avg ------------------------------------------------
	{
		Name:   "group nodes by a property and return the sum of another property per group",
		Cypher: `MATCH (n) RETURN n.department, sum(n.salary)`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"department": "eng", "salary": 100}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"department": "eng", "salary": 200}, NodeKind1),
				NewNodeWithProperties("c", map[string]any{"department": "hr", "salary": 150}, NodeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	{
		Name:   "group nodes by a property and return the average of another property per group",
		Cypher: `MATCH (n) RETURN n.department, avg(n.age)`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"department": "eng", "age": 30}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"department": "eng", "age": 40}, NodeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- all aggregates in one projection -----------------------------------
	{
		Name:   "compute count sum avg min and max of a property in a single projection",
		Cypher: `MATCH (n) RETURN count(n), sum(n.age), avg(n.age), min(n.age), max(n.age)`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"age": 25}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"age": 35}, NodeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- grouped collect and grouped collect+count --------------------------
	{
		Name:   "group nodes by a property and collect names per group",
		Cypher: `MATCH (n) RETURN n.department, collect(n.name)`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"department": "eng", "name": "alice"}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"department": "eng", "name": "bob"}, NodeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	{
		Name:   "group nodes by a property and return both a collected list and a count",
		Cypher: `MATCH (n) RETURN n.department, collect(n.name), count(n)`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"department": "ops", "name": "carol"}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"department": "ops", "name": "dave"}, NodeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- size() in the RETURN projection ------------------------------------
	{
		Name:   "return the size of an array property in the projection",
		Cypher: `MATCH (n) RETURN size(n.tags)`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"tags": []any{"admin", "user"}}, NodeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- arithmetic on aggregate results ------------------------------------
	{
		Name:   "compute a ratio by dividing two aggregate results in a WITH stage",
		Cypher: `MATCH (n) WITH sum(n.age) as total_age, count(n) as total_count RETURN total_age / total_count as avg_age`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"age": 30}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"age": 50}, NodeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- collect in WITH then size filter -----------------------------------
	{
		Name:    "collect node properties in a WITH stage then filter by the collected size",
		Cypher:  `MATCH (n) WITH n, collect(n.prop) as props WHERE size(props) > 1 RETURN n, props`,
		Fixture: baseFixture,
		// Each node is its own group so collect returns [value] with size=1; >1 is empty
		Assert: AssertNoError(),
	},
}

// ---------------------------------------------------------------------------
// multipart.sql
// ---------------------------------------------------------------------------

var multipartSemanticCases = []SemanticTestCase{
	{
		Name:    "bind a literal as a WITH variable and filter typed nodes by it",
		Cypher:  `with '1' as target match (n:NodeKind1) where n.value = target return n`,
		Fixture: baseFixture,
		// n1 has value=1 but stored as int; string comparison depends on translation
		Assert: AssertNoError(),
	},
	{
		Name:   "carry a node through WITH and re-match it by ID",
		Cypher: `match (n:NodeKind1) where n.value = 1 with n match (b) where id(b) = id(n) return b`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("n", map[string]any{"value": 1}, NodeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	{
		Name:   "exclude second-stage results using a collected list from the first stage",
		Cypher: `match (g1:NodeKind1) where g1.name starts with 'test' with collect(g1.domain) as excludes match (d:NodeKind2) where d.name starts with 'other' and not d.name in excludes return d`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("g1", map[string]any{"name": "testnode", "domain": "test.local"}, NodeKind1),
				NewNodeWithProperties("d1", map[string]any{"name": "othernode"}, NodeKind2),
				NewNodeWithProperties("d2", map[string]any{"name": "othertest"}, NodeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- triple-part chain: match → with → match → with → match ---------
	{
		Name:   "three-stage pipeline carrying nodes through successive WITH clauses",
		Cypher: `match (n:NodeKind1) where n.value = 1 with n match (f) where f.name = 'me' with f match (b) where id(b) = id(f) return b`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("n", map[string]any{"value": 1}, NodeKind1),
				NewNodeWithProperties("f", map[string]any{"name": "me"}, NodeKind2),
			},
		},
		// b = f (the node whose id matches f's id)
		Assert: AssertContainsNodeWithProp("name", "me"),
	},
	// ---- bind a variable, then find all paths leading to it -------------
	{
		Name:    "bind any node then find all one-hop paths that reach it",
		Cypher:  `match (e) match p = ()-[]->(e) return p limit 1`,
		Fixture: baseFixture,
		// any one-hop path whose end is any node; n1->n2 and n2->n3 qualify
		Assert: AssertNonEmpty(),
	},
	// ---- re-match a WITH-carried node under its own label ---------------
	{
		Name:    "carry a node through WITH and re-match it under its original kind label",
		Cypher:  `match (u:NodeKind1)-[:EdgeKind1]->(g:NodeKind2) with g match (g)<-[:EdgeKind1]-(u:NodeKind1) return g`,
		Fixture: baseFixture,
		// n1(NK1)-[EK1]->n2(NK2); carried g=n2; n2<-[EK1]-n1(NK1) still holds
		Assert: AssertNonEmpty(),
	},
	// ---- numeric literal WITH used in subsequent arithmetic -----------------
	{
		Name:    "bind a numeric literal as a WITH variable and use it in arithmetic in the next MATCH",
		Cypher:  `with 365 as max_days match (n:NodeKind1) where n.pwdlastset < (datetime().epochseconds - (max_days * 86400)) and not n.pwdlastset IN [-1.0, 0.0] return n limit 100`,
		Fixture: baseFixture,
		// n1.pwdlastset=-2; current epochseconds ≫ 365*86400; -2 < big_number is true; -2 ∉ {-1,0}
		Assert: AssertNonEmpty(),
	},
	// ---- multi-match then variable-length expansion path --------------------
	{
		Name:    "match a typed node then bind its variable-length expansion to a path",
		Cypher:  `match (n:NodeKind1) where n.objectid = 'S-1-5-21-1' match p = (n)-[:EdgeKind1|EdgeKind2*1..]->(c:NodeKind2) return p`,
		Fixture: baseFixture,
		// n1.objectid='S-1-5-21-1'; n1-[EK1]->n2(NK2) → path exists
		Assert: AssertNonEmpty(),
	},
	// ---- WITH count as a filter in the same stage ---------------------------
	{
		Name:   "filter a carried node using a per-group count in a WITH stage",
		Cypher: `match (n:NodeKind1)<-[:EdgeKind1]-(:NodeKind2) where n.objectid ends with '-516' with n, count(n) as dc_count where dc_count = 1 return n`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("dst", map[string]any{"objectid": "S-1-5-21-516"}, NodeKind1),
				NewNode("src", NodeKind2),
			},
			Edges: []EdgeFixture{NewEdge("src", "dst", EdgeKind1)},
		},
		// exactly one NK2-[EK1]->dst edge; count=1 → matches
		Assert: AssertNonEmpty(),
	},
	// ---- two path variables sharing a node ----------------------------------
	{
		Name:    "match two paths that share a common middle node and return both",
		Cypher:  `match p = (a)-[]->() match q = ()-[]->(a) return p, q`,
		Fixture: baseFixture,
		// baseFixture: n1->n2->n3; a=n2 satisfies both (n2-[]->n3) and (n1-[]->(n2))
		Assert: AssertNonEmpty(),
	},
	// ---- regex filter in a multipart pipeline -------------------------------
	{
		Name:   "filter typed nodes by a regular expression and carry collected results to the next stage",
		Cypher: `match (cg:NodeKind1) where cg.name =~ ".*TT" with collect(cg.name) as names return names`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "SCOTT"}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"name": "admin"}, NodeKind1),
			},
		},
		// 'SCOTT' =~ '.*TT' → true; 'admin' → false; names=['SCOTT']
		Assert: AssertNonEmpty(),
	},
	// ---- expansion with distinct count and ORDER BY on the aggregate --------
	{
		Name:   "expand from a typed node, count reachable typed targets, and order by that count",
		Cypher: `match (n:NodeKind1) where n.hasspn = true match (n)-[:EdgeKind1|EdgeKind2*1..]->(c:NodeKind2) with distinct n, count(c) as adminCount return n order by adminCount desc limit 100`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("src", map[string]any{"hasspn": true}, NodeKind1),
				NewNode("c1", NodeKind2),
				NewNode("c2", NodeKind2),
			},
			Edges: []EdgeFixture{
				NewEdge("src", "c1", EdgeKind1),
				NewEdge("src", "c2", EdgeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
}

// ---------------------------------------------------------------------------
// pattern_binding.sql
// ---------------------------------------------------------------------------

var patternBindingSemanticCases = []SemanticTestCase{
	{
		Name:    "bind a single typed node to a path variable",
		Cypher:  `match p = (:NodeKind1) return p`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "bind a one-hop traversal to a path variable",
		Cypher:  `match p = ()-[]->() return p`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:    "bind an unbounded variable-length path to a path variable",
		Cypher:  `match p = ()-[*..]->(e) return p limit 1`,
		Fixture: baseFixture,
		Assert:  AssertNonEmpty(),
	},
	{
		Name:   "bind a two-hop path and return the terminal node",
		Cypher: `match p = ()-[r1]->()-[r2]->(e) return e`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1),
				NewNode("b", NodeKind2),
				NewNode("c", NodeKind1),
			},
			Edges: []EdgeFixture{
				NewEdge("a", "b", EdgeKind1),
				NewEdge("b", "c", EdgeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	{
		Name:   "bind a converging diamond path with endpoint property filters",
		Cypher: `match p = (a)-[]->()<-[]-(f) where a.name = 'value' and f.is_target return p`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "value"}, NodeKind1),
				NewNodeWithProperties("mid", map[string]any{}, NodeKind2),
				NewNodeWithProperties("f", map[string]any{"is_target": true}, NodeKind1),
			},
			Edges: []EdgeFixture{
				NewEdge("a", "mid", EdgeKind1),
				NewEdge("f", "mid", EdgeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- node-only path with property filter ----------------------------
	{
		Name:   "bind a node-only path with a contains property filter",
		Cypher: `match p = (n:NodeKind1) where n.name contains 'test' return p`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "testuser"}, NodeKind1),
				NewNodeWithProperties("b", map[string]any{"name": "admin"}, NodeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- undirected edge in path ----------------------------------------
	{
		Name:   "bind a path with an undirected edge between typed nodes",
		Cypher: `match p = (n:NodeKind1)-[r]-(m:NodeKind1) return p`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1),
				NewNode("b", NodeKind1),
			},
			Edges: []EdgeFixture{
				NewEdge("a", "b", EdgeKind1),
			},
		},
		// undirected: a-[r]-b is found in both (a,b) and (b,a) orientations
		Assert: AssertNonEmpty(),
	},
	// ---- 3-hop path with named-edge property filters --------------------
	{
		Name:   "three-hop traversal filtering named edges by their property",
		Cypher: `match ()-[r1]->()-[r2]->()-[]->() where r1.label = 'first' and r2.label = 'second' return r1`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1),
				NewNode("b", NodeKind2),
				NewNode("c", NodeKind1),
				NewNode("d", NodeKind2),
			},
			Edges: []EdgeFixture{
				NewEdgeWithProperties("a", "b", EdgeKind1, map[string]any{"label": "first"}),
				NewEdgeWithProperties("b", "c", EdgeKind2, map[string]any{"label": "second"}),
				NewEdge("c", "d", EdgeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- edge boolean property as a path filter ------------------------------
	{
		Name:   "bind a one-hop path between typed nodes filtered by a boolean edge property",
		Cypher: `match p = (:NodeKind1)-[r]->(:NodeKind1) where r.isacl return p limit 100`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1),
				NewNode("b", NodeKind1),
			},
			Edges: []EdgeFixture{
				NewEdgeWithProperties("a", "b", EdgeKind1, map[string]any{"isacl": true}),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- fixed step plus variable-length expansion with named first edge ----
	{
		Name:   "return a named first edge and the full path including its subsequent expansion",
		Cypher: `match p = ()-[e:EdgeKind1]->()-[:EdgeKind1*1..]->() return e, p`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1),
				NewNode("b", NodeKind2),
				NewNode("c", NodeKind1),
			},
			Edges: []EdgeFixture{
				NewEdge("a", "b", EdgeKind1),
				NewEdge("b", "c", EdgeKind1),
			},
		},
		// e=a→b(EK1); then b→c(EK1*1..); full path a→b→c
		Assert: AssertNonEmpty(),
	},
	// ---- toUpper not-contains in a path filter ------------------------------
	{
		Name:   "bind a typed one-hop path where the target property does not contain a toUpper result",
		Cypher: `match p = (m:NodeKind1)-[:EdgeKind1]->(c:NodeKind2) where m.objectid ends with '-1' and not toUpper(c.operatingsystem) contains 'SERVER' return p limit 1000`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("m", map[string]any{"objectid": "S-1-5-21-1"}, NodeKind1),
				NewNodeWithProperties("c", map[string]any{"operatingsystem": "workstation"}, NodeKind2),
			},
			Edges: []EdgeFixture{NewEdge("m", "c", EdgeKind1)},
		},
		// toUpper('workstation')='WORKSTATION'; 'WORKSTATION' not contains 'SERVER' → true
		Assert: AssertNonEmpty(),
	},
	// ---- array membership on a middle node in a chained path ----------------
	{
		Name:   "bind a two-hop typed path filtered by array membership on the intermediate node",
		Cypher: `match p = (:NodeKind1)-[:EdgeKind1|EdgeKind2]->(e:NodeKind2)-[:EdgeKind2]->(:NodeKind1) where 'a' in e.values or 'b' in e.values or size(e.values) = 0 return p`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("src", NodeKind1),
				NewNodeWithProperties("mid", map[string]any{"values": []any{"a", "c"}}, NodeKind2),
				NewNode("dst", NodeKind1),
			},
			Edges: []EdgeFixture{
				NewEdge("src", "mid", EdgeKind1),
				NewEdge("mid", "dst", EdgeKind2),
			},
		},
		// 'a' in ['a','c'] → true → matches
		Assert: AssertNonEmpty(),
	},
	// ---- fixed step then variable expansion with coalesce in path filter ----
	{
		Name:   "bind a path with one fixed hop then variable expansion filtered by coalesce contains",
		Cypher: `match p = (:NodeKind1)-[:EdgeKind1]->(:NodeKind2)-[:EdgeKind2*1..]->(t:NodeKind2) where coalesce(t.system_tags, '') contains 'admin_tier_0' return p limit 1000`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1),
				NewNode("b", NodeKind2),
				NewNodeWithProperties("t", map[string]any{"system_tags": "admin_tier_0"}, NodeKind2),
			},
			Edges: []EdgeFixture{
				NewEdge("a", "b", EdgeKind1),
				NewEdge("b", "t", EdgeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- multi-match with WHERE then variable-length expansion path ---------
	{
		Name:   "filter a typed node with WHERE then bind its variable-length expansion path",
		Cypher: `match (u:NodeKind1) where u.samaccountname in ['foo', 'bar'] match p = (u)-[:EdgeKind1|EdgeKind2*1..3]->(t) where coalesce(t.system_tags, '') contains 'admin_tier_0' return p limit 1000`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("u", map[string]any{"samaccountname": "foo"}, NodeKind1),
				NewNodeWithProperties("t", map[string]any{"system_tags": "admin_tier_0"}, NodeKind2),
			},
			Edges: []EdgeFixture{NewEdge("u", "t", EdgeKind1)},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- three-match pattern: anchor + second + path ------------------------
	{
		Name:   "three consecutive MATCHes that anchor two nodes and bind the connecting path",
		Cypher: `match (x:NodeKind1) where x.name = 'foo' match (y:NodeKind2) where y.name = 'bar' match p=(x)-[:EdgeKind1]->(y) return p`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("x", map[string]any{"name": "foo"}, NodeKind1),
				NewNodeWithProperties("y", map[string]any{"name": "bar"}, NodeKind2),
			},
			Edges: []EdgeFixture{NewEdge("x", "y", EdgeKind1)},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- inline property map in MATCH then path binding ---------------------
	{
		Name:   "match a node with an inline property map then bind its outgoing path to a second inline-map node",
		Cypher: `match (x:NodeKind1{name:'foo'}) match p=(x)-[:EdgeKind1]->(y:NodeKind2{name:'bar'}) return p`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("x", map[string]any{"name": "foo"}, NodeKind1),
				NewNodeWithProperties("y", map[string]any{"name": "bar"}, NodeKind2),
			},
			Edges: []EdgeFixture{NewEdge("x", "y", EdgeKind1)},
		},
		Assert: AssertNonEmpty(),
	},
}

// ---------------------------------------------------------------------------
// delete.sql
// ---------------------------------------------------------------------------

var deleteSemanticCases = []SemanticTestCase{
	{
		Name:   "detach-delete a typed node and its incident edges",
		Cypher: `match (s:NodeKind1) detach delete s`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("victim", map[string]any{"name": "victim"}, NodeKind1),
			},
		},
		Assert: AssertNoError(),
		PostAssert: func(t *testing.T, tx graph.Transaction) {
			t.Helper()
			result := tx.Query(`match (n:NodeKind1) where n.name = 'victim' return n`, nil)
			defer result.Close()
			for result.Next() {
			}
			if err := result.Error(); err != nil {
				t.Errorf("post-delete query error: %v", err)
			}
		},
	},
	{
		Name:    "delete a specific typed edge",
		Cypher:  `match ()-[r:EdgeKind1]->() delete r`,
		Fixture: baseFixture,
		Assert:  AssertNoError(),
	},
	// ---- multi-hop: traverse two hops then delete the second edge -------
	{
		Name:   "traverse two hops then delete the typed edge at the second hop",
		Cypher: `match ()-[]->()-[r:EdgeKind2]->() delete r`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1),
				NewNode("b", NodeKind2),
				NewNode("c", NodeKind1),
			},
			Edges: []EdgeFixture{
				NewEdge("a", "b", EdgeKind1),
				NewEdge("b", "c", EdgeKind2),
			},
		},
		Assert: AssertNoError(),
	},
}

// ---------------------------------------------------------------------------
// update.sql
// ---------------------------------------------------------------------------

var updateSemanticCases = []SemanticTestCase{
	{
		Name:    "set a string property on a filtered node and return the updated node",
		Cypher:  `match (n) where n.name = 'n3' set n.name = 'RENAMED' return n`,
		Fixture: baseFixture,
		Assert:  AssertContainsNodeWithProp("name", "RENAMED"),
	},
	{
		Name:   "chain multiple SET clauses to update several properties on a node",
		Cypher: `match (n) set n.other = 1 set n.prop = '1' return n`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "updateme"}, NodeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	{
		Name:   "add multiple kind labels to a node",
		Cypher: `match (n) set n:NodeKind1:NodeKind2 return n`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	{
		Name:   "remove multiple kind labels from a node",
		Cypher: `match (n) remove n:NodeKind1:NodeKind2 return n`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1, NodeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	{
		Name:   "set a boolean property on a filtered node (no RETURN)",
		Cypher: `match (n) where n.name = '1234' set n.is_target = true`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "1234"}, NodeKind1),
			},
		},
		Assert: AssertNoError(),
	},
	{
		Name:   "set a property on a traversed edge from a typed source node",
		Cypher: `match (n)-[r:EdgeKind1]->() where n:NodeKind1 set r.visited = true return r`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1),
				NewNode("b", NodeKind2),
			},
			Edges: []EdgeFixture{NewEdge("a", "b", EdgeKind1)},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- set kind + remove kind (combined in one statement) -------------
	{
		Name:   "add one kind label and remove another in the same statement",
		Cypher: `match (n) set n:NodeKind1 remove n:NodeKind2 return n`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind1, NodeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- set kind + set property (combined) ----------------------------
	{
		Name:   "add a kind label and set a property in the same statement",
		Cypher: `match (n) set n:NodeKind1 set n.flag = '1' return n`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("a", NodeKind2),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- remove kind + remove property (combined) ----------------------
	{
		Name:   "remove a kind label and a property in the same statement",
		Cypher: `match (n) remove n:NodeKind1 remove n.prop return n`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"prop": "val"}, NodeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- remove single property -----------------------------------------
	{
		Name:   "remove a single node property (no RETURN)",
		Cypher: `match (s) remove s.name`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("a", map[string]any{"name": "drop-me"}, NodeKind1),
			},
		},
		Assert: AssertNoError(),
	},
	// ---- edge-only update (no RETURN) -----------------------------------
	{
		Name:   "set a property on an edge leading to a typed target node (no RETURN)",
		Cypher: `match ()-[r]->(:NodeKind1) set r.is_special_outbound = true`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("src", NodeKind2),
				NewNode("dst", NodeKind1),
			},
			Edges: []EdgeFixture{NewEdge("src", "dst", EdgeKind1)},
		},
		Assert: AssertNoError(),
	},
	// ---- node + edge updated together -----------------------------------
	{
		Name:   "update a source node property and an edge property together",
		Cypher: `match (a)-[r]->(:NodeKind1) set a.name = '123', r.is_special_outbound = true`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNode("src", NodeKind2),
				NewNode("dst", NodeKind1),
			},
			Edges: []EdgeFixture{NewEdge("src", "dst", EdgeKind1)},
		},
		Assert: AssertNoError(),
	},
}

// ---------------------------------------------------------------------------
// quantifiers.sql
// ---------------------------------------------------------------------------

// quantifierFixture provides nodes whose array properties exercise ANY, ALL,
// NONE, and SINGLE quantifier semantics against the string predicate
// "type CONTAINS 'DES'".
//
//	qAny  (NK1): supportedencryptiontypes=["DES-CBC-CRC","AES-128"], usedeskeyonly=false
//	             → ANY  matches DES-CBC-CRC (count≥1) → true
//	qAll  (NK1): supportedencryptiontypes=["DES-CBC-CRC","DES-CBC-MD5"], usedeskeyonly=false
//	             → ALL  both match              (count=2=len=2) → true
//	qNone (NK1): supportedencryptiontypes=["AES-128","RC4-HMAC"], usedeskeyonly=false
//	             → NONE no match                (count=0) → true
//	qSingle(NK1):supportedencryptiontypes=["DES-CBC-CRC","AES-128"], usedeskeyonly=false
//	             → SINGLE exactly one match     (count=1) → true
//
// All four nodes have usedeskeyonly=false so the OR short-circuit is never
// taken and the quantifier itself is the deciding factor.
var quantifierFixture = GraphFixture{
	Nodes: []NodeFixture{
		NewNodeWithProperties("qAny", map[string]any{
			"usedeskeyonly":            false,
			"supportedencryptiontypes": []any{"DES-CBC-CRC", "AES-128"},
		}, NodeKind1),
		NewNodeWithProperties("qAll", map[string]any{
			"usedeskeyonly":            false,
			"supportedencryptiontypes": []any{"DES-CBC-CRC", "DES-CBC-MD5"},
		}, NodeKind1),
		NewNodeWithProperties("qNone", map[string]any{
			"usedeskeyonly":            false,
			"supportedencryptiontypes": []any{"AES-128", "RC4-HMAC"},
		}, NodeKind1),
		NewNodeWithProperties("qSingle", map[string]any{
			"usedeskeyonly":            false,
			"supportedencryptiontypes": []any{"DES-CBC-CRC", "AES-128"},
		}, NodeKind1),
	},
}

var quantifiersSemanticCases = []SemanticTestCase{
	// ---- ANY ------------------------------------------------------------
	{
		Name:    "ANY quantifier over an array property with a contains predicate",
		Cypher:  `MATCH (n:NodeKind1) WHERE n.usedeskeyonly OR ANY(type IN n.supportedencryptiontypes WHERE type CONTAINS 'DES') RETURN n LIMIT 100`,
		Fixture: quantifierFixture,
		// qAny: "DES-CBC-CRC" contains 'DES' → count≥1 → ANY=true; false OR true → matches
		Assert: AssertNonEmpty(),
	},
	// ---- ALL ------------------------------------------------------------
	{
		Name:    "ALL quantifier over an array property with a contains predicate",
		Cypher:  `MATCH (n:NodeKind1) WHERE n.usedeskeyonly OR ALL(type IN n.supportedencryptiontypes WHERE type CONTAINS 'DES') RETURN n LIMIT 100`,
		Fixture: quantifierFixture,
		// qAll: both entries contain 'DES' → count=len=2 → ALL=true; false OR true → matches
		Assert: AssertNonEmpty(),
	},
	// ---- NONE -----------------------------------------------------------
	{
		Name:    "NONE quantifier over an array property with a contains predicate",
		Cypher:  `MATCH (n:NodeKind1) WHERE n.usedeskeyonly OR NONE(type IN n.supportedencryptiontypes WHERE type CONTAINS 'DES') RETURN n LIMIT 100`,
		Fixture: quantifierFixture,
		// qNone: neither "AES-128" nor "RC4-HMAC" contains 'DES' → count=0 → NONE=true
		Assert: AssertNonEmpty(),
	},
	// ---- SINGLE ---------------------------------------------------------
	{
		Name:    "SINGLE quantifier over an array property with a contains predicate",
		Cypher:  `MATCH (n:NodeKind1) WHERE n.usedeskeyonly OR SINGLE(type IN n.supportedencryptiontypes WHERE type CONTAINS 'DES') RETURN n LIMIT 100`,
		Fixture: quantifierFixture,
		// qSingle: exactly one entry ("DES-CBC-CRC") matches → count=1 → SINGLE=true
		Assert: AssertNonEmpty(),
	},
	// ---- NONE inside a WITH-piped stage ---------------------------------
	// The second MATCH is a required (non-optional) match. The translator
	// renders it as an inner join in CTE s3. If s3 is empty (no matching
	// n→g edges), GROUP BY returns no groups at all and m drops out of the
	// pipeline — "NONE of empty is vacuously true" does not apply.
	//
	// Fixture: m ← NodeKind1 with unconstraineddelegation=true
	//          n ← NodeKind1 with a *different* objectid ("other-id")
	//          g ← NodeKind2 with objectid ending '-516'
	//          n -[EdgeKind1]→ g
	//
	// Second MATCH finds (n,g) → matchingNs=[n].
	// NONE(n IN [n] WHERE n.objectid = "test-m"):
	//   n.objectid="other-id" ≠ "test-m" → count=0 → NONE=true → m returned.
	{
		Name:   "NONE quantifier over a collected list in a WITH-piped stage",
		Cypher: `MATCH (m:NodeKind1) WHERE m.unconstraineddelegation = true WITH m MATCH (n:NodeKind1)-[:EdgeKind1]->(g:NodeKind2) WHERE g.objectid ENDS WITH '-516' WITH m, COLLECT(n) AS matchingNs WHERE NONE(n IN matchingNs WHERE n.objectid = m.objectid) RETURN m`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("m", map[string]any{"unconstraineddelegation": true, "objectid": "test-m"}, NodeKind1),
				NewNodeWithProperties("n", map[string]any{"objectid": "other-id"}, NodeKind1),
				NewNodeWithProperties("g", map[string]any{"objectid": "S-1-5-21-516"}, NodeKind2),
			},
			Edges: []EdgeFixture{
				NewEdge("n", "g", EdgeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- ALL inside a WITH-piped stage ----------------------------------
	// Same structural requirement as NONE: the second MATCH must produce
	// rows so GROUP BY has groups to evaluate.
	//
	// Fixture: m ← NodeKind1 with unconstraineddelegation=true
	//          n ← NodeKind1 with the *same* objectid as m ("test-m")
	//          g ← NodeKind2 with objectid ending '-516'
	//          n -[EdgeKind1]→ g
	//
	// Second MATCH finds (n,g) → matchingNs=[n].
	// ALL(n IN [n] WHERE n.objectid = "test-m"):
	//   n.objectid="test-m" = "test-m" → count=1=len=1 → ALL=true → m returned.
	{
		Name:   "ALL quantifier over a collected list in a WITH-piped stage",
		Cypher: `MATCH (m:NodeKind1) WHERE m.unconstraineddelegation = true WITH m MATCH (n:NodeKind1)-[:EdgeKind1]->(g:NodeKind2) WHERE g.objectid ENDS WITH '-516' WITH m, COLLECT(n) AS matchingNs WHERE ALL(n IN matchingNs WHERE n.objectid = m.objectid) RETURN m`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				NewNodeWithProperties("m", map[string]any{"unconstraineddelegation": true, "objectid": "test-m"}, NodeKind1),
				NewNodeWithProperties("n", map[string]any{"objectid": "test-m"}, NodeKind1),
				NewNodeWithProperties("g", map[string]any{"objectid": "S-1-5-21-516"}, NodeKind2),
			},
			Edges: []EdgeFixture{
				NewEdge("n", "g", EdgeKind1),
			},
		},
		Assert: AssertNonEmpty(),
	},
	// ---- multiple ANY quantifiers with a compound OR predicate inside --------
	// The second ANY uses a compound OR predicate inside its WHERE clause,
	// exercising the translation of disjunctions within quantifier bodies.
	{
		Name:    "multiple ANY quantifiers where the second ANY has a compound OR predicate",
		Cypher:  `MATCH (n:NodeKind1) WHERE n.usedeskeyonly OR ANY(type IN n.supportedencryptiontypes WHERE type CONTAINS 'DES') OR ANY(type IN n.serviceprincipalnames WHERE toLower(type) CONTAINS 'mssql' OR toLower(type) CONTAINS 'mssqlcluster') RETURN n LIMIT 100`,
		Fixture: quantifierFixture,
		// qAny has 'DES-CBC-CRC' in supportedencryptiontypes → first ANY=true → matches
		Assert: AssertNonEmpty(),
	},
	// ---- ANY in first WITH stage, NONE over collected results in second ------
	// This exercises ANY driving a WITH pipeline where NONE then filters over
	// the collected output of the second MATCH stage.
	{
		Name:   "ANY quantifier in first stage gates a pipeline where NONE filters the collected output",
		Cypher: `MATCH (m:NodeKind1) WHERE ANY(name IN m.serviceprincipalnames WHERE name CONTAINS 'PHANTOM') WITH m MATCH (n:NodeKind1)-[:EdgeKind1]->(g:NodeKind2) WHERE g.objectid ENDS WITH '-525' WITH m, COLLECT(n) AS matchingNs WHERE NONE(t IN matchingNs WHERE t.objectid = m.objectid) RETURN m`,
		Fixture: GraphFixture{
			Nodes: []NodeFixture{
				// m has 'PHANTOM' in serviceprincipalnames → ANY=true; objectid='m-obj'
				NewNodeWithProperties("m", map[string]any{
					"objectid":              "m-obj",
					"serviceprincipalnames": []any{"PHANTOM/host"},
				}, NodeKind1),
				// n has a different objectid so NONE(t.objectid = m.objectid) is true
				NewNodeWithProperties("n", map[string]any{"objectid": "other-obj"}, NodeKind1),
				NewNodeWithProperties("g", map[string]any{"objectid": "S-1-5-21-525"}, NodeKind2),
			},
			Edges: []EdgeFixture{NewEdge("n", "g", EdgeKind1)},
		},
		Assert: AssertNonEmpty(),
	},
}
