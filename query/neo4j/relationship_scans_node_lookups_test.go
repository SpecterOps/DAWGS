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

package neo4j_test

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

func scanLookupKinds(names ...string) graph.Kinds {
	kinds := make(graph.Kinds, len(names))
	for idx, name := range names {
		kinds[idx] = graph.StringKind(name)
	}
	return kinds
}

func TestQueryBuilder_RelationshipScans(t *testing.T) {
	t.Run("SCAN-01 base endpoints and relationship ID projection", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.KindIn(query.Start(), scanLookupKinds("ADBase", "AZBase")...),
				query.Kind(query.Relationship(), graph.StringKind("PostProcessed")),
				query.KindIn(query.End(), scanLookupKinds("ADBase", "AZBase")...),
			)),
			query.Returning(query.RelationshipID()),
		),
		"match (s)-[r:PostProcessed]->(e) where (s:ADBase or s:AZBase) and (e:ADBase or e:AZBase) return id(r)",
	))

	t.Run("SCAN-02 excludes Meta endpoints and hydrates relationships", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Not(query.KindIn(query.Start(), scanLookupKinds("Meta", "MetaDetail")...)),
				query.KindIn(query.Relationship(), scanLookupKinds("TrackerA", "TrackerB")...),
				query.Not(query.KindIn(query.End(), scanLookupKinds("Meta", "MetaDetail")...)),
			)),
			query.Returning(query.Relationship()),
		),
		"match (s)-[r:TrackerA|TrackerB]->(e) where not ((s:Meta or s:MetaDetail)) and not ((e:Meta or e:MetaDetail)) return r",
	))

	t.Run("SCAN-03 non-Meta lastseen relationship IDs", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Not(query.KindIn(query.Start(), scanLookupKinds("Meta", "MetaDetail")...)),
				query.Kind(query.Relationship(), graph.StringKind("MigratedEdge")),
				query.Exists(query.RelationshipProperty("lastseen")),
				query.Not(query.KindIn(query.End(), scanLookupKinds("Meta", "MetaDetail")...)),
			)),
			query.Returning(query.RelationshipID()),
		),
		"match (s)-[r:MigratedEdge]->(e) where not ((s:Meta or s:MetaDetail)) and r.lastseen is not null and not ((e:Meta or e:MetaDetail)) return id(r)",
	))

	t.Run("SCAN-04 raw ownership scan", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Relationship(), graph.StringKind("OwnsRaw")),
				query.Kind(query.Start(), graph.StringKind("Entity")),
			)),
			query.Returning(query.Relationship()),
		),
		"match (s)-[r:OwnsRaw]->() where s:Entity return r",
	))

	nineKinds := scanLookupKinds("ADCSEdge01", "ADCSEdge02", "ADCSEdge03", "ADCSEdge04", "ADCSEdge05", "ADCSEdge06", "ADCSEdge07", "ADCSEdge08", "ADCSEdge09")
	t.Run("SCAN-05 consolidated nine-kind inbound scan", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Start(), graph.StringKind("Entity")),
				query.KindIn(query.Relationship(), nineKinds...),
				query.Equals(query.EndID(), graph.ID(202)),
			)),
			query.Returning(query.Relationship(), query.Start()),
		),
		"match (s)-[r:ADCSEdge01|ADCSEdge02|ADCSEdge03|ADCSEdge04|ADCSEdge05|ADCSEdge06|ADCSEdge07|ADCSEdge08|ADCSEdge09]->(e) where s:Entity and id(e) = $p0 return r, s",
		map[string]any{"p0": graph.ID(202)},
	))

	t.Run("SCAN-06 FetchKinds projection order", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Relationship(), graph.StringKind("LocalToComputer")),
				query.Kind(query.End(), graph.StringKind("Computer")),
			)),
			query.Returning(query.StartID(), query.RelationshipID(), query.KindsOf(query.Relationship()), query.EndID()),
		),
		"match (s)-[r:LocalToComputer]->(e) where e:Computer return id(s), id(r), type(r), id(e)",
	))

	t.Run("SCAN-07 directed ID pair projection", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.KindIn(query.Relationship(), scanLookupKinds("MemberOf", "MemberOfLocalGroup")...)),
			query.Returning(query.StartID(), query.EndID()),
		),
		"match (s)-[r:MemberOf|MemberOfLocalGroup]->(e) return id(s), id(e)",
	))

	startKinds := scanLookupKinds("Group", "User", "Computer")
	t.Run("SCAN-08 scenario A", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.KindIn(query.Start(), startKinds...),
				query.InIDs(query.EndID(), graph.ID(202), graph.ID(303)),
				query.KindIn(query.Relationship(), scanLookupKinds("GenericAll", "GenericWrite", "Owns", "WriteOwner", "WriteDACL", "WritePublicInformation")...),
			)),
			query.Returning(query.StartID()),
		),
		"match (s)-[r:GenericAll|GenericWrite|Owns|WriteOwner|WriteDACL|WritePublicInformation]->(e) where (s:Group or s:User or s:Computer) and id(e) in $p0 return id(s)",
		map[string]any{"p0": []graph.ID{202, 303}},
	))

	t.Run("SCAN-08 scenario B", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.KindIn(query.Start(), startKinds...),
				query.InIDs(query.EndID(), graph.ID(202), graph.ID(303)),
				query.Kind(query.End(), graph.StringKind("Computer")),
				query.KindIn(query.Relationship(), scanLookupKinds("GenericAll", "GenericWrite", "Owns", "WriteOwner", "WriteDACL")...),
			)),
			query.Returning(query.StartID()),
		),
		"match (s)-[r:GenericAll|GenericWrite|Owns|WriteOwner|WriteDACL]->(e) where (s:Group or s:User or s:Computer) and id(e) in $p0 and e:Computer return id(s)",
		map[string]any{"p0": []graph.ID{202, 303}},
	))
}

func TestQueryBuilder_NodeLookups(t *testing.T) {
	t.Run("LOOKUP-01 kind disjunction ID projection", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.KindIn(query.Node(), scanLookupKinds("Group", "User")...)),
			query.Returning(query.NodeID()),
		),
		"match (n) where (n:Group or n:User) return id(n)",
	))
	t.Run("LOOKUP-01 exact kind full hydration", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.Kind(query.Node(), graph.StringKind("Tenant"))),
			query.Returning(query.Node()),
		),
		"match (n) where n:Tenant return n",
	))

	t.Run("LOOKUP-02 indexed equality and LIMIT 1", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Node(), graph.StringKind("Computer")),
				query.Equals(query.NodeProperty("objectid"), "S-1-5-21"),
			)),
			query.Returning(query.Node()),
			query.Limit(1),
		),
		"match (n) where n:Computer and n.objectid = $p0 return n limit 1",
		map[string]any{"p0": "S-1-5-21"},
	))
	t.Run("LOOKUP-02 no-kind two-property equality", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Equals(query.NodeProperty("name"), "dc.example.test"),
				query.Equals(query.NodeProperty("enabled"), true),
			)),
			query.Returning(query.NodeID()),
		),
		"match (n) where n.name = $p0 and n.enabled = $p1 return id(n)",
		map[string]any{"p0": "dc.example.test", "p1": true},
	))

	t.Run("LOOKUP-03 boolean property projection order", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Node(), graph.StringKind("Computer")),
				query.Equals(query.NodeProperty("hasura"), true),
			)),
			query.Returning(query.NodeID(), query.NodeProperty("hasura")),
		),
		"match (n) where n:Computer and n.hasura = $p0 return id(n), n.hasura",
		map[string]any{"p0": true},
	))

	t.Run("LOOKUP-04 prefix and domain equality", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Node(), graph.StringKind("Container")),
				query.StringStartsWith(query.NodeProperty("distinguishedname"), "CN=ADMINSDHOLDER,CN=SYSTEM,"),
				query.Equals(query.NodeProperty("domainsid"), "S-1-5-21"),
			)),
			query.Returning(query.Node()),
		),
		"match (n) where n:Container and n.distinguishedname starts with $p0 and n.domainsid = $p1 return n",
		map[string]any{"p0": "CN=ADMINSDHOLDER,CN=SYSTEM,", "p1": "S-1-5-21"},
	))
	t.Run("LOOKUP-04 suffix disjunction", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Node(), graph.StringKind("Group")),
				query.Or(
					query.StringEndsWith(query.NodeProperty("objectid"), "-S-1"),
					query.StringEndsWith(query.NodeProperty("objectid"), "-S-2"),
				),
			)),
			query.Returning(query.NodeID()),
		),
		"match (n) where n:Group and (n.objectid ends with $p0 or n.objectid ends with $p1) return id(n)",
		map[string]any{"p0": "-S-1", "p1": "-S-2"},
	))

	t.Run("LOOKUP-05 case-insensitive prefix", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.CaseInsensitiveStringStartsWith(query.NodeProperty("name"), "Remote Desktop Users%_")),
			query.Returning(query.NodeID()),
		),
		"match (n) where toLower(n.name) starts with $p0 return id(n)",
		map[string]any{"p0": "remote desktop users%_"},
	))
	t.Run("LOOKUP-05 case-insensitive contains", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.CaseInsensitiveStringContains(query.NodeProperty("objectid"), "Approver_GUID")),
			query.Returning(query.Node()),
		),
		"match (n) where toLower(n.objectid) contains $p0 return n",
		map[string]any{"p0": "approver_guid"},
	))

	t.Run("LOOKUP-06 required kind groups and suffix", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.KindIn(query.Node(), scanLookupKinds("Group", "User")...),
				query.Kind(query.Node(), graph.StringKind("Entity")),
				query.StringEndsWith(query.NodeProperty("objectid"), "-512"),
				query.Equals(query.NodeProperty("domainsid"), "S-1-5-21"),
			)),
			query.Returning(query.Node()),
		),
		"match (n) where (n:Group or n:User) and n:Entity and n.objectid ends with $p0 and n.domainsid = $p1 return n",
		map[string]any{"p0": "-512", "p1": "S-1-5-21"},
	))
	t.Run("LOOKUP-06 required and excluded kinds", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Node(), graph.StringKind("Entity")),
				query.Not(query.KindIn(query.Node(), scanLookupKinds("Group", "LocalGroup")...)),
				query.StringEndsWith(query.NodeProperty("objectid"), "-512"),
			)),
			query.Returning(query.Node()),
		),
		"match (n) where n:Entity and not ((n:Group or n:LocalGroup)) and n.objectid ends with $p0 return n",
		map[string]any{"p0": "-512"},
	))

	t.Run("LOOKUP-07 missing name", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.Not(query.Exists(query.NodeProperty("name")))),
			query.Returning(query.Node()),
		),
		"match (n) where not (n.name is not null) return n",
	))

	t.Run("LOOKUP-08 either approver property present", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Node(), graph.StringKind("AZRole")),
				query.Equals(query.NodeProperty("tenantid"), "tenant-1"),
				query.Equals(query.NodeProperty("approvalrequired"), true),
				query.Or(
					query.IsNotNull(query.NodeProperty("userapprovers")),
					query.IsNotNull(query.NodeProperty("groupapprovers")),
				),
			)),
			query.Returning(query.Node()),
		),
		"match (n) where n:AZRole and n.tenantid = $p0 and n.approvalrequired = $p1 and (n.userapprovers is not null or n.groupapprovers is not null) return n",
		map[string]any{"p0": "tenant-1", "p1": true},
	))

	t.Run("LOOKUP-09 ID list full hydration", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.InIDs(query.NodeID(), graph.ID(101), graph.ID(202), graph.ID(101))),
			query.Returning(query.Node()),
		),
		"match (n) where id(n) in $p0 return n",
		map[string]any{"p0": []graph.ID{101, 202, 101}},
	))

	t.Run("LOOKUP-10 nested negated account flags", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Kind(query.Node(), graph.StringKind("User")),
				query.Not(query.And(
					query.Exists(query.NodeProperty("gmsa")),
					query.Equals(query.NodeProperty("gmsa"), true),
				)),
				query.Not(query.And(
					query.Exists(query.NodeProperty("msa")),
					query.Equals(query.NodeProperty("msa"), true),
				)),
				query.InIDs(query.NodeID(), graph.ID(101), graph.ID(202)),
			)),
			query.Returning(query.Node()),
		),
		"match (n) where n:User and not (n.gmsa is not null and n.gmsa = $p0) and not (n.msa is not null and n.msa = $p1) and id(n) in $p2 return n",
		map[string]any{"p0": true, "p1": true, "p2": []graph.ID{101, 202}},
	))

	t.Run("LOOKUP-11 tenant adjacency with endpoint list property", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Equals(query.StartID(), graph.ID(101)),
				query.Kind(query.Relationship(), graph.StringKind("Contains")),
				query.KindIn(query.End(), scanLookupKinds("AZRole", "AZServicePrincipal")...),
				query.In(query.EndProperty("roletemplateid"), []string{"role-a", "role-b"}),
			)),
			query.Returning(query.End()),
		),
		"match (s)-[r:Contains]->(e) where id(s) = $p0 and (e:AZRole or e:AZServicePrincipal) and e.roletemplateid in $p1 return e",
		map[string]any{"p0": graph.ID(101), "p1": []string{"role-a", "role-b"}},
	))

	t.Run("LOOKUP-12 exact relationship key First", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Equals(query.StartID(), graph.ID(101)),
				query.Equals(query.EndID(), graph.ID(202)),
				query.Kind(query.Relationship(), graph.StringKind("MemberOf")),
			)),
			query.Returning(query.Relationship()),
			query.Limit(1),
		),
		"match (s)-[r:MemberOf]->(e) where id(s) = $p0 and id(e) = $p1 return r limit 1",
		map[string]any{"p0": graph.ID(101), "p1": graph.ID(202)},
	))

	t.Run("LOOKUP-13 suffix and bound endpoint full start", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.StringEndsWith(query.StartProperty("objectid"), "-555"),
				query.Kind(query.Relationship(), graph.StringKind("LocalToComputer")),
				query.Equals(query.EndID(), graph.ID(202)),
			)),
			query.Returning(query.Start()),
		),
		"match (s)-[r:LocalToComputer]->(e) where s.objectid ends with $p0 and id(e) = $p1 return s",
		map[string]any{"p0": "-555", "p1": graph.ID(202)},
	))
	t.Run("LOOKUP-13 suffix and bound endpoint start ID", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.StringEndsWith(query.StartProperty("objectid"), "-555"),
				query.Kind(query.Relationship(), graph.StringKind("LocalToComputer")),
				query.Equals(query.EndID(), graph.ID(202)),
			)),
			query.Returning(query.StartID()),
		),
		"match (s)-[r:LocalToComputer]->(e) where s.objectid ends with $p0 and id(e) = $p1 return id(s)",
		map[string]any{"p0": "-555", "p1": graph.ID(202)},
	))

	t.Run("LOOKUP-14 descending property order", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.Kind(query.Node(), graph.StringKind("Domain"))),
			query.Returning(query.Node()),
			query.OrderBy(query.Order(query.NodeProperty("name"), query.Descending())),
		),
		"match (n) where n:Domain return n order by n.name desc",
	))

	ntlmCriteria := query.And(
		query.Kind(query.Node(), graph.StringKind("Computer")),
		query.Equals(query.NodeProperty("domainsid"), "S-1-5-21"),
		query.Equals(query.NodeProperty("isdc"), true),
		query.Equals(query.NodeProperty("ldapavailable"), true),
		query.Equals(query.NodeProperty("ldapsigning"), false),
	)
	t.Run("LOOKUP-16 typed NTLM ID projection", assertQueryResult(
		query.SinglePartQuery(query.Where(ntlmCriteria), query.Returning(query.NodeID())),
		"match (n) where n:Computer and n.domainsid = $p0 and n.isdc = $p1 and n.ldapavailable = $p2 and n.ldapsigning = $p3 return id(n)",
		map[string]any{"p0": "S-1-5-21", "p1": true, "p2": true, "p3": false},
	))
	t.Run("LOOKUP-16 untyped NTLM full hydration", assertQueryResult(
		query.SinglePartQuery(
			query.Where(query.And(
				query.Equals(query.NodeProperty("domainsid"), "S-1-5-21"),
				query.Equals(query.NodeProperty("isdc"), true),
				query.Equals(query.NodeProperty("ldapsavailable"), true),
				query.Equals(query.NodeProperty("epa"), false),
			)),
			query.Returning(query.Node()),
		),
		"match (n) where n.domainsid = $p0 and n.isdc = $p1 and n.ldapsavailable = $p2 and n.epa = $p3 return n",
		map[string]any{"p0": "S-1-5-21", "p1": true, "p2": true, "p3": false},
	))
}
