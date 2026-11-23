Feature: Path Finding Nearest Path with allShortestPaths

    Scenario: Return Shortest Path using Label Filter
        Given BED-10173 graph
        When executing query:
          """
            MATCH p = allShortestPaths((s:Group)-[:MemberOf|AdminTo|GenericAll*1..6]->(g)) WHERE s.name = 'source-513' AND (g:Tag_Tier_Zero) AND s <> g RETURN p
          """
        Then the result should be, in any order:
          | P |
          | (:Group {name: "source-513",objectid: "source-513"})-[:MemberOf]->(:Group {name: "bridge-a",objectid: "bridge-a"})-[:AdminTo]->(:Computer:Tag_Tier_Zero {name: "tier-near",objectid: "tier-near",system_tags: "admin_tier_0"}) |
          | (:Group {name: "source-513",objectid: "source-513"})-[:MemberOf]->(:Group {name: "bridge-b",objectid: "bridge-b"})-[:AdminTo]->(:Computer:Tag_Tier_Zero {name: "tier-near",objectid: "tier-near",system_tags: "admin_tier_0"}) |
          | (:Group {name: "source-513",objectid: "source-513"})-[:MemberOf]->(:Group {name: "bridge-b",objectid: "bridge-b"})-[:MemberOf]->(:Group {name: "bridge-c",objectid: "bridge-c"})-[:GenericAll]->(:Group:Tag_Tier_Zero {name: "tier-far",objectid: "tier-far",system_tags: "admin_tier_0"}) |
        And no side effects

    Scenario: Return Shortest Path using Equality
        Given BED-10173 graph
        When executing query:
          """
            MATCH p = allShortestPaths((s:Group)-[:MemberOf|AdminTo|GenericAll*1..6]->(g)) WHERE s.name contains 'source-513' AND g.system_tags = 'admin_tier_0' AND s <> g RETURN p
          """
        Then the result should be, in any order:
          | P |
          | (:Group {name: "source-513",objectid: "source-513"})-[:MemberOf]->(:Group {name: "bridge-a",objectid: "bridge-a"})-[:AdminTo]->(:Computer:Tag_Tier_Zero {name: "tier-near",objectid: "tier-near",system_tags: "admin_tier_0"}) |
          | (:Group {name: "source-513",objectid: "source-513"})-[:MemberOf]->(:Group {name: "bridge-b",objectid: "bridge-b"})-[:AdminTo]->(:Computer:Tag_Tier_Zero {name: "tier-near",objectid: "tier-near",system_tags: "admin_tier_0"}) |
          | (:Group {name: "source-513",objectid: "source-513"})-[:MemberOf]->(:Group {name: "bridge-b",objectid: "bridge-b"})-[:MemberOf]->(:Group {name: "bridge-c",objectid: "bridge-c"})-[:GenericAll]->(:Group:Tag_Tier_Zero {name: "tier-far",objectid: "tier-far",system_tags: "admin_tier_0"}) |
        And no side effects

    Scenario: Return Shortest Path using Contains
        Given BED-10173 graph
        When executing query:
          """
            MATCH p = allShortestPaths((s:Group)-[:MemberOf|AdminTo|GenericAll*1..6]->(g)) WHERE s.name = 'source-513' AND g.system_tags contains 'admin_tier_0' AND s <> g RETURN p
          """
        Then the result should be, in any order:
          | P |
          | (:Group {name: "source-513",objectid: "source-513"})-[:MemberOf]->(:Group {name: "bridge-a",objectid: "bridge-a"})-[:AdminTo]->(:Computer:Tag_Tier_Zero {name: "tier-near",objectid: "tier-near",system_tags: "admin_tier_0"}) |
          | (:Group {name: "source-513",objectid: "source-513"})-[:MemberOf]->(:Group {name: "bridge-b",objectid: "bridge-b"})-[:AdminTo]->(:Computer:Tag_Tier_Zero {name: "tier-near",objectid: "tier-near",system_tags: "admin_tier_0"}) |
          | (:Group {name: "source-513",objectid: "source-513"})-[:MemberOf]->(:Group {name: "bridge-b",objectid: "bridge-b"})-[:MemberOf]->(:Group {name: "bridge-c",objectid: "bridge-c"})-[:GenericAll]->(:Group:Tag_Tier_Zero {name: "tier-far",objectid: "tier-far",system_tags: "admin_tier_0"}) |
        And no side effects
