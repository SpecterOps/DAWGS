Feature: Leading OPTIONAL MATCH

  Scenario: Return one null row when the graph is empty
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (n:A)
      RETURN n
      """
    Then the result should be, in any order:
      | n    |
      | null |
    And no side effects

  Scenario: Return one null row when the predicate rejects all nodes
    Given an empty graph
    And having executed:
      """
      CREATE (:A {name: 'first'}), (:A {name: 'second'})
      """
    When executing query:
      """
      OPTIONAL MATCH (n:A)
      WHERE n.name = 'missing'
      RETURN n, n.name
      """
    Then the result should be, in any order:
      | n    | n.name |
      | null | null   |
    And no side effects

  Scenario: Return every matching node without an extra null row
    Given an empty graph
    And having executed:
      """
      CREATE (:A {name: 'first'}), (:A {name: 'second'}), (:B)
      """
    When executing query:
      """
      OPTIONAL MATCH (n:A)
      RETURN n
      """
    Then the result should be, in any order:
      | n                      |
      | (:A {name: 'first'})   |
      | (:A {name: 'second'})  |
    And no side effects

  Scenario: Return matching node with non null labels without an extra null row
    Given an empty graph
    And having executed:
      """
      CREATE (:A {name: 'first'}), (:A {name: 'second'}), (:B)
      """
    When executing query:
      """
      OPTIONAL MATCH (n:A)
      Where n.name IS NOT NULL
      RETURN n.name
      """
    Then the result should be, in any order:
      | n.name  |
      | first   |
      | second  |
    And no side effects

  Scenario: Null every binding when the relationship pattern does not match
    Given an empty graph
    And having executed:
      """
      CREATE (:A), (:B)
      """
    When executing query:
      """
      OPTIONAL MATCH (a:A)-[r:R]->(b:B)
      RETURN a, r, b
      """
    Then the result should be, in any order:
      | a    | r    | b    |
      | null | null | null |
    And no side effects

  Scenario: Return a matching relationship pattern
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:R]->(:B)
      """
    When executing query:
      """
      OPTIONAL MATCH (a:A)-[r:R]->(b:B)
      RETURN a, r, b
      """
    Then the result should be, in any order:
      | a    | r    | b    |
      | (:A) | [:R] | (:B) |
    And no side effects

  Scenario: Preserve the null row through a subsequent optional match
    Given an empty graph
    When executing query:
      """
      OPTIONAL MATCH (a:A)
      OPTIONAL MATCH (a)-[r:R]->(b:B)
      RETURN a, r, b
      """
    Then the result should be, in any order:
      | a    | b    | r    |
      | null | null | null |
    And no side effects
