Feature: TriadicSelection1 - Query three related nodes on binary-tree graphs

  Scenario: Load a binary tree fixture
    Given the binary-tree-1 graph
    When executing query:
      """
      MATCH (a:A)-[:KNOWS]->(b)-->(c)
      RETURN c
      """
    Then the result should be, in any order:
      | c                  |
      | (:X {name: "b2"})  |
      | (:X {name: "c12"}) |
      | (:X {name: "c11"}) |
      | (:X {name: "b3"})  |
      | (:X {name: "c22"}) |
      | (:X {name: "c21"}) |
    And no side effects

  Scenario: Start with a clean graph after fixture ingestion
    Given having executed:
      """
      CREATE (:A)
      """
    When executing query:
      """
      MATCH (n)
      RETURN n
      """
    Then the result should be, in any order:
      | n    |
      | (:A) |
    And no side effects
