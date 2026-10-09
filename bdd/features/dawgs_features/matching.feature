Feature: Match nodes

  Scenario: Match non existed nodes
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      RETURN n
      """
    Then the result should be, in any order:
      | n |

  Scenario: Matching all nodes
    Given an empty graph
    And having executed:
      """
      CREATE (:A), (:B {prefix: 'c', name: 'b'}), ({name: 'c'})
      """
    When executing query:
      """
      MATCH (n)
      RETURN n
      """
    Then the result should be, in any order:
      | n                            |
      | (:A)                         |
      | (:B{name: 'b', prefix: 'c'}) |
      | ({name: 'c'})                |

  Scenario: Matching a relationship pattern using a label predicate on both sides
    Given an empty graph
    And having executed:
      """
      CREATE (:A)-[:T1]->(:B),
      (:B)-[:T2]->(:A),
      (:B)-[:T3]->(:B),
      (:A)-[:T4]->(:A)
      """
    When executing query:
      """
      MATCH (:A)-[r]->(:B)
      RETURN r
      """
    Then the result should be, in any order:
      | r     |
      | [:T1] |
    And no side effects

Scenario: [8] Create a single node with a property and return it
    Given any graph
    When executing query:
      """
      CREATE (n {name: 'foo'})
      RETURN n.name AS p
      """
    Then the result should be, in any order:
      | p     |
      | foo   |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: [9] Create a single node with two properties
    Given any graph
    When executing query:
      """
      CREATE (n {id: 12, name: 'foo'})
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 2 |

  Scenario: [10] Create a single node with two properties and return them
    Given any graph
    When executing query:
      """
      CREATE (n {id: 12, name: 'foo'})
      RETURN n.id AS id, n.name AS p
      """
    Then the result should be, in any order:
      | id | p     |
      | 12 |  foo  |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 2 |

  Scenario: [11] Create a single node with null properties should not return those properties
    Given any graph
    When executing query:
      """
      CREATE (n {id: 12, name: null})
      RETURN n.id AS id, n.name AS p
      """
    Then the result should be, in any order:
      | id | p    |
      | 12 | null |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |

  Scenario: [12] CREATE does not lose precision on large integers
    Given an empty graph
    When executing query:
      """
      CREATE (p:TheLabel {id: 4611686018427387905})
      RETURN p.id
      """
    Then the result should be, in any order:
      | p.id                |
      | 4611686018427387905 |
    And the side effects should be:
      | +nodes      | 1 |
      | +properties | 1 |
