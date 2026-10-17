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
