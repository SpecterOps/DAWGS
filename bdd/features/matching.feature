Feature: Match nodes

  Scenario: Match non existed nodes
    Given an empty graph
    When executing query:
      """
      MATCH (n)
      RETURN n
      """
    Then the result should be:
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
    Then the result should be:
      | n                             |
      | (:A)                          |
      | (:B{name: 'b', prefix: 'c'})  |
      | ({name: 'c'})                 |
