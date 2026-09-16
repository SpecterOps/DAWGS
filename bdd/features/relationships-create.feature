Feature: Create2 - Creating relationships

  Scenario: [1] Create two nodes and a single relationship in a single pattern
    Given any graph
    When executing query:
      """
      CREATE ()-[:R]->()
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: [2] Create two nodes and a single relationship in separate patterns
    Given any graph
    When executing query:
      """
      CREATE (a), (b),
             (a)-[:R]->(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |

  Scenario: [3] Create two nodes and a single relationship in separate clauses
    Given any graph
    When executing query:
      """
      CREATE (a)
      CREATE (b)
      CREATE (a)-[:R]->(b)
      """
    Then the result should be empty
    And the side effects should be:
      | +nodes         | 2 |
      | +relationships | 1 |
