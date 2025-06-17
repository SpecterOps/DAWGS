# BloodHound Cypher Library

This project contains a golang parser implementation that for the [openCypher](https://opencypher.org/) project. The
primary goal of this implementation is to be lightweight and compatible with cypher dialect used by
[Neo4j](https://neo4j.com/).

Several features may not be fully supported while this implementation continues to mature. Eventual support for most
legacy openCypher features is planned.

* [Generating the ANTLR Grammar](#generating-the-antlr-grammar)
* [Regenerating the ANTLR Parser Implementation](#regenerating-the-antlr-parser-implementation)
* [Language Features](#language-features)
    * [Query Cost Model](#query-cost-model)
        * [Caveats](#caveats)
        * [Cost Metrics](#cost-metrics)
    * [Filtered Model Features](#filtered-model-features)
    * [Unsupported Model Features](#unsupported-model-features)

## Generating the ANTLR Grammar

openCypher Repository Version Hash: `46223b9e8215814af333b7142850fea99a3949a7`

The [openCypher repository](https://github.com/opencypher/openCypher/tree/46223b9e8215814af333b7142850fea99a3949a7/grammar)
contains instructions on how to generate output artifacts from the grammar XML sources. Below is an example on how to
generate the ANTLR grammar from the checked out version hash:

```bash
./tools/grammar/src/main/shell/launch.sh Antlr4 --INCLUDE_LEGACY=true cypher.xml > grammar/generated/Cypher.g4
```

## Regenerating the ANTLR Parser Implementation

[Download the latest version of ANTLR](https://www.antlr.org/download.html) first and ensure that you have `java`
installed. The command below can be used to generate a new version of the openCypher parser backend.

```bash
java -jar ./antlr-4.13.0-complete.jar -Dlanguage=Go -o parser grammar/Cypher.g4
```

## Language Features

### PostgreSQL Translation

The `models` package contains two implementations: `cypher` and `pgsql`. The `pgsql` package contains a translation
implementation that attempts a best-effort match of cypher semantics using vanilla pgsql syntax.

