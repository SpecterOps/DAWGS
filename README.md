# DAWGS

Database Abstraction Wrapper for Graph Schemas

## Purpose

DAWGS is a collection of tools and query language helpers to enable running property graphs on vanilla PostgreSQL
without the need for additional plugins.

At the core of the library is an abstraction layer that allows users to swap out existing database backends (currently
Neo4j and PostgreSQL) or build their own with no change to query implementation. The query interface is built around
openCypher with translation implementations for backends that do not natively support the query language.