package query

// selectKindsByNamesQuery resolves existing kind names without registering new kinds.
const selectKindsByNamesQuery = "select id, name from kind where name = ANY(@names::text[])"
