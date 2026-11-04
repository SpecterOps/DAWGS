package cypher

// UnescapeKindName decodes one Cypher schema-name token. AST kind fields store
// the resulting semantic name, never source delimiters. Do not apply this to
// names supplied through graph.StringKind or database kind APIs.
func UnescapeKindName(token string) string {
	return UnescapePropertyKeyName(token)
}

// EscapeKindName emits a raw label or relationship type as a schema-name token.
// Literal backticks, including surrounding backticks, are data and are escaped.
// Labels, relationship types and property keys share the schema-name grammar,
// but kinds do not inherit property-key validation (including empty-key policy).
func EscapeKindName(raw string) string {
	return EscapePropertyKeyName(raw)
}
