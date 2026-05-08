package session

import (
	"context"
	"strings"
	"testing"
)

func TestParseCypher(t *testing.T) {
	s := New()

	result, err := s.ParseCypher("match (n) return n limit 1")
	if err != nil {
		t.Fatalf("ParseCypher() error = %v", err)
	}
	if strings.TrimSpace(result["ast"]) == "" {
		t.Fatal("expected AST dump")
	}
}

func TestTranslateCypherToPGSQLWithoutConnection(t *testing.T) {
	s := New()

	result, err := s.TranslateCypherToPGSQL(context.Background(), "match (n) return n limit 1", "", false)
	if err != nil {
		t.Fatalf("TranslateCypherToPGSQL() error = %v", err)
	}
	if !strings.Contains(result.SQL, "SELECT") {
		t.Fatalf("expected translated SQL to contain SELECT, got %q", result.SQL)
	}
}

func TestOpenConnectionValidation(t *testing.T) {
	s := New()

	if _, err := s.OpenConnection(context.Background(), OpenConnectionRequest{}); err == nil {
		t.Fatal("expected missing name error")
	}
	if _, err := s.OpenConnection(context.Background(), OpenConnectionRequest{Name: "local"}); err == nil {
		t.Fatal("expected missing connection string error")
	}
	if _, err := s.OpenConnection(context.Background(), OpenConnectionRequest{Name: "local", ConnectionString: "file://example"}); err == nil {
		t.Fatal("expected unsupported scheme error")
	}
}

func TestRedactSecrets(t *testing.T) {
	secret := "postgres://dawgs:password@example/dawgs"
	message := redactSecrets("failed to open postgres://dawgs:password@example/dawgs", secret)

	if strings.Contains(message, secret) {
		t.Fatalf("expected secret to be redacted, got %q", message)
	}
	if !strings.Contains(message, "<redacted>") {
		t.Fatalf("expected redaction marker, got %q", message)
	}
}
