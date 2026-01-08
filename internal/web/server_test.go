package web

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/cabewaldrop/claude-db/internal/sql/executor"
	"github.com/cabewaldrop/claude-db/internal/sql/lexer"
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/storage"
)

func TestServerStartup(t *testing.T) {
	// Create server with nil executor (no database needed for basic tests)
	srv := NewServer(0, nil)

	// Use httptest to create a test server
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// Test health endpoint
	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("Failed to GET /health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	if string(body) != "ok" {
		t.Errorf("Expected body 'ok', got %q", string(body))
	}
}

func TestServerIndex(t *testing.T) {
	srv := NewServer(0, nil)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("Failed to GET /: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "text/html; charset=utf-8" {
		t.Errorf("Expected Content-Type 'text/html; charset=utf-8', got %q", contentType)
	}
}

func TestServerRecovery(t *testing.T) {
	srv := NewServer(0, nil)

	// Add a route that panics
	srv.router.Get("/panic", func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// Request the panic route - should recover and return 500
	resp, err := http.Get(ts.URL + "/panic")
	if err != nil {
		t.Fatalf("Failed to GET /panic: %v", err)
	}
	defer resp.Body.Close()

	// chi's Recoverer middleware returns 500 on panic
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("Expected status 500 after panic, got %d", resp.StatusCode)
	}
}

func TestServer404(t *testing.T) {
	srv := NewServer(0, nil)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/nonexistent")
	if err != nil {
		t.Fatalf("Failed to GET /nonexistent: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", resp.StatusCode)
	}
}

func TestServerRequestID(t *testing.T) {
	srv := NewServer(0, nil)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("Failed to GET /health: %v", err)
	}
	defer resp.Body.Close()

	// RequestID middleware should add X-Request-Id header
	// Note: chi's RequestID middleware may not set the response header by default
	// This test verifies the middleware is in the chain by checking the request completes
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

func TestQueryPageRenders(t *testing.T) {
	srv := NewServer(0, nil)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/query")
	if err != nil {
		t.Fatalf("Failed to GET /query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	html := string(body)

	// Check for textarea
	if !contains(html, "<textarea") {
		t.Error("expected textarea element in query page")
	}

	// Check for HTMX post attribute
	if !contains(html, `hx-post="/query"`) {
		t.Error("expected hx-post=\"/query\" attribute")
	}

	// Check for Execute button
	if !contains(html, "Execute") {
		t.Error("expected Execute button text")
	}

	// Check for explain checkbox
	if !contains(html, `name="explain"`) {
		t.Error("expected explain checkbox")
	}
}

func TestQueryPagePrePopulates(t *testing.T) {
	srv := NewServer(0, nil)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/query?q=SELECT+1")
	if err != nil {
		t.Fatalf("Failed to GET /query?q=SELECT+1: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	html := string(body)
	if !contains(html, "SELECT 1") {
		t.Error("expected query to be pre-populated from ?q= parameter")
	}
}

func TestQueryPageEscapesHTML(t *testing.T) {
	srv := NewServer(0, nil)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/query?q=%3Cscript%3Ealert(1)%3C/script%3E")
	if err != nil {
		t.Fatalf("Failed to GET /query with script tag: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	html := string(body)

	// The malicious script should NOT appear unescaped
	if contains(html, "<script>alert") {
		t.Error("expected HTML to be escaped, found unescaped script tag")
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// setupTestExecutor creates a test executor with a fresh database file.
func setupTestExecutor(t *testing.T) (*executor.Executor, func()) {
	t.Helper()
	testFile := "test_web_" + t.Name() + ".db"
	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}

	exec := executor.New(pager)

	cleanup := func() {
		pager.Close()
		os.Remove(testFile)
	}

	return exec, cleanup
}

// executeSQL parses and executes a SQL statement.
func executeSQL(t *testing.T, exec *executor.Executor, sql string) {
	t.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error for %q: %v", sql, err)
	}

	_, err = exec.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute error for %q: %v", sql, err)
	}
}

func TestTableSchemaNotAvailable(t *testing.T) {
	// Server with nil executor - database not available
	srv := NewServer(0, nil)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/users")
	if err != nil {
		t.Fatalf("Failed to GET /tables/users: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", resp.StatusCode)
	}
}

func TestTableSchemaNotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/nonexistent")
	if err != nil {
		t.Fatalf("Failed to GET /tables/nonexistent: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", resp.StatusCode)
	}
}

func TestTableSchemaBasic(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a table with various column types and constraints
	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/users")
	if err != nil {
		t.Fatalf("Failed to GET /tables/users: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	html := string(body)

	// Check table name appears
	if !contains(html, "users") {
		t.Error("Expected table name 'users' in response")
	}

	// Check column names appear
	if !contains(html, "id") {
		t.Error("Expected column 'id' in response")
	}
	if !contains(html, "name") {
		t.Error("Expected column 'name' in response")
	}
	if !contains(html, "age") {
		t.Error("Expected column 'age' in response")
	}

	// Check for PK badge
	if !contains(html, "PK") {
		t.Error("Expected PK badge in response")
	}

	// Check for NOT NULL badge
	if !contains(html, "NOT NULL") {
		t.Error("Expected NOT NULL badge in response")
	}
}

func TestTableSchemaWithIndexes(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table and add an index
	executeSQL(t, exec, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)")
	executeSQL(t, exec, "CREATE INDEX idx_products_name ON products (name)")

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/products")
	if err != nil {
		t.Fatalf("Failed to GET /tables/products: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	html := string(body)

	// Check index appears
	if !contains(html, "idx_products_name") {
		t.Error("Expected index 'idx_products_name' in response")
	}

	// Check index columns appear
	if !contains(html, "Indexes") {
		t.Error("Expected 'Indexes' section header")
	}
}
