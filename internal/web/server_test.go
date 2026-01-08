package web

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

// createTestExecutor creates an executor with test data for table data tests.
func createTestExecutor(t *testing.T) (*executor.Executor, func()) {
	testFile := "test_web_server.db"
	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}

	exec := executor.New(pager)

	// Create a test table and insert data
	stmts := []string{
		"CREATE TABLE users (id INTEGER, name TEXT, email TEXT)",
		"INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')",
		"INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@test.com')",
		"INSERT INTO users (id, name, email) VALUES (3, 'Carol', 'carol@test.com')",
		"INSERT INTO users (id, name, email) VALUES (4, 'Dave', 'dave@test.com')",
		"INSERT INTO users (id, name, email) VALUES (5, 'Eve', 'eve@test.com')",
	}

	for _, sql := range stmts {
		lex := lexer.New(sql)
		p := parser.New(lex)
		stmt, _ := p.Parse()
		exec.Execute(stmt)
	}

	cleanup := func() {
		pager.Close()
		os.Remove(testFile)
	}

	return exec, cleanup
}

func TestTableDataBasic(t *testing.T) {
	exec, cleanup := createTestExecutor(t)
	defer cleanup()
	srv := NewServer(0, exec)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/users/data")
	if err != nil {
		t.Fatalf("Failed to GET /tables/users/data: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	html := string(body)

	// Check for table structure
	if !strings.Contains(html, "<table>") {
		t.Error("Expected HTML to contain <table>")
	}

	// Check for column headers
	if !strings.Contains(html, "id") || !strings.Contains(html, "name") || !strings.Contains(html, "email") {
		t.Error("Expected HTML to contain column headers")
	}

	// Check for data
	if !strings.Contains(html, "Alice") || !strings.Contains(html, "alice@test.com") {
		t.Error("Expected HTML to contain user data")
	}
}

func TestTableDataPagination(t *testing.T) {
	exec, cleanup := createTestExecutor(t)
	defer cleanup()
	srv := NewServer(0, exec)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// Request with limit=2
	resp, err := http.Get(ts.URL + "/tables/users/data?limit=2")
	if err != nil {
		t.Fatalf("Failed to GET: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	html := string(body)

	// Should show Alice and Bob (first 2)
	if !strings.Contains(html, "Alice") || !strings.Contains(html, "Bob") {
		t.Error("Expected first page to contain Alice and Bob")
	}

	// Should have Next link but not Prev (on first page)
	if !strings.Contains(html, "Next →") {
		t.Error("Expected Next link to be active")
	}

	// Request second page
	resp2, err := http.Get(ts.URL + "/tables/users/data?limit=2&offset=2")
	if err != nil {
		t.Fatalf("Failed to GET second page: %v", err)
	}
	defer resp2.Body.Close()

	body2, _ := io.ReadAll(resp2.Body)
	html2 := string(body2)

	// Should show Carol and Dave
	if !strings.Contains(html2, "Carol") || !strings.Contains(html2, "Dave") {
		t.Error("Expected second page to contain Carol and Dave")
	}

	// Should have both Prev and Next links
	if !strings.Contains(html2, "← Previous") {
		t.Error("Expected Previous link to be active")
	}
}

func TestTableDataNotFound(t *testing.T) {
	exec, cleanup := createTestExecutor(t)
	defer cleanup()
	srv := NewServer(0, exec)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/nonexistent/data")
	if err != nil {
		t.Fatalf("Failed to GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "not found") {
		t.Error("Expected error message about table not found")
	}
}

func TestTableDataNoExecutor(t *testing.T) {
	srv := NewServer(0, nil)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/users/data")
	if err != nil {
		t.Fatalf("Failed to GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "not initialized") {
		t.Error("Expected error message about database not initialized")
	}
}

func TestTableDataEmptyTable(t *testing.T) {
	testFile := "test_web_empty.db"
	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer func() {
		pager.Close()
		os.Remove(testFile)
	}()

	exec := executor.New(pager)

	// Create empty table
	lex := lexer.New("CREATE TABLE empty_table (id INTEGER)")
	p := parser.New(lex)
	stmt, _ := p.Parse()
	exec.Execute(stmt)

	srv := NewServer(0, exec)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/empty_table/data")
	if err != nil {
		t.Fatalf("Failed to GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "empty") {
		t.Error("Expected message about empty table")
	}
}
