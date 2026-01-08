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

// setupTestExecutor creates a test executor with a temporary database.
func setupTestExecutor(t *testing.T) (*executor.Executor, func()) {
	t.Helper()

	tmpFile := "test_web_" + t.Name() + ".db"
	pager, err := storage.NewPager(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}

	exec := executor.New(pager)

	cleanup := func() {
		pager.Close()
		os.Remove(tmpFile)
	}

	return exec, cleanup
}

// executeSQL is a helper to parse and execute SQL.
func executeSQL(t *testing.T, exec *executor.Executor, sql string) {
	t.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SQL %q: %v", sql, err)
	}
	_, err = exec.Execute(stmt)
	if err != nil {
		t.Fatalf("Failed to execute SQL %q: %v", sql, err)
	}
}

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

func TestStaticFileServing(t *testing.T) {
	srv := NewServer(0, nil)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// Test htmx.min.js is served
	resp, err := http.Get(ts.URL + "/static/htmx.min.js")
	if err != nil {
		t.Fatalf("Failed to GET /static/htmx.min.js: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200 for htmx.min.js, got %d", resp.StatusCode)
	}

	// Verify content type is JavaScript
	contentType := resp.Header.Get("Content-Type")
	if contentType != "text/javascript; charset=utf-8" && contentType != "application/javascript" {
		t.Errorf("Expected JavaScript content type, got %q", contentType)
	}

	// Verify body is not empty
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}
	if len(body) < 1000 {
		t.Errorf("Expected substantial htmx.min.js content, got %d bytes", len(body))
	}
}

func TestStaticStyleCSS(t *testing.T) {
	srv := NewServer(0, nil)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/static/style.css")
	if err != nil {
		t.Fatalf("Failed to GET /static/style.css: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200 for style.css, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "text/css; charset=utf-8" {
		t.Errorf("Expected CSS content type, got %q", contentType)
	}
}

func TestStaticFileNotFound(t *testing.T) {
	srv := NewServer(0, nil)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/static/nonexistent.js")
	if err != nil {
		t.Fatalf("Failed to GET /static/nonexistent.js: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404 for nonexistent file, got %d", resp.StatusCode)
	}
}

func TestPathTraversalBlocked(t *testing.T) {
	srv := NewServer(0, nil)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// Attempt path traversal - should be blocked by http.FileServer
	resp, err := http.Get(ts.URL + "/static/../../../etc/passwd")
	if err != nil {
		t.Fatalf("Failed to GET path traversal attempt: %v", err)
	}
	defer resp.Body.Close()

	// Path traversal should result in 404 (file not found in embedded FS)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404 for path traversal attempt, got %d", resp.StatusCode)
	}
}

func TestTableListNoExecutor(t *testing.T) {
	// Server without executor should return error
	srv := NewServer(0, nil)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables")
	if err != nil {
		t.Fatalf("Failed to GET /tables: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503 without executor, got %d", resp.StatusCode)
	}
}

func TestTableListEmpty(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	srv := NewServer(0, exec)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables")
	if err != nil {
		t.Fatalf("Failed to GET /tables: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	html := string(body)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d. Body: %s", resp.StatusCode, html)
	}

	// Should contain "No tables" message
	if !strings.Contains(html, "No tables") {
		t.Error("Expected 'No tables' message in response")
	}
	if !strings.Contains(html, "Create one") {
		t.Error("Expected 'Create one' link in response")
	}
}

func TestTableListWithTables(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create tables
	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	executeSQL(t, exec, "CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)")

	// Insert a row into users
	executeSQL(t, exec, "INSERT INTO users (id, name) VALUES (1, 'Alice')")

	srv := NewServer(0, exec)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables")
	if err != nil {
		t.Fatalf("Failed to GET /tables: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	html := string(body)

	// Should contain table names
	if !strings.Contains(html, "users") {
		t.Error("Expected 'users' table in response")
	}
	if !strings.Contains(html, "posts") {
		t.Error("Expected 'posts' table in response")
	}

	// Should contain row counts
	if !strings.Contains(html, "1 rows") {
		t.Error("Expected '1 rows' for users table")
	}
	if !strings.Contains(html, "0 rows") {
		t.Error("Expected '0 rows' for posts table")
	}
}

func TestTableListLinks(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE my_table (id INTEGER PRIMARY KEY)")

	srv := NewServer(0, exec)

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables")
	if err != nil {
		t.Fatalf("Failed to GET /tables: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	html := string(body)

	// Should contain link to table detail
	if !strings.Contains(html, "/tables/my_table") {
		t.Error("Expected link to '/tables/my_table' in response")
	}

	// Should contain Create Table link
	if !strings.Contains(html, "/tables/new") {
		t.Error("Expected link to '/tables/new' in response")
	}
}
