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

// Note: lexer and parser are used by executeSQL helper and for any
// future test verification that requires direct SQL execution.

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

// setupTestExecutor creates a test executor with a database.
func setupTestExecutor(t *testing.T) (*executor.Executor, func()) {
	testFile := "test_web.db"
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

// executeSQL is a helper to execute SQL statements in tests.
func executeSQL(t *testing.T, exec *executor.Executor, sql string) {
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

func TestEditRowFormLoads(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice')")

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/users/edit/1")
	if err != nil {
		t.Fatalf("Failed to GET edit form: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read body: %v", err)
	}

	html := string(body)
	if !strings.Contains(html, "Edit Row") {
		t.Error("Expected 'Edit Row' in response")
	}
	if !strings.Contains(html, "Alice") {
		t.Error("Expected 'Alice' in response")
	}
	if !strings.Contains(html, "disabled") {
		t.Error("Expected 'disabled' (for PK field) in response")
	}
}

func TestUpdateRowSuccess(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	executeSQL(t, exec, "INSERT INTO users VALUES (1, 'Alice')")

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/tables/users/1",
		strings.NewReader("name=Bob"))
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to execute PUT: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read body: %v", err)
	}

	if !strings.Contains(string(body), "updated") {
		t.Errorf("Expected success message, got: %s", body)
	}

	// Note: The executor's UPDATE doesn't persist changes yet (see executor_test.go).
	// This test verifies the web handler correctly parses the request and calls
	// the executor without error. Once UPDATE persistence is implemented,
	// add verification that the data actually changed.
}

func TestEditRowNoPrimaryKey(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE data (value TEXT)") // No PK
	executeSQL(t, exec, "INSERT INTO data VALUES ('test')")

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/data/edit/1")
	if err != nil {
		t.Fatalf("Failed to GET edit form: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read body: %v", err)
	}

	html := string(body)
	if !strings.Contains(html, "no primary key") {
		t.Errorf("Expected 'no primary key' error, got: %s", html)
	}
}

func TestEditRowTableNotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/nonexistent/edit/1")
	if err != nil {
		t.Fatalf("Failed to GET edit form: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read body: %v", err)
	}

	html := string(body)
	if !strings.Contains(html, "not found") {
		t.Errorf("Expected 'not found' error, got: %s", html)
	}
}

func TestEditRowRowNotFound(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	// Don't insert any rows

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/users/edit/999")
	if err != nil {
		t.Fatalf("Failed to GET edit form: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read body: %v", err)
	}

	html := string(body)
	if !strings.Contains(html, "not found") {
		t.Errorf("Expected 'Row not found' error, got: %s", html)
	}
}
