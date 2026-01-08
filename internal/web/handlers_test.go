package web

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/cabewaldrop/claude-db/internal/sql/executor"
	"github.com/cabewaldrop/claude-db/internal/sql/lexer"
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/storage"
)

func setupTestExecutorForHandler(t *testing.T) (*executor.Executor, func()) {
	t.Helper()
	testFile := "test_handler.db"
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

func TestQueryExecuteSuccess(t *testing.T) {
	exec, cleanup := setupTestExecutorForHandler(t)
	defer cleanup()

	// Create a test table and insert data
	createStmt := mustParse(t, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	_, err := exec.Execute(createStmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	insertStmt := mustParse(t, "INSERT INTO test VALUES (1, 'Alice')")
	_, err = exec.Execute(insertStmt)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.PostForm(ts.URL+"/query", url.Values{
		"sql": {"SELECT * FROM test"},
	})
	if err != nil {
		t.Fatalf("Failed to POST /query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "Alice") {
		t.Errorf("Expected 'Alice' in response, got %q", bodyStr)
	}
	if !strings.Contains(bodyStr, "<table") {
		t.Errorf("Expected '<table' in response, got %q", bodyStr)
	}
}

func TestQueryExecuteError(t *testing.T) {
	exec, cleanup := setupTestExecutorForHandler(t)
	defer cleanup()

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.PostForm(ts.URL+"/query", url.Values{
		"sql": {"SELECT * FROM nonexistent"},
	})
	if err != nil {
		t.Fatalf("Failed to POST /query: %v", err)
	}
	defer resp.Body.Close()

	// Should still return 200, error is shown in HTML
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "error") {
		t.Errorf("Expected 'error' class in response, got %q", bodyStr)
	}
	if !strings.Contains(bodyStr, "nonexistent") {
		t.Errorf("Expected 'nonexistent' in error message, got %q", bodyStr)
	}
}

func TestQueryExecuteEmpty(t *testing.T) {
	exec, cleanup := setupTestExecutorForHandler(t)
	defer cleanup()

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.PostForm(ts.URL+"/query", url.Values{
		"sql": {""},
	})
	if err != nil {
		t.Fatalf("Failed to POST /query: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "cannot be empty") {
		t.Errorf("Expected 'cannot be empty' in response, got %q", bodyStr)
	}
}

func TestQueryExecuteTooLong(t *testing.T) {
	exec, cleanup := setupTestExecutorForHandler(t)
	defer cleanup()

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	longQuery := "SELECT " + strings.Repeat("x", 20000)
	resp, err := http.PostForm(ts.URL+"/query", url.Values{
		"sql": {longQuery},
	})
	if err != nil {
		t.Fatalf("Failed to POST /query: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "too long") {
		t.Errorf("Expected 'too long' in response, got %q", bodyStr)
	}
}

func TestQueryExecuteMethodNotAllowed(t *testing.T) {
	exec, cleanup := setupTestExecutorForHandler(t)
	defer cleanup()

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// GET is not allowed
	resp, err := http.Get(ts.URL + "/query")
	if err != nil {
		t.Fatalf("Failed to GET /query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", resp.StatusCode)
	}
}

func TestQueryExecuteNoDatabaseAvailable(t *testing.T) {
	// Create server with nil executor
	srv := NewServer(0, nil)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.PostForm(ts.URL+"/query", url.Values{
		"sql": {"SELECT 1"},
	})
	if err != nil {
		t.Fatalf("Failed to POST /query: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "Database not available") {
		t.Errorf("Expected 'Database not available' in response, got %q", bodyStr)
	}
}

func TestQueryExecuteCreateTable(t *testing.T) {
	exec, cleanup := setupTestExecutorForHandler(t)
	defer cleanup()

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.PostForm(ts.URL+"/query", url.Values{
		"sql": {"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)"},
	})
	if err != nil {
		t.Fatalf("Failed to POST /query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "created") {
		t.Errorf("Expected 'created' in response, got %q", bodyStr)
	}
}

func TestQueryExecuteSyntaxError(t *testing.T) {
	exec, cleanup := setupTestExecutorForHandler(t)
	defer cleanup()

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.PostForm(ts.URL+"/query", url.Values{
		"sql": {"SELECTT * FROM test"},
	})
	if err != nil {
		t.Fatalf("Failed to POST /query: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "error") {
		t.Errorf("Expected 'error' class in response, got %q", bodyStr)
	}
}

func TestQueryExecuteEmptyResult(t *testing.T) {
	exec, cleanup := setupTestExecutorForHandler(t)
	defer cleanup()

	// Create empty table
	createStmt := mustParse(t, "CREATE TABLE empty_table (id INTEGER PRIMARY KEY)")
	_, err := exec.Execute(createStmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.PostForm(ts.URL+"/query", url.Values{
		"sql": {"SELECT * FROM empty_table"},
	})
	if err != nil {
		t.Fatalf("Failed to POST /query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Should show table with 0 rows, not an error
	if strings.Contains(bodyStr, "error") {
		t.Errorf("Should not contain 'error' for empty result, got %q", bodyStr)
	}
	if !strings.Contains(bodyStr, "0 row") {
		t.Errorf("Expected '0 row' in response, got %q", bodyStr)
	}
}

// mustParse is a helper to parse SQL for test setup
func mustParse(t *testing.T, sql string) parser.Statement {
	t.Helper()
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Failed to parse '%s': %v", sql, err)
	}
	return stmt
}
