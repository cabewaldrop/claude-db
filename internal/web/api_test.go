package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cabewaldrop/claude-db/internal/sql/executor"
	"github.com/cabewaldrop/claude-db/internal/sql/lexer"
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/storage"
)

// createTestExecutor creates an executor with an in-memory database.
func createTestExecutor(t *testing.T) *executor.Executor {
	t.Helper()

	pager, err := storage.NewPager(":memory:")
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	return executor.New(pager)
}

// executeSQL parses and executes a SQL statement.
func executeSQL(t *testing.T, exec *executor.Executor, sql string) {
	t.Helper()

	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("failed to parse %q: %v", sql, err)
	}

	_, err = exec.Execute(stmt)
	if err != nil {
		t.Fatalf("failed to execute %q: %v", sql, err)
	}
}

func TestAPITablesWithoutExecutor(t *testing.T) {
	srv := NewServer(0, nil)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/tables")
	if err != nil {
		t.Fatalf("Failed to GET /api/tables: %v", err)
	}
	defer resp.Body.Close()

	// Should return 503 when executor is nil
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", resp.StatusCode)
	}
}

func TestAPITablesWithExecutor(t *testing.T) {
	exec := createTestExecutor(t)
	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/tables")
	if err != nil {
		t.Fatalf("Failed to GET /api/tables: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !apiResp.Success {
		t.Errorf("Expected success=true, got false: %s", apiResp.Error)
	}
}

func TestAPIQuery(t *testing.T) {
	exec := createTestExecutor(t)
	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// Test CREATE TABLE via query API
	reqBody := QueryRequest{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)"}
	body, _ := json.Marshal(reqBody)

	resp, err := http.Post(ts.URL+"/api/query", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to POST /api/query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !apiResp.Success {
		t.Errorf("Expected success=true, got false: %s", apiResp.Error)
	}
}

func TestAPIQuerySelect(t *testing.T) {
	exec := createTestExecutor(t)
	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// Create table
	reqBody := QueryRequest{SQL: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"}
	body, _ := json.Marshal(reqBody)
	resp, _ := http.Post(ts.URL+"/api/query", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	// Insert data
	reqBody = QueryRequest{SQL: "INSERT INTO users (id, name) VALUES (1, 'Alice')"}
	body, _ = json.Marshal(reqBody)
	resp, _ = http.Post(ts.URL+"/api/query", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	// Select data
	reqBody = QueryRequest{SQL: "SELECT * FROM users"}
	body, _ = json.Marshal(reqBody)
	resp, err := http.Post(ts.URL+"/api/query", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to POST /api/query: %v", err)
	}
	defer resp.Body.Close()

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !apiResp.Success {
		t.Errorf("Expected success=true, got false: %s", apiResp.Error)
	}

	// Check that we got data back
	data, ok := apiResp.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected data to be a map, got %T", apiResp.Data)
	}

	rowCount, ok := data["row_count"].(float64)
	if !ok || rowCount != 1 {
		t.Errorf("Expected row_count=1, got %v", data["row_count"])
	}
}

func TestAPIQueryParseError(t *testing.T) {
	exec := createTestExecutor(t)
	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// Invalid SQL
	reqBody := QueryRequest{SQL: "SELEKT * FROM users"}
	body, _ := json.Marshal(reqBody)

	resp, err := http.Post(ts.URL+"/api/query", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to POST /api/query: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", resp.StatusCode)
	}

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if apiResp.Success {
		t.Errorf("Expected success=false for invalid SQL")
	}
}

func TestAPITableSchema(t *testing.T) {
	exec := createTestExecutor(t)

	// Create table directly via executor
	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)")

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// Get schema
	resp, err := http.Get(ts.URL + "/api/tables/users")
	if err != nil {
		t.Fatalf("Failed to GET /api/tables/users: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !apiResp.Success {
		t.Errorf("Expected success=true, got false: %s", apiResp.Error)
	}

	// Check schema data
	data, ok := apiResp.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected data to be a map, got %T", apiResp.Data)
	}

	if data["name"] != "users" {
		t.Errorf("Expected name=users, got %v", data["name"])
	}

	columns, ok := data["columns"].([]interface{})
	if !ok || len(columns) != 3 {
		t.Errorf("Expected 3 columns, got %v", data["columns"])
	}
}

func TestAPITableSchemaNotFound(t *testing.T) {
	exec := createTestExecutor(t)
	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/tables/nonexistent")
	if err != nil {
		t.Fatalf("Failed to GET /api/tables/nonexistent: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", resp.StatusCode)
	}
}

func TestAPITableRows(t *testing.T) {
	exec := createTestExecutor(t)

	// Create table and insert data directly via executor
	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	executeSQL(t, exec, "INSERT INTO users (id, name) VALUES (1, 'Alice')")
	executeSQL(t, exec, "INSERT INTO users (id, name) VALUES (2, 'Bob')")
	executeSQL(t, exec, "INSERT INTO users (id, name) VALUES (3, 'Charlie')")

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// Get rows
	resp, err := http.Get(ts.URL + "/api/tables/users/rows")
	if err != nil {
		t.Fatalf("Failed to GET /api/tables/users/rows: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !apiResp.Success {
		t.Errorf("Expected success=true, got false: %s", apiResp.Error)
	}

	data, ok := apiResp.Data.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected data to be a map, got %T", apiResp.Data)
	}

	totalCount, ok := data["total_count"].(float64)
	if !ok || totalCount != 3 {
		t.Errorf("Expected total_count=3, got %v", data["total_count"])
	}

	rows, ok := data["rows"].([]interface{})
	if !ok || len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %v", len(rows))
	}
}

func TestAPITableRowsPagination(t *testing.T) {
	exec := createTestExecutor(t)

	// Create table and insert 10 rows
	executeSQL(t, exec, "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 1; i <= 10; i++ {
		sql := fmt.Sprintf("INSERT INTO nums (id, val) VALUES (%d, %d)", i, i*10)
		executeSQL(t, exec, sql)
	}

	srv := NewServer(0, exec)
	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// Get first page (limit=3)
	resp, err := http.Get(ts.URL + "/api/tables/nums/rows?limit=3&offset=0")
	if err != nil {
		t.Fatalf("Failed to GET rows: %v", err)
	}
	defer resp.Body.Close()

	var apiResp APIResponse
	json.NewDecoder(resp.Body).Decode(&apiResp)

	data, _ := apiResp.Data.(map[string]interface{})
	rows, _ := data["rows"].([]interface{})

	if len(rows) != 3 {
		t.Errorf("Expected 3 rows for limit=3, got %d", len(rows))
	}

	hasMore, _ := data["has_more"].(bool)
	if !hasMore {
		t.Errorf("Expected has_more=true")
	}
}
