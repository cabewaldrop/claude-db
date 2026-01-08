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
	"github.com/cabewaldrop/claude-db/internal/storage"
)

func setupTestServer(t *testing.T) (*Server, func()) {
	t.Helper()
	testFile := "test_handlers.db"
	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}

	exec := executor.New(pager)
	srv := NewServer(0, exec)

	cleanup := func() {
		pager.Close()
		os.Remove(testFile)
	}

	return srv, cleanup
}

func TestCreateTableFormRenders(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/tables/new")
	if err != nil {
		t.Fatalf("Failed to GET /tables/new: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	html := string(body)

	// Check for key form elements
	if !strings.Contains(html, "Create Table") {
		t.Error("expected 'Create Table' in response")
	}
	if !strings.Contains(html, "table_name") {
		t.Error("expected 'table_name' input in response")
	}
	if !strings.Contains(html, "Add Column") {
		t.Error("expected 'Add Column' button in response")
	}
}

func TestCreateTableSuccess(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	// Post form data to create a table
	formData := url.Values{
		"table_name":    {"users"},
		"col_name[]":    {"id", "name"},
		"col_type[]":    {"INTEGER", "TEXT"},
		"col_pk[]":      {"0"},
		"col_notnull[]": {"1"},
	}

	resp, err := http.PostForm(ts.URL+"/tables", formData)
	if err != nil {
		t.Fatalf("Failed to POST /tables: %v", err)
	}
	defer resp.Body.Close()

	// Should redirect on success (303 See Other)
	// Note: http.PostForm follows redirects, so we may get 200 or 404 (if /tables/users doesn't exist)
	// We need to check if the table was actually created

	// Query the executor to verify table was created
	exec := srv.executor
	tables := exec.GetTables()

	found := false
	for _, name := range tables {
		if name == "users" {
			found = true
			break
		}
	}

	if !found {
		t.Error("expected 'users' table to be created")
	}
}

func TestCreateTableInvalidName(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	formData := url.Values{
		"table_name": {"123invalid"},
		"col_name[]": {"id"},
		"col_type[]": {"INTEGER"},
	}

	resp, err := http.PostForm(ts.URL+"/tables", formData)
	if err != nil {
		t.Fatalf("Failed to POST /tables: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	html := string(body)

	if !strings.Contains(html, "Invalid table name") {
		t.Errorf("expected 'Invalid table name' error, got: %s", html)
	}
}

func TestCreateTableNoColumns(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	formData := url.Values{
		"table_name": {"empty_table"},
		// No columns
	}

	resp, err := http.PostForm(ts.URL+"/tables", formData)
	if err != nil {
		t.Fatalf("Failed to POST /tables: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	html := string(body)

	if !strings.Contains(html, "At least one column is required") {
		t.Errorf("expected column required error, got: %s", html)
	}
}

func TestCreateTableInvalidColumnName(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	formData := url.Values{
		"table_name": {"valid_table"},
		"col_name[]": {"invalid-column"},
		"col_type[]": {"INTEGER"},
	}

	resp, err := http.PostForm(ts.URL+"/tables", formData)
	if err != nil {
		t.Fatalf("Failed to POST /tables: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	html := string(body)

	if !strings.Contains(html, "Invalid column name") {
		t.Errorf("expected 'Invalid column name' error, got: %s", html)
	}
}

func TestCreateTableDuplicateTable(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	formData := url.Values{
		"table_name": {"users"},
		"col_name[]": {"id"},
		"col_type[]": {"INTEGER"},
	}

	// Create table first time
	resp, err := http.PostForm(ts.URL+"/tables", formData)
	if err != nil {
		t.Fatalf("Failed to POST /tables: %v", err)
	}
	resp.Body.Close()

	// Try to create same table again
	resp, err = http.PostForm(ts.URL+"/tables", formData)
	if err != nil {
		t.Fatalf("Failed to POST /tables: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	html := string(body)

	// Should show error about table already existing
	if !strings.Contains(html, "already exists") && !strings.Contains(html, "Error") {
		t.Errorf("expected error about duplicate table, got: %s", html)
	}
}

func TestCreateTableWithHTMX(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	// Create request manually to set HX-Request header
	formData := url.Values{
		"table_name": {"htmx_table"},
		"col_name[]": {"id"},
		"col_type[]": {"INTEGER"},
	}

	req := httptest.NewRequest("POST", "/tables", strings.NewReader(formData.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")

	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)

	// Should have HX-Redirect header for HTMX requests
	redirect := rec.Header().Get("HX-Redirect")
	if redirect == "" {
		t.Error("expected HX-Redirect header for HTMX request")
	}
	if !strings.Contains(redirect, "/tables/htmx_table") {
		t.Errorf("expected redirect to /tables/htmx_table, got: %s", redirect)
	}
}

func TestHealthEndpoint(t *testing.T) {
	srv, cleanup := setupTestServer(t)
	defer cleanup()

	ts := httptest.NewServer(srv.Router())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("Failed to GET /health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "OK" {
		t.Errorf("expected 'OK', got: %s", body)
	}
}

func TestContainsIndex(t *testing.T) {
	tests := []struct {
		slice    []string
		idx      int
		expected bool
	}{
		{[]string{"0", "1", "2"}, 1, true},
		{[]string{"0"}, 0, true},
		{[]string{"1", "2"}, 0, false},
		{[]string{}, 0, false},
		{nil, 0, false},
	}

	for _, tt := range tests {
		got := containsIndex(tt.slice, tt.idx)
		if got != tt.expected {
			t.Errorf("containsIndex(%v, %d) = %v, want %v", tt.slice, tt.idx, got, tt.expected)
		}
	}
}
