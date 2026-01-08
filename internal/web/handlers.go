package web

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cabewaldrop/claude-db/internal/sql/executor"
	"github.com/cabewaldrop/claude-db/internal/sql/lexer"
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
)

// handleIndex serves the main page of the web UI.
// This will be expanded with actual HTML templates in a future task.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<!DOCTYPE html>
<html>
<head>
    <title>ClaudeDB</title>
</head>
<body>
    <h1>ClaudeDB Web Interface</h1>
    <p>Welcome to ClaudeDB. This interface will be expanded with query capabilities.</p>
    <p><a href="/health">Health Check</a></p>
</body>
</html>`))
}

// handleHealth returns a simple health check response.
// This endpoint is used by load balancers and monitoring systems.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// handleQueryExecute executes SQL queries submitted via the web form.
//
// EDUCATIONAL NOTE:
// -----------------
// This handler receives SQL from the web form and executes it via the
// executor. It returns HTML partials that can be swapped into the page
// using HTMX or similar techniques.
//
// Security considerations:
// - Query length is limited to prevent abuse
// - SQL errors are displayed without exposing internal details
// - No confirmation for destructive operations (UI responsibility)
func (s *Server) handleQueryExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sql := strings.TrimSpace(r.FormValue("sql"))
	if sql == "" {
		renderError(w, "Query cannot be empty", "")
		return
	}

	// Limit query length to prevent abuse
	const maxQueryLen = 10000
	if len(sql) > maxQueryLen {
		renderError(w, fmt.Sprintf("Query too long (max %d characters)", maxQueryLen), "")
		return
	}

	exec := GetExecutor(r)
	if exec == nil {
		renderError(w, "Database not available", sql)
		return
	}

	// Check if EXPLAIN requested
	if r.FormValue("explain") == "1" {
		sql = "EXPLAIN " + sql
	}

	// Parse and execute the query with timing
	start := time.Now()

	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		renderError(w, fmt.Sprintf("SQL syntax error: %s", err.Error()), sql)
		return
	}

	// Check for parser errors
	if errs := p.Errors(); len(errs) > 0 {
		renderError(w, fmt.Sprintf("Parse error: %s", strings.Join(errs, "; ")), sql)
		return
	}

	result, err := exec.Execute(stmt)
	duration := time.Since(start)

	if err != nil {
		renderError(w, err.Error(), sql)
		return
	}

	renderResults(w, result, duration)
}

// renderError renders an error message using the error template.
func renderError(w http.ResponseWriter, errMsg, query string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK) // 200 OK, error shown in HTML

	data := TemplateData{
		Error: errMsg,
		Query: query,
	}

	if err := RenderTemplate(w, "error", data); err != nil {
		// Fallback to plain text if template fails
		http.Error(w, errMsg, http.StatusInternalServerError)
	}
}

// renderResults renders query results using the results template.
func renderResults(w http.ResponseWriter, result *executor.Result, duration time.Duration) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	data := TemplateData{
		Columns:  result.Columns,
		Rows:     result.Rows,
		RowCount: len(result.Rows),
		Duration: duration,
		Message:  result.Message,
	}

	if err := RenderTemplate(w, "results", data); err != nil {
		// Fallback to plain error if template fails
		http.Error(w, "Failed to render results", http.StatusInternalServerError)
	}
}
