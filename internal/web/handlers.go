package web

import (
	"fmt"
	"html"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

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
		renderErrorPartial(w, "Query cannot be empty", "")
		return
	}

	// Limit query length to prevent abuse
	const maxQueryLen = 10000
	if len(sql) > maxQueryLen {
		renderErrorPartial(w, fmt.Sprintf("Query too long (max %d characters)", maxQueryLen), "")
		return
	}

	exec := GetExecutor(r)
	if exec == nil {
		renderErrorPartial(w, "Database not available", sql)
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
		renderErrorPartial(w, fmt.Sprintf("SQL syntax error: %s", err.Error()), sql)
		return
	}

	// Check for parser errors
	if errs := p.Errors(); len(errs) > 0 {
		renderErrorPartial(w, fmt.Sprintf("Parse error: %s", strings.Join(errs, "; ")), sql)
		return
	}

	result, err := exec.Execute(stmt)
	duration := time.Since(start)

	if err != nil {
		renderErrorPartial(w, err.Error(), sql)
		return
	}

	renderResultsPartial(w, result, sql, duration)
}

// renderErrorPartial renders an error message as an HTML partial.
func renderErrorPartial(w http.ResponseWriter, errMsg, query string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK) // 200 OK, error shown in HTML

	var queryHTML string
	if query != "" {
		queryHTML = fmt.Sprintf(`<pre class="query">%s</pre>`, html.EscapeString(query))
	}

	fmt.Fprintf(w, `<div class="error">
<p class="error-message">%s</p>
%s
</div>`, html.EscapeString(errMsg), queryHTML)
}

// renderResultsPartial renders query results as an HTML table partial.
func renderResultsPartial(w http.ResponseWriter, result *executor.Result, query string, duration time.Duration) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	var sb strings.Builder

	// If there's a message (e.g., "Table created", "1 row inserted")
	if result.Message != "" {
		sb.WriteString(fmt.Sprintf(`<div class="result">
<p class="message">%s</p>
<p class="duration">Duration: %v</p>
</div>`, html.EscapeString(result.Message), duration))
		w.Write([]byte(sb.String()))
		return
	}

	// Render table with results
	sb.WriteString(`<div class="result">`)
	sb.WriteString(`<table class="results-table">`)

	// Header row
	sb.WriteString("<thead><tr>")
	for _, col := range result.Columns {
		sb.WriteString(fmt.Sprintf("<th>%s</th>", html.EscapeString(col)))
	}
	sb.WriteString("</tr></thead>")

	// Data rows
	sb.WriteString("<tbody>")
	for _, row := range result.Rows {
		sb.WriteString("<tr>")
		for _, val := range row {
			sb.WriteString(fmt.Sprintf("<td>%s</td>", html.EscapeString(val.String())))
		}
		sb.WriteString("</tr>")
	}
	sb.WriteString("</tbody>")

	sb.WriteString("</table>")

	// Footer with row count and duration
	sb.WriteString(fmt.Sprintf(`<p class="footer">%d row(s) returned in %v</p>`,
		len(result.Rows), duration))
	sb.WriteString("</div>")

	w.Write([]byte(sb.String()))
}

// handleQueryPage serves the SQL query input form.
// Supports pre-populating the query via ?q= query parameter for bookmarking.
func (s *Server) handleQueryPage(w http.ResponseWriter, r *http.Request) {
	data := map[string]interface{}{
		"Query": "", // Empty initially
	}

	// Pre-populate from query param if provided
	if q := r.URL.Query().Get("q"); q != "" {
		data["Query"] = q
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := RenderTemplate(w, "query.html", data); err != nil {
		http.Error(w, "Template error: "+err.Error(), http.StatusInternalServerError)
	}
}

// handleDeleteRow handles DELETE /tables/{name}/{pk} requests.
// It deletes a row from the specified table by primary key value.
// Used by HTMX with hx-delete and hx-confirm for user confirmation.
func (s *Server) handleDeleteRow(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "name")
	pkValue := chi.URLParam(r, "pk")

	// Check if executor is available
	if s.executor == nil {
		http.Error(w, "Database not available", http.StatusServiceUnavailable)
		return
	}

	// Get the table to check for primary key
	tbl, ok := s.executor.GetTable(tableName)
	if !ok {
		http.Error(w, "Table not found", http.StatusNotFound)
		return
	}

	// Check if table has a primary key
	if tbl.Schema.PrimaryKey < 0 {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`<div class="error">Table has no primary key - cannot delete specific rows</div>`))
		return
	}

	// Get the primary key column name
	pkCol := tbl.Schema.Columns[tbl.Schema.PrimaryKey].Name

	// Build DELETE SQL statement
	// Quote string values, leave numeric values unquoted
	var sql string
	if tbl.Schema.Columns[tbl.Schema.PrimaryKey].Type == parser.TypeText {
		sql = fmt.Sprintf("DELETE FROM %s WHERE %s = '%s'", tableName, pkCol, pkValue)
	} else {
		sql = fmt.Sprintf("DELETE FROM %s WHERE %s = %s", tableName, pkCol, pkValue)
	}

	// Parse and execute the SQL
	lex := lexer.New(sql)
	p := parser.New(lex)
	stmt, err := p.Parse()
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`<div class="error">Parse error: %s</div>`, err.Error())))
		return
	}

	result, err := s.executor.Execute(stmt)
	if err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`<div class="error">%s</div>`, err.Error())))
		return
	}

	// Check if any rows were affected
	if result.RowCount == 0 {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`<div class="error">Row not found - may have already been deleted</div>`))
		return
	}

	// Success - return empty response to remove row from DOM
	// HTMX will use hx-swap="outerHTML" to replace the row with this empty content
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("<!-- Row deleted -->"))
}
