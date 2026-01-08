// Package web - HTTP request handlers for the claudedb web UI.
//
// EDUCATIONAL NOTES:
// ------------------
// Handlers are the core of HTTP server logic. Each handler:
// 1. Reads request data (path params, query params, form data, JSON body)
// 2. Validates input and handles errors gracefully
// 3. Performs business logic (often by calling other packages)
// 4. Renders a response (HTML template, JSON, redirect, etc.)
//
// Handler patterns in this file:
// - Form handling with GET (show form) and POST (process form)
// - HTMX partial responses for dynamic updates
// - Redirect after successful POST (Post/Redirect/Get pattern)

package web

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/cabewaldrop/claude-db/internal/sql/lexer"
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
)

// handleIndex serves the home page.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if err := RenderTemplate(w, "base.html", nil); err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
	}
}

// handleHealth returns a simple health check response.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("OK"))
}

// handleCreateTableForm shows the create table form (GET /tables/new).
func (s *Server) handleCreateTableForm(w http.ResponseWriter, r *http.Request) {
	if err := RenderTemplate(w, "table_new.html", nil); err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
	}
}

// handleCreateTable processes the create table form (POST /tables).
//
// EDUCATIONAL NOTES:
// ------------------
// This handler demonstrates form processing:
// 1. Extract form values from POST data
// 2. Validate all input before executing SQL
// 3. Build a SQL statement programmatically
// 4. Execute and handle errors
// 5. Redirect on success (Post/Redirect/Get pattern)
func (s *Server) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		renderError(w, "Failed to parse form", "")
		return
	}

	tableName := strings.TrimSpace(r.FormValue("table_name"))

	// Validate table name
	if !IsValidIdentifier(tableName) {
		renderError(w, "Invalid table name. Use letters, numbers, underscores. Must start with letter or underscore.", "")
		return
	}

	// Get column definitions from form arrays
	colNames := r.Form["col_name[]"]
	colTypes := r.Form["col_type[]"]
	colPKs := r.Form["col_pk[]"]
	colNotNulls := r.Form["col_notnull[]"]

	if len(colNames) == 0 {
		renderError(w, "At least one column is required.", "")
		return
	}

	// Build column definitions
	var columns []string
	pkIndex := -1

	for i, name := range colNames {
		name = strings.TrimSpace(name)
		if name == "" {
			continue // Skip empty column names
		}

		if !IsValidIdentifier(name) {
			renderError(w, fmt.Sprintf("Invalid column name: %s", name), "")
			return
		}

		// Get type, default to TEXT if not specified
		colType := "TEXT"
		if i < len(colTypes) && colTypes[i] != "" {
			colType = colTypes[i]
			if !IsValidType(colType) {
				renderError(w, fmt.Sprintf("Invalid column type: %s", colType), "")
				return
			}
		}

		col := fmt.Sprintf("%s %s", name, colType)

		// Check if this column is marked as primary key
		if containsIndex(colPKs, i) {
			if pkIndex >= 0 {
				renderError(w, "Only one column can be the primary key.", "")
				return
			}
			col += " PRIMARY KEY"
			pkIndex = i
		}

		// Check if this column is marked as NOT NULL
		if containsIndex(colNotNulls, i) {
			col += " NOT NULL"
		}

		columns = append(columns, col)
	}

	if len(columns) == 0 {
		renderError(w, "At least one column is required.", "")
		return
	}

	// Build CREATE TABLE SQL
	sql := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(columns, ", "))

	// Get executor from context
	exec := GetExecutor(r)
	if exec == nil {
		renderError(w, "Database not available", sql)
		return
	}

	// Parse and execute the SQL
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		renderError(w, fmt.Sprintf("SQL parse error: %v", err), sql)
		return
	}

	_, err = exec.Execute(stmt)
	if err != nil {
		renderError(w, err.Error(), sql)
		return
	}

	// Success - redirect to the new table page
	// For HTMX requests, use HX-Redirect header
	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("HX-Redirect", "/tables/"+tableName)
		w.WriteHeader(http.StatusOK)
		return
	}

	// For regular form submission, use HTTP redirect
	http.Redirect(w, r, "/tables/"+tableName, http.StatusSeeOther)
}

// renderError renders an error message for form submissions.
// If sql is provided, it's shown for debugging purposes.
func renderError(w http.ResponseWriter, message, sql string) {
	data := struct {
		Error string
		Query string
	}{
		Error: message,
		Query: sql,
	}

	w.WriteHeader(http.StatusBadRequest)
	// Try to render error template, fall back to plain text
	if err := RenderTemplate(w, "error.html", data); err != nil {
		// Template might not exist yet, use plain HTML
		w.Header().Set("Content-Type", "text/html")
		html := fmt.Sprintf(`<div class="error">
			<p><strong>Error:</strong> %s</p>`, data.Error)
		if data.Query != "" {
			html += fmt.Sprintf(`<pre>%s</pre>`, data.Query)
		}
		html += `</div>`
		w.Write([]byte(html))
	}
}

// containsIndex checks if a string slice contains the string representation of an index.
func containsIndex(slice []string, idx int) bool {
	idxStr := fmt.Sprintf("%d", idx)
	for _, s := range slice {
		if s == idxStr {
			return true
		}
	}
	return false
}
