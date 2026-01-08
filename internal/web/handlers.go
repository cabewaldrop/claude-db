package web

import (
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/cabewaldrop/claude-db/internal/sql/lexer"
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
)

// parseSQL parses a SQL string and returns the statement.
func parseSQL(sql string) (parser.Statement, error) {
	l := lexer.New(sql)
	p := parser.New(l)
	return p.Parse()
}

//go:embed templates/*
var templateFS embed.FS

// EditColumn represents a column for the edit form.
type EditColumn struct {
	Name         string
	Type         string
	Value        string
	IsPrimaryKey bool
	NotNull      bool
}

// escapeSQL escapes single quotes in SQL strings to prevent SQL injection.
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// renderTemplate renders a template with the given data.
func renderTemplate(w http.ResponseWriter, name string, data interface{}) {
	tmpl, err := template.ParseFS(templateFS, "templates/"+name)
	if err != nil {
		http.Error(w, "Template error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, "Template execution error: "+err.Error(), http.StatusInternalServerError)
	}
}

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

// handleGetEditRow returns an HTML form for editing a row.
// GET /tables/{name}/edit/{pk}
func (s *Server) handleGetEditRow(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "name")
	pkValue := chi.URLParam(r, "pk")

	if s.executor == nil {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": "Database not available",
		})
		return
	}

	// Get the table
	tbl, ok := s.executor.GetTable(tableName)
	if !ok {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": "Table not found: " + tableName,
		})
		return
	}

	schema := tbl.Schema

	// Find primary key column
	pkColIdx := schema.PrimaryKey
	if pkColIdx < 0 {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": "Table has no primary key - cannot edit rows",
		})
		return
	}

	pkCol := schema.Columns[pkColIdx]

	// Build and execute SELECT query for the row
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = %s", tableName, pkCol.Name, pkValue)
	stmt, err := parseSQL(query)
	if err != nil {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": "Query parse error: " + err.Error(),
		})
		return
	}

	result, err := s.executor.Execute(stmt)
	if err != nil {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": "Query error: " + err.Error(),
		})
		return
	}

	if len(result.Rows) == 0 {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": "Row not found",
		})
		return
	}

	// Build column data with current values
	var columns []EditColumn
	for i, col := range schema.Columns {
		columns = append(columns, EditColumn{
			Name:         col.Name,
			Type:         col.Type.String(),
			Value:        result.Rows[0][i].String(),
			IsPrimaryKey: col.PrimaryKey,
			NotNull:      col.NotNull,
		})
	}

	renderTemplate(w, "partials/edit_row.html", map[string]interface{}{
		"Table":      tableName,
		"PrimaryKey": pkValue,
		"Columns":    columns,
	})
}

// handleUpdateRow updates a row in the database.
// PUT /tables/{name}/{pk}
func (s *Server) handleUpdateRow(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "name")
	pkValue := chi.URLParam(r, "pk")

	if s.executor == nil {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": "Database not available",
		})
		return
	}

	// Parse form data
	if err := r.ParseForm(); err != nil {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": "Invalid form data: " + err.Error(),
		})
		return
	}

	// Get the table
	tbl, ok := s.executor.GetTable(tableName)
	if !ok {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": "Table not found: " + tableName,
		})
		return
	}

	schema := tbl.Schema

	// Find primary key column
	pkColIdx := schema.PrimaryKey
	if pkColIdx < 0 {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": "Table has no primary key",
		})
		return
	}

	pkCol := schema.Columns[pkColIdx]

	// Build SET clause
	var sets []string
	for _, col := range schema.Columns {
		if col.PrimaryKey {
			continue // Don't update PK
		}
		value := r.FormValue(col.Name)
		if col.Type == parser.TypeText {
			sets = append(sets, fmt.Sprintf("%s = '%s'", col.Name, escapeSQL(value)))
		} else if value == "" || value == "NULL" {
			sets = append(sets, fmt.Sprintf("%s = NULL", col.Name))
		} else {
			sets = append(sets, fmt.Sprintf("%s = %s", col.Name, value))
		}
	}

	if len(sets) == 0 {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": "No columns to update",
		})
		return
	}

	sqlStmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s = %s",
		tableName, strings.Join(sets, ", "), pkCol.Name, pkValue)

	stmt, err := parseSQL(sqlStmt)
	if err != nil {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": "SQL error: " + err.Error(),
		})
		return
	}

	_, err = s.executor.Execute(stmt)
	if err != nil {
		renderTemplate(w, "partials/error.html", map[string]string{
			"Error": err.Error(),
		})
		return
	}

	renderTemplate(w, "partials/success.html", map[string]string{
		"Message": "Row updated successfully",
	})
}
