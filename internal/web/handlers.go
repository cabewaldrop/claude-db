package web

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
)

// ColumnInfo holds column information for template rendering.
type ColumnInfo struct {
	Name         string
	Type         string
	IsPrimaryKey bool
	NotNull      bool
}

// IndexInfo holds index information for template rendering.
type IndexInfo struct {
	Name    string
	Columns string
}

// TableDetail holds table information for template rendering.
type TableDetail struct {
	Name    string
	Columns []ColumnInfo
	Indexes []IndexInfo
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

// handleTableSchema shows the schema for a specific table.
func (s *Server) handleTableSchema(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "name")

	// Check if executor is available
	if s.executor == nil {
		http.Error(w, "Database not available", http.StatusServiceUnavailable)
		return
	}

	// Get table from executor
	tbl, ok := s.executor.GetTable(tableName)
	if !ok {
		http.Error(w, "Table not found: "+tableName, http.StatusNotFound)
		return
	}

	// Build table detail for template
	detail := TableDetail{
		Name: tbl.Name,
	}

	// Add columns
	for _, col := range tbl.Schema.Columns {
		detail.Columns = append(detail.Columns, ColumnInfo{
			Name:         col.Name,
			Type:         col.Type.String(),
			IsPrimaryKey: col.PrimaryKey,
			NotNull:      col.NotNull,
		})
	}

	// Add indexes
	for _, indexName := range tbl.ListIndexes() {
		idx, ok := tbl.GetIndex(indexName)
		if ok {
			detail.Indexes = append(detail.Indexes, IndexInfo{
				Name:    idx.Name,
				Columns: strings.Join(idx.Columns, ", "),
			})
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := RenderTemplate(w, "table.html", map[string]interface{}{
		"Table": detail,
	}); err != nil {
		http.Error(w, "Template error: "+err.Error(), http.StatusInternalServerError)
	}
}
