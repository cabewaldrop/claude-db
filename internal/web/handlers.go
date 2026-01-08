package web

import (
	"net/http"
)

// TableInfo represents metadata about a table for display.
type TableInfo struct {
	Name     string
	RowCount int
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
    <p><a href="/tables">View Tables</a></p>
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

// handleTableList renders a list of all tables in the database.
//
// EDUCATIONAL NOTE:
// -----------------
// This handler demonstrates a common pattern: gathering data from
// the database layer and passing it to a template for rendering.
// Error handling is graceful - if we can't get a row count for a
// table, we show "?" rather than failing the entire page.
func (s *Server) handleTableList(w http.ResponseWriter, r *http.Request) {
	// Check if executor is available
	if s.executor == nil {
		http.Error(w, "Database not available", http.StatusServiceUnavailable)
		return
	}

	// Get list of tables from the executor
	tableNames := s.executor.GetTables()

	// Build table info with row counts
	var tables []TableInfo
	for _, name := range tableNames {
		info := TableInfo{Name: name, RowCount: 0}

		// Try to get the row count - if it fails, we'll just show 0
		if tbl, ok := s.executor.GetTable(name); ok {
			stats := tbl.Stats()
			info.RowCount = int(stats.RowCount)
		}

		tables = append(tables, info)
	}

	// Render the template
	renderTemplate(w, "tables.html", map[string]interface{}{
		"Tables": tables,
	})
}
