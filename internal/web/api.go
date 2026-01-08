// Package web provides the HTTP server for the database web UI.
//
// This file contains the JSON API endpoints for programmatic access.

package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/cabewaldrop/claude-db/internal/sql/lexer"
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/table"
)

// ============================================================================
// API Response Types
// ============================================================================

// APIResponse wraps all API responses with success/error info.
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// TableListResponse contains the list of tables.
type TableListResponse struct {
	Tables []string `json:"tables"`
}

// ColumnInfo describes a single column in a table.
type ColumnInfo struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	PrimaryKey bool   `json:"primary_key"`
	NotNull    bool   `json:"not_null"`
}

// TableSchemaResponse describes a table's structure.
type TableSchemaResponse struct {
	Name       string       `json:"name"`
	Columns    []ColumnInfo `json:"columns"`
	PrimaryKey string       `json:"primary_key,omitempty"`
	RowCount   int64        `json:"row_count"`
}

// RowsResponse contains paginated row data.
type RowsResponse struct {
	Columns    []string        `json:"columns"`
	Rows       []RowData       `json:"rows"`
	TotalCount int64           `json:"total_count"`
	Offset     int             `json:"offset"`
	Limit      int             `json:"limit"`
	HasMore    bool            `json:"has_more"`
}

// RowData represents a single row with its ID and values.
type RowData struct {
	ID     uint64                 `json:"_id"`
	Values map[string]interface{} `json:"values"`
}

// QueryRequest is the body for query execution.
type QueryRequest struct {
	SQL string `json:"sql"`
}

// QueryResponse contains query results.
type QueryResponse struct {
	Columns  []string        `json:"columns,omitempty"`
	Rows     [][]interface{} `json:"rows,omitempty"`
	RowCount int             `json:"row_count"`
	Message  string          `json:"message,omitempty"`
}

// ============================================================================
// Helper Functions
// ============================================================================

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// writeSuccess writes a successful API response.
func writeSuccess(w http.ResponseWriter, data interface{}) {
	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    data,
	})
}

// writeError writes an error API response.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, APIResponse{
		Success: false,
		Error:   message,
	})
}

// valueToInterface converts a table.Value to a JSON-serializable interface{}.
func valueToInterface(v table.Value) interface{} {
	if v.IsNull {
		return nil
	}
	switch v.Type {
	case parser.TypeInteger:
		return v.Integer
	case parser.TypeReal:
		return v.Real
	case parser.TypeText:
		return v.Text
	case parser.TypeBoolean:
		return v.Boolean
	default:
		return v.String()
	}
}

// dataTypeToString converts a parser.DataType to a human-readable string.
func dataTypeToString(dt parser.DataType) string {
	switch dt {
	case parser.TypeInteger:
		return "INTEGER"
	case parser.TypeReal:
		return "REAL"
	case parser.TypeText:
		return "TEXT"
	case parser.TypeBoolean:
		return "BOOLEAN"
	default:
		return "UNKNOWN"
	}
}

// ============================================================================
// API Handlers
// ============================================================================

// handleAPITables returns a list of all tables.
// GET /api/tables
func (s *Server) handleAPITables(w http.ResponseWriter, r *http.Request) {
	if s.executor == nil {
		writeError(w, http.StatusServiceUnavailable, "database not initialized")
		return
	}

	tables := s.executor.GetTables()
	writeSuccess(w, TableListResponse{Tables: tables})
}

// handleAPITableSchema returns the schema for a specific table.
// GET /api/tables/{name}
func (s *Server) handleAPITableSchema(w http.ResponseWriter, r *http.Request) {
	if s.executor == nil {
		writeError(w, http.StatusServiceUnavailable, "database not initialized")
		return
	}

	tableName := chi.URLParam(r, "name")
	tbl, exists := s.executor.GetTable(tableName)
	if !exists {
		writeError(w, http.StatusNotFound, fmt.Sprintf("table '%s' not found", tableName))
		return
	}

	// Build column info
	columns := make([]ColumnInfo, len(tbl.Schema.Columns))
	var pkName string
	for i, col := range tbl.Schema.Columns {
		columns[i] = ColumnInfo{
			Name:       col.Name,
			Type:       dataTypeToString(col.Type),
			PrimaryKey: col.PrimaryKey,
			NotNull:    col.NotNull,
		}
		if col.PrimaryKey {
			pkName = col.Name
		}
	}

	// Get row count from stats
	stats := tbl.Stats()

	writeSuccess(w, TableSchemaResponse{
		Name:       tableName,
		Columns:    columns,
		PrimaryKey: pkName,
		RowCount:   stats.RowCount,
	})
}

// handleAPITableRows returns paginated rows from a table.
// GET /api/tables/{name}/rows?limit=50&offset=0
func (s *Server) handleAPITableRows(w http.ResponseWriter, r *http.Request) {
	if s.executor == nil {
		writeError(w, http.StatusServiceUnavailable, "database not initialized")
		return
	}

	tableName := chi.URLParam(r, "name")
	tbl, exists := s.executor.GetTable(tableName)
	if !exists {
		writeError(w, http.StatusNotFound, fmt.Sprintf("table '%s' not found", tableName))
		return
	}

	// Parse pagination params
	limit := 50
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 1000 {
			limit = parsed
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	// Get all rows (we'll implement proper pagination later with LIMIT/OFFSET in executor)
	allRows, err := tbl.Scan()
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("scan failed: %v", err))
		return
	}

	// Manual pagination
	totalCount := int64(len(allRows))
	start := offset
	end := offset + limit
	if start > len(allRows) {
		start = len(allRows)
	}
	if end > len(allRows) {
		end = len(allRows)
	}
	pageRows := allRows[start:end]

	// Build column names
	colNames := make([]string, len(tbl.Schema.Columns))
	for i, col := range tbl.Schema.Columns {
		colNames[i] = col.Name
	}

	// Convert rows to response format
	rows := make([]RowData, len(pageRows))
	for i, row := range pageRows {
		values := make(map[string]interface{})
		for j, val := range row.Values {
			if j < len(colNames) {
				values[colNames[j]] = valueToInterface(val)
			}
		}
		rows[i] = RowData{
			ID:     row.ID,
			Values: values,
		}
	}

	writeSuccess(w, RowsResponse{
		Columns:    colNames,
		Rows:       rows,
		TotalCount: totalCount,
		Offset:     offset,
		Limit:      limit,
		HasMore:    int64(end) < totalCount,
	})
}

// handleAPIQuery executes an arbitrary SQL query.
// POST /api/query
func (s *Server) handleAPIQuery(w http.ResponseWriter, r *http.Request) {
	if s.executor == nil {
		writeError(w, http.StatusServiceUnavailable, "database not initialized")
		return
	}

	// Parse request body
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.SQL == "" {
		writeError(w, http.StatusBadRequest, "sql field is required")
		return
	}

	// Parse SQL
	l := lexer.New(req.SQL)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("parse error: %v", err))
		return
	}

	// Execute
	result, err := s.executor.Execute(stmt)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("execution error: %v", err))
		return
	}

	// Convert result to response
	resp := QueryResponse{
		RowCount: result.RowCount,
		Message:  result.Message,
	}

	if len(result.Columns) > 0 {
		resp.Columns = result.Columns
		resp.Rows = make([][]interface{}, len(result.Rows))
		for i, row := range result.Rows {
			resp.Rows[i] = make([]interface{}, len(row))
			for j, val := range row {
				resp.Rows[i][j] = valueToInterface(val)
			}
		}
	}

	writeSuccess(w, resp)
}
