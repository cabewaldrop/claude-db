package web

import (
	"fmt"
	"html/template"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

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

// tableDataPage holds data for rendering the table data template.
type tableDataPage struct {
	TableName string
	Columns   []string
	Rows      [][]string
	Limit     int
	Offset    int
	OffsetEnd int
	HasPrev   bool
	HasNext   bool
	PrevURL   string
	NextURL   string
	Empty     bool
	Error     string
}

// tableDataTemplate is the HTML template for table data display.
var tableDataTemplate = template.Must(template.New("tableData").Parse(`<!DOCTYPE html>
<html>
<head>
    <title>{{.TableName}} - ClaudeDB</title>
    <style>
        body { font-family: system-ui, sans-serif; margin: 20px; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f4f4f4; }
        tr:nth-child(even) { background-color: #fafafa; }
        .nav { margin: 10px 0; }
        .nav a { margin-right: 10px; padding: 5px 10px; background: #007bff; color: white; text-decoration: none; border-radius: 3px; }
        .nav a.disabled { background: #ccc; pointer-events: none; }
        .empty { color: #666; font-style: italic; }
        .error { color: red; }
        h1 a { color: inherit; text-decoration: none; }
    </style>
</head>
<body>
    <h1><a href="/">ClaudeDB</a> / {{.TableName}}</h1>
    {{if .Error}}
        <p class="error">{{.Error}}</p>
    {{else if .Empty}}
        <p class="empty">This table is empty.</p>
    {{else}}
        <div class="nav">
            {{if .HasPrev}}<a href="{{.PrevURL}}">← Previous</a>{{else}}<a class="disabled">← Previous</a>{{end}}
            {{if .HasNext}}<a href="{{.NextURL}}">Next →</a>{{else}}<a class="disabled">Next →</a>{{end}}
            <span>Showing rows {{.Offset}} - {{.OffsetEnd}} (limit {{.Limit}})</span>
        </div>
        <table>
            <thead>
                <tr>{{range .Columns}}<th>{{.}}</th>{{end}}</tr>
            </thead>
            <tbody>
                {{range .Rows}}<tr>{{range .}}<td>{{.}}</td>{{end}}</tr>{{end}}
            </tbody>
        </table>
        <div class="nav">
            {{if .HasPrev}}<a href="{{.PrevURL}}">← Previous</a>{{else}}<a class="disabled">← Previous</a>{{end}}
            {{if .HasNext}}<a href="{{.NextURL}}">Next →</a>{{else}}<a class="disabled">Next →</a>{{end}}
        </div>
    {{end}}
</body>
</html>`))

// handleTableData serves paginated table data.
func (s *Server) handleTableData(w http.ResponseWriter, r *http.Request) {
	tableName := chi.URLParam(r, "name")

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

	page := tableDataPage{
		TableName: tableName,
		Limit:     limit,
		Offset:    offset,
	}

	// Check if executor is available
	if s.executor == nil {
		page.Error = "Database not initialized"
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		tableDataTemplate.Execute(w, page)
		return
	}

	// Check if table exists
	if _, ok := s.executor.GetTable(tableName); !ok {
		page.Error = fmt.Sprintf("Table '%s' not found", tableName)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		tableDataTemplate.Execute(w, page)
		return
	}

	// Query table data with limit+1 to check for more rows
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", tableName, limit+1, offset)
	lex := lexer.New(query)
	p := parser.New(lex)
	stmt, err := p.Parse()
	if err != nil {
		page.Error = fmt.Sprintf("Query error: %v", err)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		tableDataTemplate.Execute(w, page)
		return
	}

	result, err := s.executor.Execute(stmt)
	if err != nil {
		page.Error = fmt.Sprintf("Query error: %v", err)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		tableDataTemplate.Execute(w, page)
		return
	}

	page.Columns = result.Columns

	// Check if there are more rows
	hasMore := len(result.Rows) > limit
	if hasMore {
		result.Rows = result.Rows[:limit]
	}

	// Convert values to strings for template
	for _, row := range result.Rows {
		strRow := make([]string, len(row))
		for i, val := range row {
			strRow[i] = val.String()
		}
		page.Rows = append(page.Rows, strRow)
	}

	page.Empty = len(page.Rows) == 0
	page.OffsetEnd = offset + len(page.Rows)
	page.HasPrev = offset > 0
	page.HasNext = hasMore

	if page.HasPrev {
		prevOffset := offset - limit
		if prevOffset < 0 {
			prevOffset = 0
		}
		page.PrevURL = fmt.Sprintf("/tables/%s/data?limit=%d&offset=%d", tableName, limit, prevOffset)
	}
	if page.HasNext {
		page.NextURL = fmt.Sprintf("/tables/%s/data?limit=%d&offset=%d", tableName, limit, offset+limit)
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	tableDataTemplate.Execute(w, page)
}
