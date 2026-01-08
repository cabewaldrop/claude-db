// Package web - Template rendering
//
// EDUCATIONAL NOTES:
// ------------------
// Go's html/template package provides automatic HTML escaping to prevent XSS attacks.
// Template functions extend the built-in functionality:
//
// 1. renderValue: Handles NULL values, long strings, and type formatting
// 2. gt: Greater-than comparison for conditional logic
//
// Templates are parsed once at startup and cached for efficiency.

package web

import (
	"embed"
	"fmt"
	"html/template"
	"io"
	"time"

	"github.com/cabewaldrop/claude-db/internal/table"
)

//go:embed templates/*.html templates/partials/*.html
var templateFS embed.FS

// Templates holds the parsed templates for the web UI.
var Templates *template.Template

// TemplateData holds common data for template rendering.
type TemplateData struct {
	Columns  []string
	Rows     [][]table.Value
	RowCount int
	Duration time.Duration
	Message  string
	Error    string
	Query    string
}

// init parses all templates at startup.
func init() {
	funcMap := template.FuncMap{
		"renderValue": renderValue,
		"gt":          func(a, b int) bool { return a > b },
	}

	Templates = template.Must(template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/*.html", "templates/partials/*.html"))
}

// RenderTemplate renders a template to the given writer.
func RenderTemplate(w io.Writer, name string, data interface{}) error {
	return Templates.ExecuteTemplate(w, name, data)
}

// renderValue converts a table.Value to HTML with proper formatting.
//
// EDUCATIONAL NOTE:
// -----------------
// This function handles the various edge cases when displaying database values:
// - NULL values get a styled indicator
// - Long strings are truncated for display
// - HTML characters are escaped to prevent XSS
// - Different types are formatted appropriately
func renderValue(v table.Value) template.HTML {
	// Handle NULL
	if v.IsNull {
		return template.HTML(`<span class="null">NULL</span>`)
	}

	// Get string representation
	str := v.String()

	// Truncate very long values
	const maxLen = 100
	if len(str) > maxLen {
		str = str[:maxLen] + "..."
	}

	// Escape HTML and return
	return template.HTML(template.HTMLEscapeString(str))
}

// renderValueInterface handles interface{} values for backward compatibility.
func renderValueInterface(v interface{}) template.HTML {
	if v == nil {
		return template.HTML(`<span class="null">NULL</span>`)
	}

	// Handle table.Value directly
	if tv, ok := v.(table.Value); ok {
		return renderValue(tv)
	}

	// Format other types
	str := fmt.Sprintf("%v", v)

	// Truncate very long values
	const maxLen = 100
	if len(str) > maxLen {
		str = str[:maxLen] + "..."
	}

	// Escape HTML and return
	return template.HTML(template.HTMLEscapeString(str))
}
