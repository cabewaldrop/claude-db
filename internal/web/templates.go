// Package web provides HTTP handlers and template rendering for the web UI.
package web

import (
	"bytes"
	"embed"
	"html/template"
	"io"
	"strings"
)

//go:embed templates/partials/*.html
var templateFS embed.FS

// QueryPlan represents plan data for template rendering.
type QueryPlan struct {
	Type          string
	Table         string
	Index         string
	EstimatedRows int
	Predicates    []string
}

// PlanData contains all data passed to the plan template.
type PlanData struct {
	Plan      *QueryPlan
	RawOutput string
}

// funcMap provides custom template functions.
var funcMap = template.FuncMap{
	"lower": strings.ToLower,
}

// Templates holds parsed templates.
type Templates struct {
	templates *template.Template
}

// NewTemplates creates a new Templates instance with embedded templates.
func NewTemplates() (*Templates, error) {
	tmpl, err := template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/partials/*.html")
	if err != nil {
		return nil, err
	}
	return &Templates{templates: tmpl}, nil
}

// Render renders a template to the writer.
func (t *Templates) Render(w io.Writer, name string, data interface{}) error {
	return t.templates.ExecuteTemplate(w, name, data)
}

// RenderToString renders a template and returns the result as a string.
func (t *Templates) RenderToString(name string, data interface{}) (string, error) {
	var buf bytes.Buffer
	err := t.Render(&buf, name, data)
	return buf.String(), err
}
