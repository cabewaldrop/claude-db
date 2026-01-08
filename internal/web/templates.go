// Package web provides HTTP server and web UI functionality for claudedb.
package web

import (
	"embed"
	"fmt"
	"html/template"
	"io"
)

//go:embed templates/*
var templateFS embed.FS

var templates *template.Template

func init() {
	var err error
	templates, err = template.ParseFS(templateFS, "templates/*.html")
	if err != nil {
		panic(fmt.Sprintf("failed to parse templates: %v", err))
	}
}

// RenderTemplate renders the named template with the given data to the writer.
// It returns an error if the template is not found or rendering fails.
func RenderTemplate(w io.Writer, name string, data interface{}) error {
	if templates == nil {
		return fmt.Errorf("templates not initialized")
	}

	tmpl := templates.Lookup(name)
	if tmpl == nil {
		return fmt.Errorf("template %q not found", name)
	}

	return tmpl.Execute(w, data)
}

// MustRenderTemplate is like RenderTemplate but panics on error.
// Use only when template errors indicate programming bugs.
func MustRenderTemplate(w io.Writer, name string, data interface{}) {
	if err := RenderTemplate(w, name, data); err != nil {
		panic(fmt.Sprintf("template render failed: %v", err))
	}
}
