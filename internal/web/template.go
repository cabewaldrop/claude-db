// Package web provides the HTTP server for the database web UI.
//
// This file handles HTML template loading and rendering.

package web

import (
	"embed"
	"html/template"
	"io"
	"net/http"
	"path/filepath"
)

//go:embed templates/*.html
var templatesFS embed.FS

// templateCache holds parsed templates keyed by content template name.
var templateCache map[string]*template.Template

func init() {
	templateCache = make(map[string]*template.Template)

	// Pre-parse all content templates with the layout
	entries, err := templatesFS.ReadDir("templates")
	if err != nil {
		panic("failed to read templates directory: " + err.Error())
	}

	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "layout.html" {
			continue
		}

		name := entry.Name()
		// Parse layout first, then content template so content blocks override
		tmpl := template.Must(template.ParseFS(templatesFS, "templates/layout.html", "templates/"+name))
		templateCache[name] = tmpl
	}
}

// renderTemplate renders a template with the given data.
// It combines the layout template with the named content template.
func renderTemplate(w http.ResponseWriter, name string, data interface{}) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	tmpl, ok := templateCache[name]
	if !ok {
		http.Error(w, "Template not found: "+name, http.StatusInternalServerError)
		return
	}

	// Execute the layout template (first one parsed)
	// The layout includes blocks that are overridden by the content template
	layoutName := filepath.Base("templates/layout.html")
	if err := tmpl.ExecuteTemplate(w, layoutName, data); err != nil {
		http.Error(w, "Template execution error: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

// renderTemplateToWriter renders a template to any io.Writer.
func renderTemplateToWriter(w io.Writer, name string, data interface{}) error {
	tmpl, ok := templateCache[name]
	if !ok {
		return nil
	}

	layoutName := filepath.Base("templates/layout.html")
	return tmpl.ExecuteTemplate(w, layoutName, data)
}
