// Package web provides HTTP server and web UI functionality for claudedb.
//
// EDUCATIONAL NOTES:
// ------------------
// Go templates support inheritance through define/block/template actions:
// - base.html defines the structure with {{block "name" .}}default{{end}}
// - Child templates override blocks with {{define "name"}}content{{end}}
// - We parse base + child together and execute base to render the full page
package web

import (
	"embed"
	"fmt"
	"html/template"
	"io"
)

//go:embed templates/*
var templateFS embed.FS

// pageTemplates maps page names to their compiled templates.
// Each page template is parsed together with base.html to enable inheritance.
var pageTemplates = make(map[string]*template.Template)

func init() {
	// Get base template content
	baseContent, err := templateFS.ReadFile("templates/base.html")
	if err != nil {
		panic(fmt.Sprintf("failed to read base template: %v", err))
	}

	// Parse base template alone (for pages that just render base)
	baseTmpl, err := template.New("base.html").Parse(string(baseContent))
	if err != nil {
		panic(fmt.Sprintf("failed to parse base template: %v", err))
	}
	pageTemplates["base.html"] = baseTmpl

	// List of child templates that extend base
	childTemplates := []string{
		"table_new.html",
	}

	for _, name := range childTemplates {
		childContent, err := templateFS.ReadFile("templates/" + name)
		if err != nil {
			// Skip templates that don't exist yet
			continue
		}

		// Clone base and parse child template into it
		tmpl, err := template.Must(baseTmpl.Clone()).Parse(string(childContent))
		if err != nil {
			panic(fmt.Sprintf("failed to parse %s: %v", name, err))
		}
		pageTemplates[name] = tmpl
	}
}

// RenderTemplate renders the named template with the given data to the writer.
// For page templates (table_new.html, etc.), it renders base.html with blocks
// overridden by the page template.
func RenderTemplate(w io.Writer, name string, data interface{}) error {
	tmpl, ok := pageTemplates[name]
	if !ok {
		return fmt.Errorf("template %q not found", name)
	}

	// Execute base.html which will use the overridden blocks
	return tmpl.ExecuteTemplate(w, "base.html", data)
}

// MustRenderTemplate is like RenderTemplate but panics on error.
// Use only when template errors indicate programming bugs.
func MustRenderTemplate(w io.Writer, name string, data interface{}) {
	if err := RenderTemplate(w, name, data); err != nil {
		panic(fmt.Sprintf("template render failed: %v", err))
	}
}
