package web

import (
	"bytes"
	"strings"
	"testing"
)

func TestBaseTemplateRenders(t *testing.T) {
	var buf bytes.Buffer
	err := RenderTemplate(&buf, "base.html", nil)
	if err != nil {
		t.Fatalf("RenderTemplate failed: %v", err)
	}

	html := buf.String()

	// Check HTML5 doctype
	if !strings.Contains(html, "<!DOCTYPE html>") {
		t.Error("expected HTML5 doctype")
	}

	// Check HTMX script tag
	if !strings.Contains(html, "htmx.min.js") {
		t.Error("expected HTMX script tag")
	}

	// Check navigation
	if !strings.Contains(html, "<nav") {
		t.Error("expected nav element")
	}

	// Check navigation links
	if !strings.Contains(html, `href="/query"`) {
		t.Error("expected Query link in navigation")
	}
	if !strings.Contains(html, `href="/tables"`) {
		t.Error("expected Tables link in navigation")
	}

	// Check default title
	if !strings.Contains(html, "<title>claudedb</title>") {
		t.Error("expected default title 'claudedb'")
	}
}

func TestTemplateEscapesHTML(t *testing.T) {
	// Create a simple child template that uses data
	// Since base.html uses block "title", we test with a struct that provides Title
	data := struct {
		Title string
	}{
		Title: "<script>alert(1)</script>",
	}

	var buf bytes.Buffer
	err := RenderTemplate(&buf, "base.html", data)
	if err != nil {
		t.Fatalf("RenderTemplate failed: %v", err)
	}

	html := buf.String()

	// The malicious script should NOT appear unescaped
	if strings.Contains(html, "<script>alert") {
		t.Error("expected HTML to be escaped, found unescaped script tag")
	}
}

func TestRenderTemplateNotFound(t *testing.T) {
	var buf bytes.Buffer
	err := RenderTemplate(&buf, "nonexistent.html", nil)
	if err == nil {
		t.Error("expected error for nonexistent template")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error, got: %v", err)
	}
}

func TestRenderTemplateWithNilData(t *testing.T) {
	var buf bytes.Buffer
	err := RenderTemplate(&buf, "base.html", nil)
	if err != nil {
		t.Fatalf("RenderTemplate with nil data failed: %v", err)
	}

	// Should still render without error
	if buf.Len() == 0 {
		t.Error("expected non-empty output")
	}
}

func TestBaseTemplateHasMainContainer(t *testing.T) {
	var buf bytes.Buffer
	err := RenderTemplate(&buf, "base.html", nil)
	if err != nil {
		t.Fatalf("RenderTemplate failed: %v", err)
	}

	html := buf.String()

	// Check main container
	if !strings.Contains(html, `<main class="container">`) {
		t.Error("expected main element with container class")
	}

	// Check footer
	if !strings.Contains(html, "<footer>") {
		t.Error("expected footer element")
	}
}
