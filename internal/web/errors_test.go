package web

import (
	"bytes"
	"strings"
	"testing"
)

func TestErrorTemplateBasic(t *testing.T) {
	data := map[string]interface{}{
		"Error": "no such table: users",
		"Query": "SELECT * FROM users",
	}

	var buf bytes.Buffer
	err := RenderTemplate(&buf, "error.html", data)
	if err != nil {
		t.Fatalf("Failed to render error template: %v", err)
	}

	html := buf.String()

	// Check error message appears
	if !strings.Contains(html, "no such table") {
		t.Error("expected error message in output")
	}

	// Check query appears
	if !strings.Contains(html, "SELECT * FROM users") {
		t.Error("expected query in output")
	}

	// Check role="alert" for accessibility
	if !strings.Contains(html, `role="alert"`) {
		t.Error("expected role=\"alert\" for accessibility")
	}
}

func TestErrorTemplateWithHint(t *testing.T) {
	data := map[string]interface{}{
		"Error": "no such table: users",
		"Query": "SELECT * FROM users",
		"Hint":  "Check table name spelling",
	}

	var buf bytes.Buffer
	err := RenderTemplate(&buf, "error.html", data)
	if err != nil {
		t.Fatalf("Failed to render error template: %v", err)
	}

	html := buf.String()

	if !strings.Contains(html, "Check table name") {
		t.Error("expected hint in output")
	}
}

func TestErrorTemplateEscapesHTML(t *testing.T) {
	data := map[string]interface{}{
		"Error": "<script>alert(1)</script>",
	}

	var buf bytes.Buffer
	err := RenderTemplate(&buf, "error.html", data)
	if err != nil {
		t.Fatalf("Failed to render error template: %v", err)
	}

	html := buf.String()

	// Should NOT contain unescaped script tag
	if strings.Contains(html, "<script>alert") {
		t.Error("expected HTML to be escaped, found unescaped script tag")
	}

	// Should contain escaped version
	if !strings.Contains(html, "&lt;script&gt;") {
		t.Error("expected escaped script tag in output")
	}
}

func TestErrorTemplateEmptyError(t *testing.T) {
	data := map[string]interface{}{
		"Error": "",
	}

	var buf bytes.Buffer
	err := RenderTemplate(&buf, "error.html", data)
	if err != nil {
		t.Fatalf("Failed to render error template: %v", err)
	}

	html := buf.String()

	// Should show generic error message
	if !strings.Contains(html, "An error occurred") {
		t.Error("expected generic error message for empty error")
	}
}

func TestGetErrorHint(t *testing.T) {
	tests := []struct {
		err      string
		contains string
		empty    bool
	}{
		{"no such table: foo", "SHOW TABLES", false},
		{"table not found: bar", "SHOW TABLES", false},
		{"no such column: bar", "DESCRIBE", false},
		{"column not found: baz", "DESCRIBE", false},
		{"syntax error near SELECT", "syntax", false},
		{"UNIQUE constraint failed", "already exists", false},
		{"duplicate key", "already exists", false},
		{"NOT NULL constraint failed", "requires a value", false},
		{"cannot be null", "requires a value", false},
		{"query timed out", "LIMIT", false},
		{"timeout exceeded", "LIMIT", false},
		{"random error", "", true},
		{"some other message", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.err, func(t *testing.T) {
			hint := GetErrorHint(tt.err)
			if tt.empty {
				if hint != "" {
					t.Errorf("expected empty hint for %q, got %q", tt.err, hint)
				}
			} else {
				if !strings.Contains(hint, tt.contains) {
					t.Errorf("expected hint for %q to contain %q, got %q", tt.err, tt.contains, hint)
				}
			}
		})
	}
}
