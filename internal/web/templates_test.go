package web

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/table"
)

func TestResultsTemplateBasic(t *testing.T) {
	data := TemplateData{
		Columns:  []string{"id", "name"},
		Rows:     [][]table.Value{
			{{Type: parser.TypeInteger, Integer: 1}, {Type: parser.TypeText, Text: "Alice"}},
			{{Type: parser.TypeInteger, Integer: 2}, {Type: parser.TypeText, Text: "Bob"}},
		},
		RowCount: 2,
		Duration: 5 * time.Millisecond,
	}

	var buf bytes.Buffer
	err := RenderTemplate(&buf, "results", data)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	html := buf.String()

	if !strings.Contains(html, "2 row(s)") {
		t.Errorf("Expected '2 row(s)' in output, got %s", html)
	}
	if !strings.Contains(html, "<th>id</th>") {
		t.Errorf("Expected '<th>id</th>' in output, got %s", html)
	}
	if !strings.Contains(html, "<th>name</th>") {
		t.Errorf("Expected '<th>name</th>' in output, got %s", html)
	}
	if !strings.Contains(html, "Alice") {
		t.Errorf("Expected 'Alice' in output, got %s", html)
	}
	if !strings.Contains(html, "Bob") {
		t.Errorf("Expected 'Bob' in output, got %s", html)
	}
}

func TestResultsTemplateEmpty(t *testing.T) {
	data := TemplateData{
		Columns:  []string{"id"},
		Rows:     [][]table.Value{},
		RowCount: 0,
	}

	var buf bytes.Buffer
	err := RenderTemplate(&buf, "results", data)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	html := buf.String()

	if !strings.Contains(html, "no rows") {
		t.Errorf("Expected 'no rows' in output for empty result, got %s", html)
	}
}

func TestResultsTemplateNull(t *testing.T) {
	data := TemplateData{
		Columns:  []string{"value"},
		Rows:     [][]table.Value{
			{{IsNull: true}},
		},
		RowCount: 1,
	}

	var buf bytes.Buffer
	err := RenderTemplate(&buf, "results", data)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	html := buf.String()

	if !strings.Contains(html, `class="null"`) {
		t.Errorf("Expected 'class=\"null\"' in output for NULL value, got %s", html)
	}
	if !strings.Contains(html, "NULL") {
		t.Errorf("Expected 'NULL' in output for NULL value, got %s", html)
	}
}

func TestResultsTemplateEscapesHTML(t *testing.T) {
	data := TemplateData{
		Columns:  []string{"data"},
		Rows:     [][]table.Value{
			{{Type: parser.TypeText, Text: "<script>alert(1)</script>"}},
		},
		RowCount: 1,
	}

	var buf bytes.Buffer
	err := RenderTemplate(&buf, "results", data)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	html := buf.String()

	if strings.Contains(html, "<script>alert") {
		t.Errorf("HTML should be escaped, but found unescaped script tag in %s", html)
	}
	if !strings.Contains(html, "&lt;script&gt;") {
		t.Errorf("Expected escaped script tag '&lt;script&gt;' in output, got %s", html)
	}
}

func TestResultsTemplateTruncatesLong(t *testing.T) {
	longValue := strings.Repeat("x", 200)
	data := TemplateData{
		Columns:  []string{"data"},
		Rows:     [][]table.Value{
			{{Type: parser.TypeText, Text: longValue}},
		},
		RowCount: 1,
	}

	var buf bytes.Buffer
	err := RenderTemplate(&buf, "results", data)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	html := buf.String()

	if !strings.Contains(html, "...") {
		t.Errorf("Expected '...' for truncated value, got %s", html)
	}
	// The output should not contain the full 200-character string
	if strings.Contains(html, longValue) {
		t.Errorf("Long value should be truncated, but found full value in output")
	}
}

func TestResultsTemplateMessage(t *testing.T) {
	data := TemplateData{
		Message:  "Table 'users' created",
		RowCount: 0,
		Duration: 10 * time.Millisecond,
	}

	var buf bytes.Buffer
	err := RenderTemplate(&buf, "results", data)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	html := buf.String()

	if !strings.Contains(html, "Table &#39;users&#39; created") {
		t.Errorf("Expected message in output, got %s", html)
	}
}

func TestErrorTemplate(t *testing.T) {
	data := TemplateData{
		Error: "table 'nonexistent' does not exist",
		Query: "SELECT * FROM nonexistent",
	}

	var buf bytes.Buffer
	err := RenderTemplate(&buf, "error", data)
	if err != nil {
		t.Fatalf("Failed to render template: %v", err)
	}

	html := buf.String()

	if !strings.Contains(html, "error") {
		t.Errorf("Expected 'error' class in output, got %s", html)
	}
	if !strings.Contains(html, "nonexistent") {
		t.Errorf("Expected error message in output, got %s", html)
	}
	if !strings.Contains(html, "SELECT * FROM nonexistent") {
		t.Errorf("Expected query in output, got %s", html)
	}
}

func TestRenderValueNil(t *testing.T) {
	result := renderValue(table.Value{IsNull: true})
	if !strings.Contains(string(result), "NULL") {
		t.Errorf("Expected 'NULL' for nil value, got %s", result)
	}
	if !strings.Contains(string(result), `class="null"`) {
		t.Errorf("Expected 'class=\"null\"' for nil value, got %s", result)
	}
}

func TestRenderValueTruncation(t *testing.T) {
	longText := strings.Repeat("a", 200)
	result := renderValue(table.Value{Type: parser.TypeText, Text: longText})
	resultStr := string(result)

	if !strings.Contains(resultStr, "...") {
		t.Errorf("Expected '...' for truncated value, got %s", resultStr)
	}
	if len(resultStr) >= 200 {
		t.Errorf("Expected truncated output, but length is %d", len(resultStr))
	}
}

func TestRenderValueHTMLEscape(t *testing.T) {
	result := renderValue(table.Value{Type: parser.TypeText, Text: "<b>bold</b>"})
	resultStr := string(result)

	if strings.Contains(resultStr, "<b>") {
		t.Errorf("HTML should be escaped, but found unescaped tags in %s", resultStr)
	}
	if !strings.Contains(resultStr, "&lt;b&gt;") {
		t.Errorf("Expected escaped tags, got %s", resultStr)
	}
}
