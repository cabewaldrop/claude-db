package web

import (
	"bytes"
	"strings"
	"testing"
)

func TestPlanTemplateRenders(t *testing.T) {
	tmpl, err := NewTemplates()
	if err != nil {
		t.Fatalf("Failed to create templates: %v", err)
	}

	plan := &QueryPlan{
		Type:          "IndexScan",
		Table:         "users",
		Index:         "idx_email",
		EstimatedRows: 1,
	}

	var buf bytes.Buffer
	err = tmpl.Render(&buf, "plan.html", PlanData{
		Plan:      plan,
		RawOutput: "Plan: IndexScan on users using idx_email",
	})
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "IndexScan") {
		t.Error("expected 'IndexScan' in output")
	}
	if !strings.Contains(html, "idx_email") {
		t.Error("expected 'idx_email' in output")
	}
	if !strings.Contains(html, "indexscan") {
		t.Error("expected 'indexscan' CSS class in output")
	}
	if !strings.Contains(html, "users") {
		t.Error("expected 'users' table name in output")
	}
}

func TestPlanTemplateTableScan(t *testing.T) {
	tmpl, err := NewTemplates()
	if err != nil {
		t.Fatalf("Failed to create templates: %v", err)
	}

	plan := &QueryPlan{
		Type:  "TableScan",
		Table: "users",
	}

	var buf bytes.Buffer
	err = tmpl.Render(&buf, "plan.html", PlanData{
		Plan: plan,
	})
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "TableScan") {
		t.Error("expected 'TableScan' in output")
	}
	if !strings.Contains(html, "tablescan") {
		t.Error("expected 'tablescan' CSS class (warning style) in output")
	}
	if strings.Contains(html, "Index:") {
		t.Error("should not show Index field when not set")
	}
}

func TestPlanTemplateFallback(t *testing.T) {
	tmpl, err := NewTemplates()
	if err != nil {
		t.Fatalf("Failed to create templates: %v", err)
	}

	// When plan is nil, show raw output only
	var buf bytes.Buffer
	err = tmpl.Render(&buf, "plan.html", PlanData{
		Plan:      nil,
		RawOutput: "Unparseable plan output here",
	})
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "Unparseable plan") {
		t.Error("expected raw output to be shown when plan is nil")
	}
}

func TestPlanTemplateWithPredicates(t *testing.T) {
	tmpl, err := NewTemplates()
	if err != nil {
		t.Fatalf("Failed to create templates: %v", err)
	}

	plan := &QueryPlan{
		Type:       "IndexRangeScan",
		Table:      "orders",
		Index:      "idx_date",
		Predicates: []string{"date >= '2024-01-01'", "date < '2024-02-01'"},
	}

	var buf bytes.Buffer
	err = tmpl.Render(&buf, "plan.html", PlanData{
		Plan: plan,
	})
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "indexrangescan") {
		t.Error("expected 'indexrangescan' CSS class in output")
	}
	// HTML escapes >= to &gt;= and < to &lt;
	if !strings.Contains(html, "date &gt;= &#39;2024-01-01&#39;") {
		t.Errorf("expected first predicate in output, got: %s", html)
	}
	if !strings.Contains(html, "date &lt; &#39;2024-02-01&#39;") {
		t.Errorf("expected second predicate in output, got: %s", html)
	}
}

func TestPlanTemplateEstimatedRows(t *testing.T) {
	tmpl, err := NewTemplates()
	if err != nil {
		t.Fatalf("Failed to create templates: %v", err)
	}

	// With estimated rows
	plan := &QueryPlan{
		Type:          "IndexScan",
		Table:         "users",
		EstimatedRows: 42,
	}

	var buf bytes.Buffer
	err = tmpl.Render(&buf, "plan.html", PlanData{Plan: plan})
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}

	html := buf.String()
	if !strings.Contains(html, "Est. Rows:") {
		t.Error("expected estimated rows label")
	}
	if !strings.Contains(html, "42") {
		t.Error("expected estimated rows value")
	}

	// Without estimated rows (0 value)
	plan2 := &QueryPlan{
		Type:  "TableScan",
		Table: "logs",
	}

	buf.Reset()
	err = tmpl.Render(&buf, "plan.html", PlanData{Plan: plan2})
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}

	html = buf.String()
	if strings.Contains(html, "Est. Rows:") {
		t.Error("should not show Est. Rows when value is 0")
	}
}
