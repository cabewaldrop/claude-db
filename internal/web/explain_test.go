package web

import (
	"testing"

	"github.com/cabewaldrop/claude-db/internal/sql/executor"
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/table"
)

func makeTextValue(s string) table.Value {
	return table.Value{Type: parser.TypeText, Text: s}
}

func TestParseExplainOutput_TableScan(t *testing.T) {
	result := &executor.Result{
		Columns: []string{"Property", "Value"},
		Rows: [][]table.Value{
			{makeTextValue("Query Plan"), makeTextValue("SELECT on users using FULL_TABLE_SCAN")},
			{makeTextValue("Access Method"), makeTextValue("FULL_TABLE_SCAN")},
			{makeTextValue("Estimated Cost"), makeTextValue("100.00")},
		},
	}

	plan, err := ParseExplainOutput(result)
	if err != nil {
		t.Fatalf("ParseExplainOutput failed: %v", err)
	}

	if plan.Type != "FULL_TABLE_SCAN" {
		t.Errorf("expected type FULL_TABLE_SCAN, got %s", plan.Type)
	}
	if plan.Table != "users" {
		t.Errorf("expected table users, got %s", plan.Table)
	}
	if plan.Index != "" {
		t.Errorf("expected empty index, got %s", plan.Index)
	}
	if plan.EstimatedCost != 100.0 {
		t.Errorf("expected cost 100.0, got %f", plan.EstimatedCost)
	}
}

func TestParseExplainOutput_IndexLookup(t *testing.T) {
	result := &executor.Result{
		Columns: []string{"Property", "Value"},
		Rows: [][]table.Value{
			{makeTextValue("Query Plan"), makeTextValue("SELECT on users using pk")},
			{makeTextValue("Access Method"), makeTextValue("INDEX_LOOKUP")},
			{makeTextValue("Estimated Cost"), makeTextValue("1.00")},
			{makeTextValue("Predicates"), makeTextValue("1 condition(s)")},
			{makeTextValue("  [1]"), makeTextValue("id = 5 (indexed)")},
		},
	}

	plan, err := ParseExplainOutput(result)
	if err != nil {
		t.Fatalf("ParseExplainOutput failed: %v", err)
	}

	if plan.Type != "INDEX_LOOKUP" {
		t.Errorf("expected type INDEX_LOOKUP, got %s", plan.Type)
	}
	if plan.Table != "users" {
		t.Errorf("expected table users, got %s", plan.Table)
	}
	if plan.Index != "pk" {
		t.Errorf("expected index pk, got %s", plan.Index)
	}
	if plan.EstimatedCost != 1.0 {
		t.Errorf("expected cost 1.0, got %f", plan.EstimatedCost)
	}
	if len(plan.Predicates) != 1 {
		t.Errorf("expected 1 predicate, got %d", len(plan.Predicates))
	}
	if len(plan.Predicates) > 0 && plan.Predicates[0] != "id = 5 (indexed)" {
		t.Errorf("expected predicate 'id = 5 (indexed)', got %s", plan.Predicates[0])
	}
}

func TestParseExplainOutput_IndexRangeScan(t *testing.T) {
	result := &executor.Result{
		Columns: []string{"Property", "Value"},
		Rows: [][]table.Value{
			{makeTextValue("Query Plan"), makeTextValue("SELECT on orders using idx_date")},
			{makeTextValue("Access Method"), makeTextValue("INDEX_RANGE_SCAN")},
			{makeTextValue("Estimated Cost"), makeTextValue("50.00")},
			{makeTextValue("Predicates"), makeTextValue("2 condition(s)")},
			{makeTextValue("  [1]"), makeTextValue("date > '2024-01-01' (indexed)")},
			{makeTextValue("  [2]"), makeTextValue("date < '2024-12-31' (indexed)")},
		},
	}

	plan, err := ParseExplainOutput(result)
	if err != nil {
		t.Fatalf("ParseExplainOutput failed: %v", err)
	}

	if plan.Type != "INDEX_RANGE_SCAN" {
		t.Errorf("expected type INDEX_RANGE_SCAN, got %s", plan.Type)
	}
	if plan.Table != "orders" {
		t.Errorf("expected table orders, got %s", plan.Table)
	}
	if plan.Index != "idx_date" {
		t.Errorf("expected index idx_date, got %s", plan.Index)
	}
	if len(plan.Predicates) != 2 {
		t.Errorf("expected 2 predicates, got %d", len(plan.Predicates))
	}
}

func TestParseExplainOutput_EmptyResult(t *testing.T) {
	result := &executor.Result{
		Columns: []string{"Property", "Value"},
		Rows:    [][]table.Value{},
	}

	_, err := ParseExplainOutput(result)
	if err == nil {
		t.Error("expected error for empty result")
	}
}

func TestParseExplainOutput_NilResult(t *testing.T) {
	_, err := ParseExplainOutput(nil)
	if err == nil {
		t.Error("expected error for nil result")
	}
}

func TestQueryPlan_FormatPlanText(t *testing.T) {
	plan := &QueryPlan{
		Type:          "INDEX_LOOKUP",
		Table:         "users",
		Index:         "pk",
		EstimatedCost: 1.0,
		Predicates:    []string{"id = 5 (indexed)"},
	}

	text := plan.FormatPlanText()

	if text == "" {
		t.Error("expected non-empty text output")
	}

	// Check key elements are present
	mustContain := []string{
		"Query Plan",
		"Access Method: INDEX_LOOKUP",
		"Table: users",
		"Index: pk",
		"Estimated Cost: 1.00",
		"Predicates:",
		"id = 5 (indexed)",
	}

	for _, want := range mustContain {
		if !contains(text, want) {
			t.Errorf("text output missing %q", want)
		}
	}
}

func TestQueryPlan_FormatPlanHTML(t *testing.T) {
	plan := &QueryPlan{
		Type:          "FULL_TABLE_SCAN",
		Table:         "users",
		EstimatedCost: 100.0,
	}

	html := plan.FormatPlanHTML()

	if html == "" {
		t.Error("expected non-empty HTML output")
	}

	// Check key elements are present
	mustContain := []string{
		"query-plan",
		"FULL_TABLE_SCAN",
		"users",
		"100.00",
	}

	for _, want := range mustContain {
		if !contains(html, want) {
			t.Errorf("HTML output missing %q", want)
		}
	}
}

func TestExtractTableFromPlan(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"SELECT on users using FULL_TABLE_SCAN", "users"},
		{"SELECT on orders using pk", "orders"},
		{"SELECT on products", "products"},
		{"invalid plan", ""},
	}

	for _, tt := range tests {
		got := extractTableFromPlan(tt.input)
		if got != tt.want {
			t.Errorf("extractTableFromPlan(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestExtractIndexFromPlan(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"SELECT on users using pk", "pk"},
		{"SELECT on orders using idx_date", "idx_date"},
		{"SELECT on products using FULL_TABLE_SCAN", "FULL_TABLE_SCAN"},
		{"SELECT on products", ""},
		{"invalid plan", ""},
	}

	for _, tt := range tests {
		got := extractIndexFromPlan(tt.input)
		if got != tt.want {
			t.Errorf("extractIndexFromPlan(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
