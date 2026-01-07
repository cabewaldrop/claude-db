package planner

import (
	"testing"

	"github.com/cabewaldrop/claude-db/internal/sql/lexer"
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/table"
)

// Helper to create a test schema with primary key
func testSchema() *table.Schema {
	return table.NewSchema([]parser.ColumnDefinition{
		{Name: "id", Type: parser.TypeInteger, PrimaryKey: true},
		{Name: "name", Type: parser.TypeText},
		{Name: "age", Type: parser.TypeInteger},
	})
}

// Helper to parse a WHERE clause
func parseWhere(t *testing.T, sql string) parser.Expression {
	t.Helper()
	// Parse a full SELECT to get the WHERE clause
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("failed to parse %q: %v", sql, err)
	}
	sel, ok := stmt.(*parser.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}
	return sel.Where
}

func TestPlanSelect_NoWhere(t *testing.T) {
	planner := New()
	schema := testSchema()

	stmt := &parser.SelectStatement{
		Columns: []parser.Expression{&parser.StarExpression{}},
		From:    "users",
		Where:   nil,
	}

	plan := planner.PlanSelect(stmt, schema)

	if plan.AccessMethod != FullTableScan {
		t.Errorf("expected FullTableScan, got %v", plan.AccessMethod)
	}
	if len(plan.Predicates) != 0 {
		t.Errorf("expected 0 predicates, got %d", len(plan.Predicates))
	}
}

func TestPlanSelect_PKEquality(t *testing.T) {
	planner := New()
	schema := testSchema()

	where := parseWhere(t, "SELECT * FROM users WHERE id = 5")
	stmt := &parser.SelectStatement{
		Columns: []parser.Expression{&parser.StarExpression{}},
		From:    "users",
		Where:   where,
	}

	plan := planner.PlanSelect(stmt, schema)

	if plan.AccessMethod != IndexLookup {
		t.Errorf("expected IndexLookup, got %v", plan.AccessMethod)
	}
	if plan.IndexColumn != "id" {
		t.Errorf("expected index on 'id', got %q", plan.IndexColumn)
	}
	if plan.IndexLookupKey != int64(5) {
		t.Errorf("expected lookup key 5, got %v", plan.IndexLookupKey)
	}
}

func TestPlanSelect_PKRangeGreater(t *testing.T) {
	planner := New()
	schema := testSchema()

	where := parseWhere(t, "SELECT * FROM users WHERE id > 10")
	stmt := &parser.SelectStatement{
		Columns: []parser.Expression{&parser.StarExpression{}},
		From:    "users",
		Where:   where,
	}

	plan := planner.PlanSelect(stmt, schema)

	if plan.AccessMethod != IndexRangeScan {
		t.Errorf("expected IndexRangeScan, got %v", plan.AccessMethod)
	}
	if plan.IndexColumn != "id" {
		t.Errorf("expected index on 'id', got %q", plan.IndexColumn)
	}
	if plan.RangeLower != int64(10) {
		t.Errorf("expected lower bound 10, got %v", plan.RangeLower)
	}
	if plan.LowerInclusive {
		t.Error("expected lower bound to be exclusive (>)")
	}
	if plan.RangeUpper != nil {
		t.Errorf("expected no upper bound, got %v", plan.RangeUpper)
	}
}

func TestPlanSelect_PKRangeLess(t *testing.T) {
	planner := New()
	schema := testSchema()

	where := parseWhere(t, "SELECT * FROM users WHERE id <= 100")
	stmt := &parser.SelectStatement{
		Columns: []parser.Expression{&parser.StarExpression{}},
		From:    "users",
		Where:   where,
	}

	plan := planner.PlanSelect(stmt, schema)

	if plan.AccessMethod != IndexRangeScan {
		t.Errorf("expected IndexRangeScan, got %v", plan.AccessMethod)
	}
	if plan.RangeUpper != int64(100) {
		t.Errorf("expected upper bound 100, got %v", plan.RangeUpper)
	}
	if !plan.UpperInclusive {
		t.Error("expected upper bound to be inclusive (<=)")
	}
}

func TestPlanSelect_PKRangeBoth(t *testing.T) {
	planner := New()
	schema := testSchema()

	where := parseWhere(t, "SELECT * FROM users WHERE id >= 5 AND id < 20")
	stmt := &parser.SelectStatement{
		Columns: []parser.Expression{&parser.StarExpression{}},
		From:    "users",
		Where:   where,
	}

	plan := planner.PlanSelect(stmt, schema)

	if plan.AccessMethod != IndexRangeScan {
		t.Errorf("expected IndexRangeScan, got %v", plan.AccessMethod)
	}
	if plan.RangeLower != int64(5) {
		t.Errorf("expected lower bound 5, got %v", plan.RangeLower)
	}
	if !plan.LowerInclusive {
		t.Error("expected lower bound to be inclusive (>=)")
	}
	if plan.RangeUpper != int64(20) {
		t.Errorf("expected upper bound 20, got %v", plan.RangeUpper)
	}
	if plan.UpperInclusive {
		t.Error("expected upper bound to be exclusive (<)")
	}
}

func TestPlanSelect_NonPKColumn(t *testing.T) {
	planner := New()
	schema := testSchema()

	where := parseWhere(t, "SELECT * FROM users WHERE age > 18")
	stmt := &parser.SelectStatement{
		Columns: []parser.Expression{&parser.StarExpression{}},
		From:    "users",
		Where:   where,
	}

	plan := planner.PlanSelect(stmt, schema)

	// Should fall back to full table scan since 'age' is not indexed
	if plan.AccessMethod != FullTableScan {
		t.Errorf("expected FullTableScan for non-PK column, got %v", plan.AccessMethod)
	}
	if len(plan.Predicates) != 1 {
		t.Errorf("expected 1 predicate, got %d", len(plan.Predicates))
	}
	if plan.Predicates[0].Column != "age" {
		t.Errorf("expected predicate on 'age', got %q", plan.Predicates[0].Column)
	}
}

func TestPlanSelect_PKEqualityPreferredOverRange(t *testing.T) {
	planner := New()
	schema := testSchema()

	// Even with range predicates, equality should be preferred
	where := parseWhere(t, "SELECT * FROM users WHERE id = 5 AND id > 0")
	stmt := &parser.SelectStatement{
		Columns: []parser.Expression{&parser.StarExpression{}},
		From:    "users",
		Where:   where,
	}

	plan := planner.PlanSelect(stmt, schema)

	if plan.AccessMethod != IndexLookup {
		t.Errorf("expected IndexLookup (equality preferred), got %v", plan.AccessMethod)
	}
}

func TestAnalyzeWhere_NoWhere(t *testing.T) {
	planner := New()
	schema := testSchema()

	analysis := planner.AnalyzeWhere(nil, schema)

	if analysis.HasWhere {
		t.Error("expected HasWhere to be false")
	}
	if analysis.HasIndexablePred {
		t.Error("expected HasIndexablePred to be false")
	}
}

func TestAnalyzeWhere_WithOR(t *testing.T) {
	planner := New()
	schema := testSchema()

	where := parseWhere(t, "SELECT * FROM users WHERE id = 5 OR name = 'Alice'")

	analysis := planner.AnalyzeWhere(where, schema)

	if !analysis.HasWhere {
		t.Error("expected HasWhere to be true")
	}
	if !analysis.HasORCondition {
		t.Error("expected HasORCondition to be true")
	}
}

func TestAnalyzeWhere_PKPredicate(t *testing.T) {
	planner := New()
	schema := testSchema()

	where := parseWhere(t, "SELECT * FROM users WHERE id = 5 AND age > 18")

	analysis := planner.AnalyzeWhere(where, schema)

	if !analysis.HasIndexablePred {
		t.Error("expected HasIndexablePred to be true (pk predicate)")
	}

	// Should have 2 predicates
	if len(analysis.Predicates) != 2 {
		t.Fatalf("expected 2 predicates, got %d", len(analysis.Predicates))
	}

	// Find the PK predicate
	var pkPred *Predicate
	for i := range analysis.Predicates {
		if analysis.Predicates[i].Column == "id" {
			pkPred = &analysis.Predicates[i]
			break
		}
	}

	if pkPred == nil {
		t.Fatal("expected to find 'id' predicate")
	}
	if !pkPred.IsOnPK {
		t.Error("expected IsOnPK to be true for 'id' predicate")
	}
}

func TestQueryPlan_String(t *testing.T) {
	tests := []struct {
		name     string
		plan     QueryPlan
		contains string
	}{
		{
			name: "full scan",
			plan: QueryPlan{
				AccessMethod:  FullTableScan,
				EstimatedCost: 100.0,
			},
			contains: "FULL_TABLE_SCAN",
		},
		{
			name: "index lookup",
			plan: QueryPlan{
				AccessMethod:   IndexLookup,
				IndexColumn:    "id",
				IndexLookupKey: int64(42),
				EstimatedCost:  1.0,
			},
			contains: "INDEX_LOOKUP on id = 42",
		},
		{
			name: "index range scan",
			plan: QueryPlan{
				AccessMethod:   IndexRangeScan,
				IndexColumn:    "id",
				RangeLower:     int64(5),
				LowerInclusive: true,
				EstimatedCost:  10.0,
			},
			contains: "INDEX_RANGE_SCAN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			str := tt.plan.String()
			if !contains(str, tt.contains) {
				t.Errorf("expected %q to contain %q", str, tt.contains)
			}
		})
	}
}

func TestAccessMethod_String(t *testing.T) {
	tests := []struct {
		method   AccessMethod
		expected string
	}{
		{FullTableScan, "FULL_TABLE_SCAN"},
		{IndexLookup, "INDEX_LOOKUP"},
		{IndexRangeScan, "INDEX_RANGE_SCAN"},
	}

	for _, tt := range tests {
		if got := tt.method.String(); got != tt.expected {
			t.Errorf("AccessMethod(%d).String() = %q, want %q", tt.method, got, tt.expected)
		}
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr, 0))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
