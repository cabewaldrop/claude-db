// Package planner implements the SQL query planner.
//
// EDUCATIONAL NOTES:
// ------------------
// A query planner analyzes SQL queries to determine the most efficient
// execution strategy. For SELECT queries, the key decision is:
// - Should we scan the entire table (full table scan)?
// - Can we use an index for faster lookup?
//
// The planner examines WHERE clauses to identify:
// 1. Predicates on indexed columns (can use index)
// 2. Types of comparisons (equality vs range)
// 3. Selectivity (what fraction of rows match)
//
// Our planner focuses on WHERE clause analysis and access path selection.
// Production databases have much more sophisticated planners that handle
// joins, subqueries, and cost-based optimization.

package planner

import (
	"fmt"

	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/table"
)

// AccessMethod represents how to access table data.
type AccessMethod int

const (
	// FullTableScan reads all rows and filters them.
	FullTableScan AccessMethod = iota
	// IndexLookup uses an index for exact key match (e.g., pk = 5).
	IndexLookup
	// IndexRangeScan uses an index for range queries (e.g., pk > 5).
	IndexRangeScan
)

func (m AccessMethod) String() string {
	switch m {
	case FullTableScan:
		return "FULL_TABLE_SCAN"
	case IndexLookup:
		return "INDEX_LOOKUP"
	case IndexRangeScan:
		return "INDEX_RANGE_SCAN"
	default:
		return "UNKNOWN"
	}
}

// Predicate represents a condition in the WHERE clause.
type Predicate struct {
	Column   string
	Operator parser.BinaryOp
	Value    interface{} // The literal value being compared to
	IsOnPK   bool        // True if this predicate is on the primary key
}

// QueryPlan represents the execution plan for a query.
type QueryPlan struct {
	AccessMethod   AccessMethod
	Predicates     []Predicate
	IndexColumn    string      // Column to use for index access (if applicable)
	IndexLookupKey interface{} // Key value for IndexLookup
	RangeLower     interface{} // Lower bound for IndexRangeScan (nil = unbounded)
	RangeUpper     interface{} // Upper bound for IndexRangeScan (nil = unbounded)
	LowerInclusive bool        // True if lower bound is inclusive (>=)
	UpperInclusive bool        // True if upper bound is inclusive (<=)
	EstimatedCost  float64     // Relative cost estimate (lower is better)
}

// String returns a human-readable representation of the query plan.
func (p *QueryPlan) String() string {
	switch p.AccessMethod {
	case IndexLookup:
		return fmt.Sprintf("INDEX_LOOKUP on %s = %v (cost: %.2f)", p.IndexColumn, p.IndexLookupKey, p.EstimatedCost)
	case IndexRangeScan:
		lower := "-inf"
		upper := "+inf"
		if p.RangeLower != nil {
			op := ">"
			if p.LowerInclusive {
				op = ">="
			}
			lower = fmt.Sprintf("%s %v", op, p.RangeLower)
		}
		if p.RangeUpper != nil {
			op := "<"
			if p.UpperInclusive {
				op = "<="
			}
			upper = fmt.Sprintf("%s %v", op, p.RangeUpper)
		}
		return fmt.Sprintf("INDEX_RANGE_SCAN on %s (%s, %s) (cost: %.2f)", p.IndexColumn, lower, upper, p.EstimatedCost)
	default:
		return fmt.Sprintf("FULL_TABLE_SCAN (cost: %.2f)", p.EstimatedCost)
	}
}

// Planner analyzes queries and generates execution plans.
type Planner struct{}

// New creates a new Planner.
func New() *Planner {
	return &Planner{}
}

// PlanSelect analyzes a SELECT statement and returns a query plan.
func (p *Planner) PlanSelect(stmt *parser.SelectStatement, schema *table.Schema) *QueryPlan {
	plan := &QueryPlan{
		AccessMethod:  FullTableScan,
		Predicates:    []Predicate{},
		EstimatedCost: 100.0, // Base cost for full table scan
	}

	if stmt.Where == nil {
		return plan
	}

	// Extract predicates from WHERE clause
	plan.Predicates = p.extractPredicates(stmt.Where, schema)

	// Check if we can use an index
	pkName := ""
	if schema.PrimaryKey >= 0 {
		pkName = schema.Columns[schema.PrimaryKey].Name
	}

	// Look for predicates on the primary key
	for i := range plan.Predicates {
		pred := &plan.Predicates[i]
		if pred.Column == pkName {
			pred.IsOnPK = true
		}
	}

	// Determine best access method based on PK predicates
	p.selectAccessMethod(plan, pkName)

	return plan
}

// extractPredicates recursively extracts predicates from a WHERE expression.
//
// EDUCATIONAL NOTE:
// -----------------
// WHERE clauses can be arbitrarily complex:
//   WHERE a = 1 AND b > 5 AND (c = 3 OR d = 4)
//
// We recursively walk the expression tree to find simple predicates
// (column op value). Complex expressions like OR make index use difficult.
func (p *Planner) extractPredicates(expr parser.Expression, schema *table.Schema) []Predicate {
	var predicates []Predicate

	switch e := expr.(type) {
	case *parser.BinaryExpression:
		switch e.Operator {
		case parser.OpAnd:
			// AND: both sides contribute predicates
			predicates = append(predicates, p.extractPredicates(e.Left, schema)...)
			predicates = append(predicates, p.extractPredicates(e.Right, schema)...)

		case parser.OpOr:
			// OR: we can't easily use predicates from OR branches for index access
			// Just note that we have an OR condition

		case parser.OpEquals, parser.OpNotEquals, parser.OpLessThan,
			parser.OpGreaterThan, parser.OpLessOrEqual, parser.OpGreaterOrEqual:
			// This is a comparison predicate
			pred := p.extractComparison(e, schema)
			if pred != nil {
				predicates = append(predicates, *pred)
			}
		}
	}

	return predicates
}

// extractComparison extracts a predicate from a comparison expression.
func (p *Planner) extractComparison(expr *parser.BinaryExpression, schema *table.Schema) *Predicate {
	// Check for: column op literal
	ident, isIdent := expr.Left.(*parser.Identifier)
	if !isIdent {
		// Try reversed: literal op column
		ident, isIdent = expr.Right.(*parser.Identifier)
		if !isIdent {
			return nil
		}
		// Reverse the operator
		return p.extractReversedComparison(expr, schema, ident)
	}

	// Verify column exists
	if _, ok := schema.GetColumnIndex(ident.Name); !ok {
		return nil
	}

	// Extract the literal value
	value := p.extractLiteral(expr.Right)
	if value == nil {
		return nil
	}

	return &Predicate{
		Column:   ident.Name,
		Operator: expr.Operator,
		Value:    value,
	}
}

// extractReversedComparison handles cases like "5 = id" instead of "id = 5".
func (p *Planner) extractReversedComparison(expr *parser.BinaryExpression, schema *table.Schema, ident *parser.Identifier) *Predicate {
	// Verify column exists
	if _, ok := schema.GetColumnIndex(ident.Name); !ok {
		return nil
	}

	// Extract the literal value (from left side now)
	value := p.extractLiteral(expr.Left)
	if value == nil {
		return nil
	}

	// Reverse the operator for comparison
	op := expr.Operator
	switch op {
	case parser.OpLessThan:
		op = parser.OpGreaterThan
	case parser.OpGreaterThan:
		op = parser.OpLessThan
	case parser.OpLessOrEqual:
		op = parser.OpGreaterOrEqual
	case parser.OpGreaterOrEqual:
		op = parser.OpLessOrEqual
	}

	return &Predicate{
		Column:   ident.Name,
		Operator: op,
		Value:    value,
	}
}

// extractLiteral extracts the value from a literal expression.
func (p *Planner) extractLiteral(expr parser.Expression) interface{} {
	switch e := expr.(type) {
	case *parser.IntegerLiteral:
		return e.Value
	case *parser.RealLiteral:
		return e.Value
	case *parser.StringLiteral:
		return e.Value
	case *parser.BooleanLiteral:
		return e.Value
	}
	return nil
}

// selectAccessMethod determines the best access method based on predicates.
//
// EDUCATIONAL NOTE:
// -----------------
// Access method selection is a key query optimization decision:
//
// 1. INDEX_LOOKUP: Best for equality on indexed column (pk = 5)
//    Cost: O(log n) - just follow B-tree path
//
// 2. INDEX_RANGE_SCAN: Good for ranges on indexed column (pk > 5)
//    Cost: O(log n + k) where k is matching rows
//
// 3. FULL_TABLE_SCAN: Required when no useful index exists
//    Cost: O(n) - must examine every row
func (p *Planner) selectAccessMethod(plan *QueryPlan, pkName string) {
	if pkName == "" {
		// No primary key, can only do full scan
		return
	}

	var eqPredicate *Predicate
	var rangeLower *Predicate
	var rangeUpper *Predicate

	for i := range plan.Predicates {
		pred := &plan.Predicates[i]
		if !pred.IsOnPK {
			continue
		}

		switch pred.Operator {
		case parser.OpEquals:
			eqPredicate = pred
		case parser.OpGreaterThan, parser.OpGreaterOrEqual:
			rangeLower = pred
		case parser.OpLessThan, parser.OpLessOrEqual:
			rangeUpper = pred
		}
	}

	// Prefer equality lookup over range scan
	if eqPredicate != nil {
		plan.AccessMethod = IndexLookup
		plan.IndexColumn = pkName
		plan.IndexLookupKey = eqPredicate.Value
		plan.EstimatedCost = 1.0 // Very cheap - single lookup
		return
	}

	// Check for range scan
	if rangeLower != nil || rangeUpper != nil {
		plan.AccessMethod = IndexRangeScan
		plan.IndexColumn = pkName
		plan.EstimatedCost = 10.0 // Cheaper than full scan, more than lookup

		if rangeLower != nil {
			plan.RangeLower = rangeLower.Value
			plan.LowerInclusive = rangeLower.Operator == parser.OpGreaterOrEqual
		}
		if rangeUpper != nil {
			plan.RangeUpper = rangeUpper.Value
			plan.UpperInclusive = rangeUpper.Operator == parser.OpLessOrEqual
		}
	}
}

// AnalyzeWhere analyzes a WHERE expression and returns analysis information.
// This is a simpler interface for just understanding the WHERE clause.
func (p *Planner) AnalyzeWhere(where parser.Expression, schema *table.Schema) *WhereAnalysis {
	analysis := &WhereAnalysis{
		HasWhere:         where != nil,
		Predicates:       []Predicate{},
		HasIndexablePred: false,
		HasORCondition:   false,
	}

	if where == nil {
		return analysis
	}

	// Check for OR conditions
	analysis.HasORCondition = p.containsOR(where)

	// Extract predicates
	analysis.Predicates = p.extractPredicates(where, schema)

	// Check for primary key predicates
	pkName := ""
	if schema.PrimaryKey >= 0 {
		pkName = schema.Columns[schema.PrimaryKey].Name
	}

	for i := range analysis.Predicates {
		pred := &analysis.Predicates[i]
		if pred.Column == pkName {
			pred.IsOnPK = true
			analysis.HasIndexablePred = true
		}
	}

	return analysis
}

// WhereAnalysis contains analysis results for a WHERE clause.
type WhereAnalysis struct {
	HasWhere         bool
	Predicates       []Predicate
	HasIndexablePred bool // True if any predicate can use an index
	HasORCondition   bool // True if WHERE contains OR (complicates index use)
}

// String returns a human-readable summary of the analysis.
func (a *WhereAnalysis) String() string {
	if !a.HasWhere {
		return "No WHERE clause (full table scan required)"
	}

	result := fmt.Sprintf("WHERE analysis: %d predicate(s)", len(a.Predicates))
	if a.HasIndexablePred {
		result += ", index usable"
	} else {
		result += ", no index applicable"
	}
	if a.HasORCondition {
		result += ", contains OR"
	}
	return result
}

// containsOR checks if an expression contains an OR operator.
func (p *Planner) containsOR(expr parser.Expression) bool {
	switch e := expr.(type) {
	case *parser.BinaryExpression:
		if e.Operator == parser.OpOr {
			return true
		}
		return p.containsOR(e.Left) || p.containsOR(e.Right)
	}
	return false
}
