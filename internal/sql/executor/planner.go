// Package executor - Query planner implementation
//
// EDUCATIONAL NOTES:
// ------------------
// A query planner analyzes SQL queries and decides how to execute them efficiently.
// The main decision is whether to:
// 1. Scan the entire table (TableScan) - simple but O(n)
// 2. Use an index (IndexScan) - fast O(log n) for indexed columns
//
// Our simple planner looks for equality conditions on the primary key
// in the WHERE clause. More sophisticated planners would also consider:
// - Range conditions (>, <, BETWEEN)
// - Multiple indexes
// - Join ordering
// - Statistics about data distribution

package executor

import (
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/table"
)

// PlanType indicates the type of execution plan.
type PlanType int

const (
	// PlanTableScan indicates a full table scan.
	PlanTableScan PlanType = iota
	// PlanIndexScan indicates a primary key index lookup.
	PlanIndexScan
)

// QueryPlan represents how to execute a SELECT query.
type QueryPlan struct {
	Type PlanType

	// For IndexScan: the primary key value to look up
	IndexKey *table.Value
}

// Planner analyzes queries and produces execution plans.
type Planner struct{}

// NewPlanner creates a new query planner.
func NewPlanner() *Planner {
	return &Planner{}
}

// Plan analyzes a SELECT statement and returns an execution plan.
//
// EDUCATIONAL NOTE:
// -----------------
// We look for WHERE clauses of the form: pk_column = literal_value
// This is the simplest case for index usage. A real planner would handle:
// - pk_column = expression (if expression can be evaluated without row data)
// - Compound conditions with AND (pk = val AND other_condition)
// - IN clauses (pk IN (1, 2, 3))
func (p *Planner) Plan(stmt *parser.SelectStatement, schema *table.Schema) *QueryPlan {
	// Default to table scan
	plan := &QueryPlan{Type: PlanTableScan}

	// Can't use index if no WHERE clause
	if stmt.Where == nil {
		return plan
	}

	// Can't use index if no primary key
	if schema.PrimaryKey < 0 {
		return plan
	}

	pkColumn := schema.Columns[schema.PrimaryKey].Name

	// Try to extract PK equality condition
	keyValue := extractPKEquality(stmt.Where, pkColumn)
	if keyValue != nil {
		plan.Type = PlanIndexScan
		plan.IndexKey = keyValue
	}

	return plan
}

// extractPKEquality looks for a condition of the form: pk_column = literal
// Returns the literal value if found, nil otherwise.
func extractPKEquality(expr parser.Expression, pkColumn string) *table.Value {
	switch e := expr.(type) {
	case *parser.BinaryExpression:
		// Look for equality operator
		if e.Operator == parser.OpEquals {
			// Check if left side is the PK column and right side is a literal
			if ident, ok := e.Left.(*parser.Identifier); ok {
				if ident.Name == pkColumn {
					return extractLiteralValue(e.Right)
				}
			}
			// Check if right side is the PK column and left side is a literal
			if ident, ok := e.Right.(*parser.Identifier); ok {
				if ident.Name == pkColumn {
					return extractLiteralValue(e.Left)
				}
			}
		}

		// For AND conditions, check both sides
		if e.Operator == parser.OpAnd {
			if val := extractPKEquality(e.Left, pkColumn); val != nil {
				return val
			}
			if val := extractPKEquality(e.Right, pkColumn); val != nil {
				return val
			}
		}
	}

	return nil
}

// extractLiteralValue extracts a table.Value from a literal expression.
func extractLiteralValue(expr parser.Expression) *table.Value {
	switch lit := expr.(type) {
	case *parser.IntegerLiteral:
		return &table.Value{Type: parser.TypeInteger, Integer: lit.Value}
	case *parser.RealLiteral:
		return &table.Value{Type: parser.TypeReal, Real: lit.Value}
	case *parser.StringLiteral:
		return &table.Value{Type: parser.TypeText, Text: lit.Value}
	case *parser.BooleanLiteral:
		return &table.Value{Type: parser.TypeBoolean, Boolean: lit.Value}
	default:
		return nil
	}
}
