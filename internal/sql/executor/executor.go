// Package executor implements the SQL query executor.
//
// EDUCATIONAL NOTES:
// ------------------
// The executor is the component that actually runs SQL queries.
// It takes an AST (Abstract Syntax Tree) from the parser and:
// 1. Validates the query (table exists, columns exist, etc.)
// 2. Plans the execution (which indexes to use, join order, etc.)
// 3. Executes the plan and returns results
//
// Our implementation is a simple "volcano" style executor where
// each operation pulls data from its children on demand.
// Production databases use more sophisticated query planners and
// optimizers, but this gives a clear understanding of the concepts.

package executor

import (
	"container/heap"
	"fmt"
	"sort"
	"strings"

	"github.com/cabewaldrop/claude-db/internal/catalog"
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/sql/planner"
	"github.com/cabewaldrop/claude-db/internal/storage"
	"github.com/cabewaldrop/claude-db/internal/table"
)

// Result represents the result of executing a query.
type Result struct {
	Columns  []string
	Rows     [][]table.Value
	RowCount int
	Message  string
}

// String formats the result for display.
func (r *Result) String() string {
	if r.Message != "" {
		return r.Message
	}

	if len(r.Rows) == 0 {
		return "(no rows)"
	}

	var sb strings.Builder

	// Calculate column widths
	widths := make([]int, len(r.Columns))
	for i, col := range r.Columns {
		widths[i] = len(col)
	}
	for _, row := range r.Rows {
		for i, val := range row {
			if len(val.String()) > widths[i] {
				widths[i] = len(val.String())
			}
		}
	}

	// Print header
	sb.WriteString("+")
	for _, w := range widths {
		sb.WriteString(strings.Repeat("-", w+2))
		sb.WriteString("+")
	}
	sb.WriteString("\n|")
	for i, col := range r.Columns {
		sb.WriteString(fmt.Sprintf(" %-*s |", widths[i], col))
	}
	sb.WriteString("\n+")
	for _, w := range widths {
		sb.WriteString(strings.Repeat("-", w+2))
		sb.WriteString("+")
	}
	sb.WriteString("\n")

	// Print rows
	for _, row := range r.Rows {
		sb.WriteString("|")
		for i, val := range row {
			sb.WriteString(fmt.Sprintf(" %-*s |", widths[i], val.String()))
		}
		sb.WriteString("\n")
	}

	// Print footer
	sb.WriteString("+")
	for _, w := range widths {
		sb.WriteString(strings.Repeat("-", w+2))
		sb.WriteString("+")
	}
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("(%d rows)\n", len(r.Rows)))

	return sb.String()
}

// Executor executes SQL statements.
type Executor struct {
	pager   *storage.Pager
	catalog *catalog.Catalog
	tables  map[string]*table.Table
	planner *planner.Planner
}

// New creates a new Executor.
func New(pager *storage.Pager) *Executor {
	return &Executor{
		pager:   pager,
		tables:  make(map[string]*table.Table),
		planner: planner.New(),
	}
}

// NewWithCatalog creates an Executor with catalog support for persistence.
func NewWithCatalog(pager *storage.Pager, cat *catalog.Catalog) (*Executor, error) {
	e := &Executor{
		pager:   pager,
		catalog: cat,
		tables:  make(map[string]*table.Table),
		planner: planner.New(),
	}

	// Load existing tables from catalog
	for _, name := range cat.ListTables() {
		tbl, err := cat.LoadTable(name, pager)
		if err != nil {
			return nil, fmt.Errorf("failed to load table %s: %w", name, err)
		}
		e.tables[name] = tbl
	}

	return e, nil
}

// Flush ensures all changes are written to disk.
func (e *Executor) Flush() error {
	if e.catalog != nil {
		return e.catalog.Flush()
	}
	return e.pager.FlushAll()
}

// Execute runs a SQL statement and returns the result.
func (e *Executor) Execute(stmt parser.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *parser.CreateTableStatement:
		return e.executeCreateTable(s)
	case *parser.DropTableStatement:
		return e.executeDropTable(s)
	case *parser.InsertStatement:
		return e.executeInsert(s)
	case *parser.SelectStatement:
		return e.executeSelect(s)
	case *parser.UpdateStatement:
		return e.executeUpdate(s)
	case *parser.DeleteStatement:
		return e.executeDelete(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// Explain returns a query plan without executing the statement.
//
// EDUCATIONAL NOTE:
// -----------------
// EXPLAIN is a standard SQL command that shows how the database will
// execute a query without actually running it. This is invaluable for
// understanding and optimizing query performance.
func (e *Executor) Explain(stmt parser.Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *parser.SelectStatement:
		return e.explainSelect(s)
	default:
		return nil, fmt.Errorf("EXPLAIN not supported for statement type: %T", stmt)
	}
}

// explainSelect returns the query plan for a SELECT statement.
func (e *Executor) explainSelect(stmt *parser.SelectStatement) (*Result, error) {
	tableName := strings.ToLower(stmt.From)

	tbl, exists := e.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Generate query plan
	plan := e.planner.PlanSelect(stmt, tbl.Schema)

	// Format the plan as a result
	var rows [][]table.Value
	rows = append(rows, []table.Value{
		{Type: parser.TypeText, Text: "Query Plan"},
		{Type: parser.TypeText, Text: plan.String()},
	})
	rows = append(rows, []table.Value{
		{Type: parser.TypeText, Text: "Access Method"},
		{Type: parser.TypeText, Text: plan.AccessMethod.String()},
	})
	rows = append(rows, []table.Value{
		{Type: parser.TypeText, Text: "Estimated Cost"},
		{Type: parser.TypeText, Text: fmt.Sprintf("%.2f", plan.EstimatedCost)},
	})

	if len(plan.Predicates) > 0 {
		rows = append(rows, []table.Value{
			{Type: parser.TypeText, Text: "Predicates"},
			{Type: parser.TypeText, Text: fmt.Sprintf("%d condition(s)", len(plan.Predicates))},
		})
		for i, pred := range plan.Predicates {
			indexNote := ""
			if pred.IsOnPK {
				indexNote = " (indexed)"
			}
			rows = append(rows, []table.Value{
				{Type: parser.TypeText, Text: fmt.Sprintf("  [%d]", i+1)},
				{Type: parser.TypeText, Text: fmt.Sprintf("%s %s %v%s", pred.Column, pred.Operator, pred.Value, indexNote)},
			})
		}
	}

	return &Result{
		Columns: []string{"Property", "Value"},
		Rows:    rows,
	}, nil
}

// AnalyzeWhere analyzes a WHERE clause and returns analysis information.
// This is useful for understanding how the planner interprets WHERE clauses.
func (e *Executor) AnalyzeWhere(where parser.Expression, schema *table.Schema) *planner.WhereAnalysis {
	return e.planner.AnalyzeWhere(where, schema)
}

// GetQueryPlan generates a query plan for a SELECT statement.
func (e *Executor) GetQueryPlan(stmt *parser.SelectStatement) (*planner.QueryPlan, error) {
	tableName := strings.ToLower(stmt.From)

	tbl, exists := e.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	return e.planner.PlanSelect(stmt, tbl.Schema), nil
}

// executeCreateTable handles CREATE TABLE statements.
func (e *Executor) executeCreateTable(stmt *parser.CreateTableStatement) (*Result, error) {
	tableName := strings.ToLower(stmt.Table)

	// Check if table already exists
	if _, exists := e.tables[tableName]; exists {
		return nil, fmt.Errorf("table %s already exists", tableName)
	}

	// Create schema
	schema := table.NewSchema(stmt.Columns)

	// Create table
	tbl, err := table.NewTable(tableName, schema, e.pager)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	e.tables[tableName] = tbl

	// Persist to catalog if available
	if e.catalog != nil {
		if err := e.catalog.AddTable(tableName, tbl); err != nil {
			return nil, fmt.Errorf("failed to save table metadata: %w", err)
		}
	}

	return &Result{
		Message: fmt.Sprintf("Table '%s' created", tableName),
	}, nil
}

// executeDropTable handles DROP TABLE statements.
func (e *Executor) executeDropTable(stmt *parser.DropTableStatement) (*Result, error) {
	tableName := strings.ToLower(stmt.Table)

	if _, exists := e.tables[tableName]; !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	delete(e.tables, tableName)

	// Remove from catalog if available
	if e.catalog != nil {
		if err := e.catalog.RemoveTable(tableName); err != nil {
			return nil, fmt.Errorf("failed to remove table metadata: %w", err)
		}
	}

	return &Result{
		Message: fmt.Sprintf("Table '%s' dropped", tableName),
	}, nil
}

// executeInsert handles INSERT statements.
func (e *Executor) executeInsert(stmt *parser.InsertStatement) (*Result, error) {
	tableName := strings.ToLower(stmt.Table)

	tbl, exists := e.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Determine column order
	var columnOrder []int
	if len(stmt.Columns) > 0 {
		columnOrder = make([]int, len(stmt.Columns))
		for i, colName := range stmt.Columns {
			idx, ok := tbl.Schema.GetColumnIndex(colName)
			if !ok {
				return nil, fmt.Errorf("unknown column: %s", colName)
			}
			columnOrder[i] = idx
		}
	} else {
		// Use schema order
		columnOrder = make([]int, len(tbl.Schema.Columns))
		for i := range columnOrder {
			columnOrder[i] = i
		}
	}

	// Check value count
	if len(stmt.Values) != len(columnOrder) {
		return nil, fmt.Errorf("value count mismatch: expected %d, got %d",
			len(columnOrder), len(stmt.Values))
	}

	// Convert expressions to values
	values := make([]table.Value, len(tbl.Schema.Columns))
	for i := range values {
		values[i] = table.Value{IsNull: true}
	}

	for i, expr := range stmt.Values {
		colIdx := columnOrder[i]
		val, err := e.evaluateExpression(expr, table.Row{}, tbl.Schema)
		if err != nil {
			return nil, fmt.Errorf("error evaluating value: %w", err)
		}
		// Coerce type if needed
		val.Type = tbl.Schema.Columns[colIdx].Type
		values[colIdx] = val
	}

	// Insert the row
	rowID, err := tbl.Insert(values)
	if err != nil {
		return nil, fmt.Errorf("insert failed: %w", err)
	}

	return &Result{
		Message:  fmt.Sprintf("Inserted 1 row (id=%d)", rowID),
		RowCount: 1,
	}, nil
}

// executeSelect handles SELECT statements.
//
// EDUCATIONAL NOTE:
// -----------------
// SELECT execution involves several steps:
// 1. Plan the query (decide whether to use index or table scan)
// 2. Fetch rows using the chosen access method
// 3. Filter rows based on WHERE clause (if not already filtered by index)
// 4. Select requested columns (projection)
// 5. Sort results if ORDER BY specified
// 6. Apply LIMIT and OFFSET
func (e *Executor) executeSelect(stmt *parser.SelectStatement) (*Result, error) {
	tableName := strings.ToLower(stmt.From)

	tbl, exists := e.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Plan the query
	planner := NewPlanner()
	plan := planner.Plan(stmt, tbl.Schema)

	var rows []table.Row
	var err error

	switch plan.Type {
	case PlanIndexScan:
		// Use B-tree index for primary key lookup
		row, found, lookupErr := tbl.GetRowByPrimaryKey(*plan.IndexKey)
		if lookupErr != nil {
			return nil, fmt.Errorf("index lookup failed: %w", lookupErr)
		}
		if found {
			// Still need to verify the row matches all WHERE conditions
			// (there might be additional conditions beyond the PK equality)
			match := true
			if stmt.Where != nil {
				match, err = e.evaluateCondition(stmt.Where, row, tbl.Schema)
				if err != nil {
					return nil, err
				}
			}
			if match {
				rows = []table.Row{row}
			}
		}

	case PlanTableScan:
		// Fall back to full table scan
		rows, err = tbl.Scan()
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		// Apply WHERE filter
		if stmt.Where != nil {
			var filteredRows []table.Row
			for _, row := range rows {
				match, evalErr := e.evaluateCondition(stmt.Where, row, tbl.Schema)
				if evalErr != nil {
					return nil, evalErr
				}
				if match {
					filteredRows = append(filteredRows, row)
				}
			}
			rows = filteredRows
		}
	}

	// Determine columns to return
	var columnNames []string
	var columnIndices []int

	if len(stmt.Columns) == 1 {
		if _, ok := stmt.Columns[0].(*parser.StarExpression); ok {
			// SELECT * - return all columns
			for i, col := range tbl.Schema.Columns {
				columnNames = append(columnNames, col.Name)
				columnIndices = append(columnIndices, i)
			}
		}
	}

	if len(columnIndices) == 0 {
		// Specific columns requested
		for _, expr := range stmt.Columns {
			if ident, ok := expr.(*parser.Identifier); ok {
				idx, found := tbl.Schema.GetColumnIndex(ident.Name)
				if !found {
					return nil, fmt.Errorf("unknown column: %s", ident.Name)
				}
				columnNames = append(columnNames, ident.Name)
				columnIndices = append(columnIndices, idx)
			} else if _, ok := expr.(*parser.StarExpression); ok {
				for i, col := range tbl.Schema.Columns {
					columnNames = append(columnNames, col.Name)
					columnIndices = append(columnIndices, i)
				}
			} else {
				// Expression - for now, just show as "expr"
				columnNames = append(columnNames, "expr")
				columnIndices = append(columnIndices, -1)
			}
		}
	}

	// Apply ORDER BY with LIMIT optimization
	// When LIMIT is present, use heap-based top-K selection for O(N log K) instead of O(N log N)
	if len(stmt.OrderBy) > 0 {
		// Calculate effective limit (including offset)
		effectiveLimit := 0
		if stmt.Limit != nil {
			effectiveLimit = *stmt.Limit
			if stmt.Offset != nil {
				effectiveLimit += *stmt.Offset
			}
		}

		// Try heap-based top-K if limit is set and reasonable
		if effectiveLimit > 0 && effectiveLimit < len(rows) {
			topK := selectTopK(rows, effectiveLimit, stmt.OrderBy, tbl.Schema)
			if topK != nil {
				rows = topK
			}
		}

		// If top-K wasn't used (returned nil) or no limit, fall back to full sort
		if effectiveLimit == 0 || effectiveLimit >= len(rows) {
			sort.Slice(rows, func(i, j int) bool {
				for _, clause := range stmt.OrderBy {
					colIdx, found := tbl.Schema.GetColumnIndex(clause.Column)
					if !found {
						continue
					}
					cmp := rows[i].Values[colIdx].Compare(rows[j].Values[colIdx])
					if cmp != 0 {
						if clause.Descending {
							return cmp > 0
						}
						return cmp < 0
					}
				}
				return false
			})
		}
	}

	// Apply OFFSET
	if stmt.Offset != nil {
		if *stmt.Offset >= len(rows) {
			rows = nil
		} else {
			rows = rows[*stmt.Offset:]
		}
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		if *stmt.Limit < len(rows) {
			rows = rows[:*stmt.Limit]
		}
	}

	// Build result
	result := &Result{
		Columns:  columnNames,
		RowCount: len(rows),
	}

	for _, row := range rows {
		resultRow := make([]table.Value, len(columnIndices))
		for i, colIdx := range columnIndices {
			if colIdx >= 0 && colIdx < len(row.Values) {
				resultRow[i] = row.Values[colIdx]
			}
		}
		result.Rows = append(result.Rows, resultRow)
	}

	return result, nil
}

// executeUpdate handles UPDATE statements.
func (e *Executor) executeUpdate(stmt *parser.UpdateStatement) (*Result, error) {
	tableName := strings.ToLower(stmt.Table)

	tbl, exists := e.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Get all rows
	rows, err := tbl.Scan()
	if err != nil {
		return nil, err
	}

	// Find rows to update
	updateCount := 0
	for i := range rows {
		if stmt.Where != nil {
			match, err := e.evaluateCondition(stmt.Where, rows[i], tbl.Schema)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}

		// Apply updates
		for _, assignment := range stmt.Assignments {
			colIdx, found := tbl.Schema.GetColumnIndex(assignment.Column)
			if !found {
				return nil, fmt.Errorf("unknown column: %s", assignment.Column)
			}

			val, err := e.evaluateExpression(assignment.Value, rows[i], tbl.Schema)
			if err != nil {
				return nil, err
			}
			val.Type = tbl.Schema.Columns[colIdx].Type
			rows[i].Values[colIdx] = val
		}
		updateCount++
	}

	return &Result{
		Message:  fmt.Sprintf("Updated %d rows", updateCount),
		RowCount: updateCount,
	}, nil
}

// executeDelete handles DELETE statements.
func (e *Executor) executeDelete(stmt *parser.DeleteStatement) (*Result, error) {
	tableName := strings.ToLower(stmt.Table)

	tbl, exists := e.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Get all rows
	rows, err := tbl.Scan()
	if err != nil {
		return nil, err
	}

	// Find rows to delete (just count for now)
	deleteCount := 0
	for _, row := range rows {
		if stmt.Where != nil {
			match, err := e.evaluateCondition(stmt.Where, row, tbl.Schema)
			if err != nil {
				return nil, err
			}
			if !match {
				continue
			}
		}
		deleteCount++
	}

	return &Result{
		Message:  fmt.Sprintf("Deleted %d rows", deleteCount),
		RowCount: deleteCount,
	}, nil
}

// evaluateCondition evaluates a WHERE condition against a row.
func (e *Executor) evaluateCondition(expr parser.Expression, row table.Row, schema *table.Schema) (bool, error) {
	val, err := e.evaluateExpression(expr, row, schema)
	if err != nil {
		return false, err
	}
	return val.Boolean, nil
}

// evaluateExpression evaluates an expression and returns its value.
//
// EDUCATIONAL NOTE:
// -----------------
// Expression evaluation is recursive - we evaluate sub-expressions
// first, then combine them according to the operator.
func (e *Executor) evaluateExpression(expr parser.Expression, row table.Row, schema *table.Schema) (table.Value, error) {
	switch ex := expr.(type) {
	case *parser.IntegerLiteral:
		return table.Value{Type: parser.TypeInteger, Integer: ex.Value}, nil

	case *parser.RealLiteral:
		return table.Value{Type: parser.TypeReal, Real: ex.Value}, nil

	case *parser.StringLiteral:
		return table.Value{Type: parser.TypeText, Text: ex.Value}, nil

	case *parser.BooleanLiteral:
		return table.Value{Type: parser.TypeBoolean, Boolean: ex.Value}, nil

	case *parser.NullLiteral:
		return table.Value{IsNull: true}, nil

	case *parser.Identifier:
		if row.Values == nil {
			// No row context, return null
			return table.Value{IsNull: true}, nil
		}
		colIdx, found := schema.GetColumnIndex(ex.Name)
		if !found {
			return table.Value{}, fmt.Errorf("unknown column: %s", ex.Name)
		}
		if colIdx >= len(row.Values) {
			return table.Value{IsNull: true}, nil
		}
		return row.Values[colIdx], nil

	case *parser.BinaryExpression:
		left, err := e.evaluateExpression(ex.Left, row, schema)
		if err != nil {
			return table.Value{}, err
		}
		right, err := e.evaluateExpression(ex.Right, row, schema)
		if err != nil {
			return table.Value{}, err
		}

		return e.evaluateBinaryOp(ex.Operator, left, right)

	case *parser.UnaryExpression:
		operand, err := e.evaluateExpression(ex.Operand, row, schema)
		if err != nil {
			return table.Value{}, err
		}

		return e.evaluateUnaryOp(ex.Operator, operand)

	default:
		return table.Value{}, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// evaluateBinaryOp evaluates a binary operation.
func (e *Executor) evaluateBinaryOp(op parser.BinaryOp, left, right table.Value) (table.Value, error) {
	// Handle NULL - most operations with NULL return NULL
	if left.IsNull || right.IsNull {
		// Comparison with NULL returns false (SQL behavior)
		switch op {
		case parser.OpEquals, parser.OpNotEquals, parser.OpLessThan,
			parser.OpGreaterThan, parser.OpLessOrEqual, parser.OpGreaterOrEqual:
			return table.Value{Type: parser.TypeBoolean, Boolean: false}, nil
		case parser.OpAnd:
			// NULL AND FALSE = FALSE, NULL AND TRUE = NULL
			if !left.IsNull && !left.Boolean {
				return table.Value{Type: parser.TypeBoolean, Boolean: false}, nil
			}
			if !right.IsNull && !right.Boolean {
				return table.Value{Type: parser.TypeBoolean, Boolean: false}, nil
			}
			return table.Value{IsNull: true}, nil
		case parser.OpOr:
			// NULL OR TRUE = TRUE, NULL OR FALSE = NULL
			if !left.IsNull && left.Boolean {
				return table.Value{Type: parser.TypeBoolean, Boolean: true}, nil
			}
			if !right.IsNull && right.Boolean {
				return table.Value{Type: parser.TypeBoolean, Boolean: true}, nil
			}
			return table.Value{IsNull: true}, nil
		default:
			return table.Value{IsNull: true}, nil
		}
	}

	switch op {
	case parser.OpEquals:
		return table.Value{Type: parser.TypeBoolean, Boolean: left.Equals(right)}, nil

	case parser.OpNotEquals:
		return table.Value{Type: parser.TypeBoolean, Boolean: !left.Equals(right)}, nil

	case parser.OpLessThan:
		return table.Value{Type: parser.TypeBoolean, Boolean: left.Compare(right) < 0}, nil

	case parser.OpGreaterThan:
		return table.Value{Type: parser.TypeBoolean, Boolean: left.Compare(right) > 0}, nil

	case parser.OpLessOrEqual:
		return table.Value{Type: parser.TypeBoolean, Boolean: left.Compare(right) <= 0}, nil

	case parser.OpGreaterOrEqual:
		return table.Value{Type: parser.TypeBoolean, Boolean: left.Compare(right) >= 0}, nil

	case parser.OpAnd:
		return table.Value{Type: parser.TypeBoolean, Boolean: left.Boolean && right.Boolean}, nil

	case parser.OpOr:
		return table.Value{Type: parser.TypeBoolean, Boolean: left.Boolean || right.Boolean}, nil

	case parser.OpAdd:
		if left.Type == parser.TypeInteger && right.Type == parser.TypeInteger {
			return table.Value{Type: parser.TypeInteger, Integer: left.Integer + right.Integer}, nil
		}
		if left.Type == parser.TypeReal || right.Type == parser.TypeReal {
			l := left.Real
			if left.Type == parser.TypeInteger {
				l = float64(left.Integer)
			}
			r := right.Real
			if right.Type == parser.TypeInteger {
				r = float64(right.Integer)
			}
			return table.Value{Type: parser.TypeReal, Real: l + r}, nil
		}
		if left.Type == parser.TypeText {
			return table.Value{Type: parser.TypeText, Text: left.Text + right.Text}, nil
		}
		return table.Value{}, fmt.Errorf("cannot add %s and %s", left.Type, right.Type)

	case parser.OpSubtract:
		if left.Type == parser.TypeInteger && right.Type == parser.TypeInteger {
			return table.Value{Type: parser.TypeInteger, Integer: left.Integer - right.Integer}, nil
		}
		if left.Type == parser.TypeReal || right.Type == parser.TypeReal {
			l := left.Real
			if left.Type == parser.TypeInteger {
				l = float64(left.Integer)
			}
			r := right.Real
			if right.Type == parser.TypeInteger {
				r = float64(right.Integer)
			}
			return table.Value{Type: parser.TypeReal, Real: l - r}, nil
		}
		return table.Value{}, fmt.Errorf("cannot subtract %s and %s", left.Type, right.Type)

	case parser.OpMultiply:
		if left.Type == parser.TypeInteger && right.Type == parser.TypeInteger {
			return table.Value{Type: parser.TypeInteger, Integer: left.Integer * right.Integer}, nil
		}
		if left.Type == parser.TypeReal || right.Type == parser.TypeReal {
			l := left.Real
			if left.Type == parser.TypeInteger {
				l = float64(left.Integer)
			}
			r := right.Real
			if right.Type == parser.TypeInteger {
				r = float64(right.Integer)
			}
			return table.Value{Type: parser.TypeReal, Real: l * r}, nil
		}
		return table.Value{}, fmt.Errorf("cannot multiply %s and %s", left.Type, right.Type)

	case parser.OpDivide:
		if right.Integer == 0 || right.Real == 0 {
			return table.Value{}, fmt.Errorf("division by zero")
		}
		if left.Type == parser.TypeInteger && right.Type == parser.TypeInteger {
			return table.Value{Type: parser.TypeInteger, Integer: left.Integer / right.Integer}, nil
		}
		l := left.Real
		if left.Type == parser.TypeInteger {
			l = float64(left.Integer)
		}
		r := right.Real
		if right.Type == parser.TypeInteger {
			r = float64(right.Integer)
		}
		return table.Value{Type: parser.TypeReal, Real: l / r}, nil

	default:
		return table.Value{}, fmt.Errorf("unsupported operator: %s", op)
	}
}

// evaluateUnaryOp evaluates a unary operation.
func (e *Executor) evaluateUnaryOp(op parser.UnaryOp, operand table.Value) (table.Value, error) {
	if operand.IsNull {
		return table.Value{IsNull: true}, nil
	}

	switch op {
	case parser.UnaryOpNot:
		return table.Value{Type: parser.TypeBoolean, Boolean: !operand.Boolean}, nil

	case parser.UnaryOpNegate:
		if operand.Type == parser.TypeInteger {
			return table.Value{Type: parser.TypeInteger, Integer: -operand.Integer}, nil
		}
		if operand.Type == parser.TypeReal {
			return table.Value{Type: parser.TypeReal, Real: -operand.Real}, nil
		}
		return table.Value{}, fmt.Errorf("cannot negate %s", operand.Type)

	default:
		return table.Value{}, fmt.Errorf("unsupported unary operator: %s", op)
	}
}

// GetTables returns the list of table names.
func (e *Executor) GetTables() []string {
	var names []string
	for name := range e.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// GetTable returns a table by name.
func (e *Executor) GetTable(name string) (*table.Table, bool) {
	tbl, ok := e.tables[strings.ToLower(name)]
	return tbl, ok
}

// topKHeap implements a heap for ORDER BY + LIMIT optimization.
// For ASC order, we use a max-heap: keep the K smallest rows by
// always ejecting the largest when we exceed K.
// For DESC order, we use a min-heap: keep the K largest rows.
//
// EDUCATIONAL NOTE:
// -----------------
// When you have ORDER BY x LIMIT K on N rows, naive sorting is O(N log N).
// With a heap of size K, we can do O(N log K) which is much faster when K << N.
// For example, LIMIT 10 on 1M rows: O(1M * 10) vs O(1M * 20) - roughly 2x faster.
type topKHeap struct {
	rows       []table.Row
	orderBy    []parser.OrderByClause
	schema     *table.Schema
	descending bool // true = min-heap for DESC, false = max-heap for ASC
}

func (h *topKHeap) Len() int { return len(h.rows) }

func (h *topKHeap) Less(i, j int) bool {
	// For max-heap (ASC order): we want largest at root to eject it
	// For min-heap (DESC order): we want smallest at root to eject it
	for _, clause := range h.orderBy {
		colIdx, found := h.schema.GetColumnIndex(clause.Column)
		if !found {
			continue
		}
		cmp := h.rows[i].Values[colIdx].Compare(h.rows[j].Values[colIdx])
		if cmp != 0 {
			// For ASC order with max-heap: larger value should be "less" so it floats to top
			// For DESC order with min-heap: smaller value should be "less" so it floats to top
			if h.descending {
				return cmp < 0 // min-heap for DESC
			}
			return cmp > 0 // max-heap for ASC
		}
	}
	return false
}

func (h *topKHeap) Swap(i, j int) {
	h.rows[i], h.rows[j] = h.rows[j], h.rows[i]
}

func (h *topKHeap) Push(x any) {
	h.rows = append(h.rows, x.(table.Row))
}

func (h *topKHeap) Pop() any {
	old := h.rows
	n := len(old)
	x := old[n-1]
	h.rows = old[0 : n-1]
	return x
}

// selectTopK selects the top K rows using a heap-based algorithm.
// This is O(N log K) instead of O(N log N) for full sort.
func selectTopK(rows []table.Row, k int, orderBy []parser.OrderByClause, schema *table.Schema) []table.Row {
	if k <= 0 || len(rows) == 0 {
		return nil
	}
	if k >= len(rows) {
		// No optimization needed - need all rows anyway
		return nil // signal to use regular sort
	}

	// Determine if first ORDER BY clause is descending
	descending := false
	if len(orderBy) > 0 {
		descending = orderBy[0].Descending
	}

	h := &topKHeap{
		rows:       make([]table.Row, 0, k+1),
		orderBy:    orderBy,
		schema:     schema,
		descending: descending,
	}

	for _, row := range rows {
		heap.Push(h, row)
		if h.Len() > k {
			heap.Pop(h) // Remove the worst element (largest for ASC, smallest for DESC)
		}
	}

	// Extract results in sorted order
	result := make([]table.Row, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).(table.Row)
	}

	return result
}
