package parser

import (
	"testing"

	"github.com/cabewaldrop/claude-db/internal/sql/lexer"
)

func TestParseSelect(t *testing.T) {
	tests := []struct {
		input      string
		expectCols int
		expectFrom string
	}{
		{"SELECT * FROM users", 1, "users"},
		{"SELECT name FROM users", 1, "users"},
		{"SELECT name, age FROM users", 2, "users"},
		{"SELECT id, name, age FROM people", 3, "people"},
	}

	for _, tt := range tests {
		l := lexer.New(tt.input)
		p := New(l)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse(%q) error: %v", tt.input, err)
			continue
		}

		sel, ok := stmt.(*SelectStatement)
		if !ok {
			t.Errorf("Parse(%q) expected SelectStatement, got %T", tt.input, stmt)
			continue
		}

		if len(sel.Columns) != tt.expectCols {
			t.Errorf("Parse(%q) expected %d columns, got %d", tt.input, tt.expectCols, len(sel.Columns))
		}

		if sel.From != tt.expectFrom {
			t.Errorf("Parse(%q) expected FROM %q, got %q", tt.input, tt.expectFrom, sel.From)
		}
	}
}

func TestParseSelectWithWhere(t *testing.T) {
	input := "SELECT name FROM users WHERE age > 18"

	l := lexer.New(input)
	p := New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}

	binExpr, ok := sel.Where.(*BinaryExpression)
	if !ok {
		t.Fatalf("expected BinaryExpression in WHERE, got %T", sel.Where)
	}

	if binExpr.Operator != OpGreaterThan {
		t.Errorf("expected > operator, got %v", binExpr.Operator)
	}
}

func TestParseSelectOrderBy(t *testing.T) {
	input := "SELECT * FROM users ORDER BY name DESC, age ASC"

	l := lexer.New(input)
	p := New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if len(sel.OrderBy) != 2 {
		t.Fatalf("expected 2 ORDER BY clauses, got %d", len(sel.OrderBy))
	}

	if sel.OrderBy[0].Column != "name" || !sel.OrderBy[0].Descending {
		t.Errorf("first ORDER BY should be name DESC")
	}

	if sel.OrderBy[1].Column != "age" || sel.OrderBy[1].Descending {
		t.Errorf("second ORDER BY should be age ASC")
	}
}

func TestParseSelectLimit(t *testing.T) {
	input := "SELECT * FROM users LIMIT 10 OFFSET 5"

	l := lexer.New(input)
	p := New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", stmt)
	}

	if sel.Limit == nil || *sel.Limit != 10 {
		t.Errorf("expected LIMIT 10")
	}

	if sel.Offset == nil || *sel.Offset != 5 {
		t.Errorf("expected OFFSET 5")
	}
}

func TestParseInsert(t *testing.T) {
	input := "INSERT INTO users (name, age) VALUES ('Alice', 30)"

	l := lexer.New(input)
	p := New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	ins, ok := stmt.(*InsertStatement)
	if !ok {
		t.Fatalf("expected InsertStatement, got %T", stmt)
	}

	if ins.Table != "users" {
		t.Errorf("expected table users, got %s", ins.Table)
	}

	if len(ins.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(ins.Columns))
	}

	if len(ins.Values) != 2 {
		t.Errorf("expected 2 values, got %d", len(ins.Values))
	}

	// Check first value is string 'Alice'
	strVal, ok := ins.Values[0].(*StringLiteral)
	if !ok {
		t.Errorf("expected StringLiteral, got %T", ins.Values[0])
	} else if strVal.Value != "Alice" {
		t.Errorf("expected 'Alice', got %q", strVal.Value)
	}

	// Check second value is integer 30
	intVal, ok := ins.Values[1].(*IntegerLiteral)
	if !ok {
		t.Errorf("expected IntegerLiteral, got %T", ins.Values[1])
	} else if intVal.Value != 30 {
		t.Errorf("expected 30, got %d", intVal.Value)
	}
}

func TestParseCreateTable(t *testing.T) {
	input := "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"

	l := lexer.New(input)
	p := New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	create, ok := stmt.(*CreateTableStatement)
	if !ok {
		t.Fatalf("expected CreateTableStatement, got %T", stmt)
	}

	if create.Table != "users" {
		t.Errorf("expected table users, got %s", create.Table)
	}

	if len(create.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(create.Columns))
	}

	// Check first column
	if create.Columns[0].Name != "id" {
		t.Errorf("expected column id, got %s", create.Columns[0].Name)
	}
	if create.Columns[0].Type != TypeInteger {
		t.Errorf("expected INTEGER type, got %v", create.Columns[0].Type)
	}
	if !create.Columns[0].PrimaryKey {
		t.Errorf("expected PRIMARY KEY")
	}

	// Check primary key detection
	if create.PrimaryKey != "id" {
		t.Errorf("expected primary key 'id', got %s", create.PrimaryKey)
	}
}

func TestParseUpdate(t *testing.T) {
	input := "UPDATE users SET age = 31 WHERE name = 'Alice'"

	l := lexer.New(input)
	p := New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	upd, ok := stmt.(*UpdateStatement)
	if !ok {
		t.Fatalf("expected UpdateStatement, got %T", stmt)
	}

	if upd.Table != "users" {
		t.Errorf("expected table users, got %s", upd.Table)
	}

	if len(upd.Assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(upd.Assignments))
	}

	if upd.Assignments[0].Column != "age" {
		t.Errorf("expected column age, got %s", upd.Assignments[0].Column)
	}

	if upd.Where == nil {
		t.Error("expected WHERE clause")
	}
}

func TestParseDelete(t *testing.T) {
	input := "DELETE FROM users WHERE age < 18"

	l := lexer.New(input)
	p := New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	del, ok := stmt.(*DeleteStatement)
	if !ok {
		t.Fatalf("expected DeleteStatement, got %T", stmt)
	}

	if del.Table != "users" {
		t.Errorf("expected table users, got %s", del.Table)
	}

	if del.Where == nil {
		t.Error("expected WHERE clause")
	}
}

func TestParseDropTable(t *testing.T) {
	input := "DROP TABLE users"

	l := lexer.New(input)
	p := New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	drop, ok := stmt.(*DropTableStatement)
	if !ok {
		t.Fatalf("expected DropTableStatement, got %T", stmt)
	}

	if drop.Table != "users" {
		t.Errorf("expected table users, got %s", drop.Table)
	}
}

func TestParseExpressionPrecedence(t *testing.T) {
	// Test that 1 + 2 * 3 is parsed as 1 + (2 * 3)
	input := "SELECT * FROM t WHERE x = 1 + 2 * 3"

	l := lexer.New(input)
	p := New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	binExpr := sel.Where.(*BinaryExpression)

	// The top-level should be = comparison
	if binExpr.Operator != OpEquals {
		t.Errorf("expected = at top level, got %v", binExpr.Operator)
	}

	// Right side should be + with * nested
	addExpr, ok := binExpr.Right.(*BinaryExpression)
	if !ok {
		t.Fatalf("expected BinaryExpression on right, got %T", binExpr.Right)
	}

	if addExpr.Operator != OpAdd {
		t.Errorf("expected + operator, got %v", addExpr.Operator)
	}

	mulExpr, ok := addExpr.Right.(*BinaryExpression)
	if !ok {
		t.Fatalf("expected BinaryExpression for *, got %T", addExpr.Right)
	}

	if mulExpr.Operator != OpMultiply {
		t.Errorf("expected * operator, got %v", mulExpr.Operator)
	}
}

func TestParseComplexWhere(t *testing.T) {
	input := "SELECT * FROM users WHERE age >= 18 AND (name = 'Alice' OR name = 'Bob')"

	l := lexer.New(input)
	p := New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}

	// Should parse as AND at the top level
	andExpr, ok := sel.Where.(*BinaryExpression)
	if !ok {
		t.Fatalf("expected BinaryExpression, got %T", sel.Where)
	}

	if andExpr.Operator != OpAnd {
		t.Errorf("expected AND at top level, got %v", andExpr.Operator)
	}
}
