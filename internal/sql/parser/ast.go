// Package parser implements a SQL parser that builds an Abstract Syntax Tree (AST).
//
// EDUCATIONAL NOTES:
// ------------------
// An Abstract Syntax Tree (AST) is a tree representation of the structure
// of source code. Each node in the tree represents a construct in the code.
//
// For example, the SQL:
//   SELECT name, age FROM users WHERE age > 18
//
// Becomes an AST like:
//   SelectStatement
//   ├── Columns: [name, age]
//   ├── From: users
//   └── Where: BinaryExpr(age > 18)
//
// The AST is independent of the original text syntax and is much easier
// to work with when executing queries.

package parser

import (
	"fmt"
)

// Node is the base interface for all AST nodes.
type Node interface {
	node()
	String() string
}

// Statement represents a SQL statement.
type Statement interface {
	Node
	statement()
}

// Expression represents an expression that can be evaluated.
type Expression interface {
	Node
	expression()
}

// ============================================================================
// Statements
// ============================================================================

// SelectStatement represents a SELECT query.
//
// Example: SELECT name, age FROM users WHERE age > 18 ORDER BY name LIMIT 10
type SelectStatement struct {
	Columns  []Expression    // Columns to select (* means all)
	From     string          // Table name
	Where    Expression      // Optional WHERE clause
	OrderBy  []OrderByClause // Optional ORDER BY clause
	Limit    *int            // Optional LIMIT
	Offset   *int            // Optional OFFSET
}

func (s *SelectStatement) node()      {}
func (s *SelectStatement) statement() {}
func (s *SelectStatement) String() string {
	return fmt.Sprintf("SELECT %v FROM %s", s.Columns, s.From)
}

// OrderByClause represents a single ORDER BY item.
type OrderByClause struct {
	Column     string
	Descending bool
}

// InsertStatement represents an INSERT query.
//
// Example: INSERT INTO users (name, age) VALUES ('Alice', 30)
type InsertStatement struct {
	Table   string
	Columns []string
	Values  []Expression
}

func (s *InsertStatement) node()      {}
func (s *InsertStatement) statement() {}
func (s *InsertStatement) String() string {
	return fmt.Sprintf("INSERT INTO %s (%v) VALUES (%v)", s.Table, s.Columns, s.Values)
}

// UpdateStatement represents an UPDATE query.
//
// Example: UPDATE users SET age = 31 WHERE name = 'Alice'
type UpdateStatement struct {
	Table       string
	Assignments []Assignment
	Where       Expression
}

func (s *UpdateStatement) node()      {}
func (s *UpdateStatement) statement() {}
func (s *UpdateStatement) String() string {
	return fmt.Sprintf("UPDATE %s SET %v", s.Table, s.Assignments)
}

// Assignment represents a column = value assignment in UPDATE.
type Assignment struct {
	Column string
	Value  Expression
}

// DeleteStatement represents a DELETE query.
//
// Example: DELETE FROM users WHERE age < 18
type DeleteStatement struct {
	Table string
	Where Expression
}

func (s *DeleteStatement) node()      {}
func (s *DeleteStatement) statement() {}
func (s *DeleteStatement) String() string {
	return fmt.Sprintf("DELETE FROM %s", s.Table)
}

// CreateTableStatement represents a CREATE TABLE query.
//
// Example: CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)
type CreateTableStatement struct {
	Table      string
	Columns    []ColumnDefinition
	PrimaryKey string
}

func (s *CreateTableStatement) node()      {}
func (s *CreateTableStatement) statement() {}
func (s *CreateTableStatement) String() string {
	return fmt.Sprintf("CREATE TABLE %s (%v)", s.Table, s.Columns)
}

// ColumnDefinition represents a column definition in CREATE TABLE.
type ColumnDefinition struct {
	Name       string
	Type       DataType
	PrimaryKey bool
	NotNull    bool
}

func (c ColumnDefinition) String() string {
	s := fmt.Sprintf("%s %s", c.Name, c.Type)
	if c.PrimaryKey {
		s += " PRIMARY KEY"
	}
	if c.NotNull {
		s += " NOT NULL"
	}
	return s
}

// DataType represents a SQL data type.
type DataType int

const (
	TypeUnknown DataType = iota
	TypeInteger
	TypeReal
	TypeText
	TypeBoolean
)

func (d DataType) String() string {
	switch d {
	case TypeInteger:
		return "INTEGER"
	case TypeReal:
		return "REAL"
	case TypeText:
		return "TEXT"
	case TypeBoolean:
		return "BOOLEAN"
	default:
		return "UNKNOWN"
	}
}

// DropTableStatement represents a DROP TABLE query.
type DropTableStatement struct {
	Table string
}

func (s *DropTableStatement) node()      {}
func (s *DropTableStatement) statement() {}
func (s *DropTableStatement) String() string {
	return fmt.Sprintf("DROP TABLE %s", s.Table)
}

// ============================================================================
// Expressions
// ============================================================================

// Identifier represents a column or table name.
type Identifier struct {
	Name string
}

func (e *Identifier) node()       {}
func (e *Identifier) expression() {}
func (e *Identifier) String() string {
	return e.Name
}

// IntegerLiteral represents an integer value.
type IntegerLiteral struct {
	Value int64
}

func (e *IntegerLiteral) node()       {}
func (e *IntegerLiteral) expression() {}
func (e *IntegerLiteral) String() string {
	return fmt.Sprintf("%d", e.Value)
}

// RealLiteral represents a floating-point value.
type RealLiteral struct {
	Value float64
}

func (e *RealLiteral) node()       {}
func (e *RealLiteral) expression() {}
func (e *RealLiteral) String() string {
	return fmt.Sprintf("%f", e.Value)
}

// StringLiteral represents a string value.
type StringLiteral struct {
	Value string
}

func (e *StringLiteral) node()       {}
func (e *StringLiteral) expression() {}
func (e *StringLiteral) String() string {
	return fmt.Sprintf("'%s'", e.Value)
}

// BooleanLiteral represents a boolean value.
type BooleanLiteral struct {
	Value bool
}

func (e *BooleanLiteral) node()       {}
func (e *BooleanLiteral) expression() {}
func (e *BooleanLiteral) String() string {
	if e.Value {
		return "TRUE"
	}
	return "FALSE"
}

// NullLiteral represents a NULL value.
type NullLiteral struct{}

func (e *NullLiteral) node()       {}
func (e *NullLiteral) expression() {}
func (e *NullLiteral) String() string {
	return "NULL"
}

// StarExpression represents * (all columns).
type StarExpression struct{}

func (e *StarExpression) node()       {}
func (e *StarExpression) expression() {}
func (e *StarExpression) String() string {
	return "*"
}

// BinaryExpression represents a binary operation (e.g., a = b, a + b).
//
// EDUCATIONAL NOTE:
// -----------------
// Binary expressions have two operands and an operator.
// They can be:
// - Comparison: =, !=, <, >, <=, >=
// - Logical: AND, OR
// - Arithmetic: +, -, *, /
type BinaryExpression struct {
	Left     Expression
	Operator BinaryOp
	Right    Expression
}

func (e *BinaryExpression) node()       {}
func (e *BinaryExpression) expression() {}
func (e *BinaryExpression) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left, e.Operator, e.Right)
}

// BinaryOp represents a binary operator.
type BinaryOp int

const (
	OpUnknown BinaryOp = iota
	// Comparison operators
	OpEquals
	OpNotEquals
	OpLessThan
	OpGreaterThan
	OpLessOrEqual
	OpGreaterOrEqual
	// Logical operators
	OpAnd
	OpOr
	// Arithmetic operators
	OpAdd
	OpSubtract
	OpMultiply
	OpDivide
)

func (op BinaryOp) String() string {
	switch op {
	case OpEquals:
		return "="
	case OpNotEquals:
		return "!="
	case OpLessThan:
		return "<"
	case OpGreaterThan:
		return ">"
	case OpLessOrEqual:
		return "<="
	case OpGreaterOrEqual:
		return ">="
	case OpAnd:
		return "AND"
	case OpOr:
		return "OR"
	case OpAdd:
		return "+"
	case OpSubtract:
		return "-"
	case OpMultiply:
		return "*"
	case OpDivide:
		return "/"
	default:
		return "?"
	}
}

// UnaryExpression represents a unary operation (e.g., NOT x, -5).
type UnaryExpression struct {
	Operator UnaryOp
	Operand  Expression
}

func (e *UnaryExpression) node()       {}
func (e *UnaryExpression) expression() {}
func (e *UnaryExpression) String() string {
	return fmt.Sprintf("(%s %s)", e.Operator, e.Operand)
}

// UnaryOp represents a unary operator.
type UnaryOp int

const (
	UnaryOpNot UnaryOp = iota
	UnaryOpNegate
)

func (op UnaryOp) String() string {
	switch op {
	case UnaryOpNot:
		return "NOT"
	case UnaryOpNegate:
		return "-"
	default:
		return "?"
	}
}
