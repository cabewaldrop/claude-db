// Package parser - SQL Parser implementation
//
// EDUCATIONAL NOTES:
// ------------------
// A parser reads tokens from the lexer and builds an Abstract Syntax Tree (AST).
// This is the second phase of compilation/interpretation, after lexing.
//
// We use a "recursive descent" parser, which is one of the simplest and most
// intuitive parsing techniques. Each grammar rule becomes a function:
// - parseStatement() handles SELECT, INSERT, UPDATE, etc.
// - parseExpression() handles expressions with proper operator precedence
// - parseSelectStatement() handles the SELECT grammar specifically
//
// The parser maintains a "current token" and can "peek" at the next token.
// This allows it to make decisions about what to parse next.

package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cabewaldrop/claude-db/internal/sql/lexer"
)

// Parser parses SQL tokens into an AST.
type Parser struct {
	lexer     *lexer.Lexer
	curToken  lexer.Token
	peekToken lexer.Token
	errors    []string
}

// New creates a new Parser for the given lexer.
func New(l *lexer.Lexer) *Parser {
	p := &Parser{
		lexer:  l,
		errors: []string{},
	}
	// Read two tokens to initialize curToken and peekToken
	p.nextToken()
	p.nextToken()
	return p
}

// Parse parses the input and returns the AST.
func (p *Parser) Parse() (Statement, error) {
	stmt := p.parseStatement()
	if len(p.errors) > 0 {
		return nil, fmt.Errorf("parse errors: %s", strings.Join(p.errors, "; "))
	}
	return stmt, nil
}

// Errors returns any parsing errors encountered.
func (p *Parser) Errors() []string {
	return p.errors
}

// nextToken advances to the next token.
func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

// curTokenIs checks if the current token is of the given type.
func (p *Parser) curTokenIs(t lexer.TokenType) bool {
	return p.curToken.Type == t
}

// peekTokenIs checks if the next token is of the given type.
func (p *Parser) peekTokenIs(t lexer.TokenType) bool {
	return p.peekToken.Type == t
}

// expectPeek advances if the next token is of the expected type.
func (p *Parser) expectPeek(t lexer.TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	}
	p.peekError(t)
	return false
}

// peekError records an error for unexpected token type.
func (p *Parser) peekError(t lexer.TokenType) {
	msg := fmt.Sprintf("expected next token to be %d, got %d instead (literal: %q)",
		t, p.peekToken.Type, p.peekToken.Literal)
	p.errors = append(p.errors, msg)
}

// parseStatement parses a SQL statement.
//
// EDUCATIONAL NOTE:
// -----------------
// This is the entry point for parsing. We look at the first token
// to determine what kind of statement we're parsing.
func (p *Parser) parseStatement() Statement {
	switch p.curToken.Type {
	case lexer.TokenSelect:
		return p.parseSelectStatement()
	case lexer.TokenInsert:
		return p.parseInsertStatement()
	case lexer.TokenUpdate:
		return p.parseUpdateStatement()
	case lexer.TokenDelete:
		return p.parseDeleteStatement()
	case lexer.TokenCreate:
		return p.parseCreateStatement()
	case lexer.TokenDrop:
		return p.parseDropStatement()
	case lexer.TokenExplain:
		return p.parseExplainStatement()
	case lexer.TokenAnalyze:
		return p.parseAnalyzeStatement()
	default:
		p.errors = append(p.errors, fmt.Sprintf("unexpected token: %s", p.curToken.Literal))
		return nil
	}
}

// parseSelectStatement parses: SELECT columns FROM table [WHERE condition] [ORDER BY ...] [LIMIT n]
func (p *Parser) parseSelectStatement() *SelectStatement {
	stmt := &SelectStatement{}

	// Parse column list
	p.nextToken() // move past SELECT
	stmt.Columns = p.parseExpressionList()

	// Expect FROM
	if !p.expectPeek(lexer.TokenFrom) {
		return nil
	}

	// Parse table name
	if !p.expectPeek(lexer.TokenIdent) {
		return nil
	}
	stmt.From = p.curToken.Literal

	// Optional WHERE clause
	if p.peekTokenIs(lexer.TokenWhere) {
		p.nextToken() // move to WHERE
		p.nextToken() // move past WHERE
		stmt.Where = p.parseExpression(PrecedenceLowest)
	}

	// Optional ORDER BY clause
	if p.peekTokenIs(lexer.TokenOrder) {
		p.nextToken() // move to ORDER
		if !p.expectPeek(lexer.TokenBy) {
			return nil
		}
		stmt.OrderBy = p.parseOrderByClause()
	}

	// Optional LIMIT clause
	if p.peekTokenIs(lexer.TokenLimit) {
		p.nextToken() // move to LIMIT
		p.nextToken() // move past LIMIT
		limit, err := strconv.Atoi(p.curToken.Literal)
		if err != nil {
			p.errors = append(p.errors, "LIMIT must be an integer")
			return nil
		}
		stmt.Limit = &limit

		// Optional OFFSET clause
		if p.peekTokenIs(lexer.TokenOffset) {
			p.nextToken() // move to OFFSET
			p.nextToken() // move past OFFSET
			offset, err := strconv.Atoi(p.curToken.Literal)
			if err != nil {
				p.errors = append(p.errors, "OFFSET must be an integer")
				return nil
			}
			stmt.Offset = &offset
		}
	}

	return stmt
}

// parseOrderByClause parses: ORDER BY column [ASC|DESC], ...
func (p *Parser) parseOrderByClause() []OrderByClause {
	var clauses []OrderByClause

	for {
		p.nextToken()
		if !p.curTokenIs(lexer.TokenIdent) {
			p.errors = append(p.errors, "expected column name in ORDER BY")
			return nil
		}

		clause := OrderByClause{
			Column:     p.curToken.Literal,
			Descending: false,
		}

		// Check for ASC or DESC
		if p.peekTokenIs(lexer.TokenAsc) {
			p.nextToken()
		} else if p.peekTokenIs(lexer.TokenDesc) {
			p.nextToken()
			clause.Descending = true
		}

		clauses = append(clauses, clause)

		// Check for comma (more columns)
		if !p.peekTokenIs(lexer.TokenComma) {
			break
		}
		p.nextToken() // consume comma
	}

	return clauses
}

// parseInsertStatement parses: INSERT INTO table (columns) VALUES (values)
func (p *Parser) parseInsertStatement() *InsertStatement {
	stmt := &InsertStatement{}

	// Expect INTO
	if !p.expectPeek(lexer.TokenInto) {
		return nil
	}

	// Parse table name
	if !p.expectPeek(lexer.TokenIdent) {
		return nil
	}
	stmt.Table = p.curToken.Literal

	// Parse column list (optional)
	if p.peekTokenIs(lexer.TokenLeftParen) {
		p.nextToken() // move to (
		stmt.Columns = p.parseIdentifierList()
		if !p.expectPeek(lexer.TokenRightParen) {
			return nil
		}
	}

	// Expect VALUES
	if !p.expectPeek(lexer.TokenValues) {
		return nil
	}

	// Parse values
	if !p.expectPeek(lexer.TokenLeftParen) {
		return nil
	}
	p.nextToken() // move past (
	stmt.Values = p.parseExpressionList()
	if !p.expectPeek(lexer.TokenRightParen) {
		return nil
	}

	return stmt
}

// parseUpdateStatement parses: UPDATE table SET column = value, ... [WHERE condition]
func (p *Parser) parseUpdateStatement() *UpdateStatement {
	stmt := &UpdateStatement{}

	// Parse table name
	if !p.expectPeek(lexer.TokenIdent) {
		return nil
	}
	stmt.Table = p.curToken.Literal

	// Expect SET
	if !p.expectPeek(lexer.TokenSet) {
		return nil
	}

	// Parse assignments
	stmt.Assignments = p.parseAssignmentList()

	// Optional WHERE clause
	if p.peekTokenIs(lexer.TokenWhere) {
		p.nextToken() // move to WHERE
		p.nextToken() // move past WHERE
		stmt.Where = p.parseExpression(PrecedenceLowest)
	}

	return stmt
}

// parseAssignmentList parses: column = value, column = value, ...
func (p *Parser) parseAssignmentList() []Assignment {
	var assignments []Assignment

	for {
		p.nextToken()
		if !p.curTokenIs(lexer.TokenIdent) {
			p.errors = append(p.errors, "expected column name")
			return nil
		}
		column := p.curToken.Literal

		if !p.expectPeek(lexer.TokenEquals) {
			return nil
		}

		p.nextToken()
		value := p.parseExpression(PrecedenceLowest)

		assignments = append(assignments, Assignment{
			Column: column,
			Value:  value,
		})

		if !p.peekTokenIs(lexer.TokenComma) {
			break
		}
		p.nextToken() // consume comma
	}

	return assignments
}

// parseDeleteStatement parses: DELETE FROM table [WHERE condition]
func (p *Parser) parseDeleteStatement() *DeleteStatement {
	stmt := &DeleteStatement{}

	// Expect FROM
	if !p.expectPeek(lexer.TokenFrom) {
		return nil
	}

	// Parse table name
	if !p.expectPeek(lexer.TokenIdent) {
		return nil
	}
	stmt.Table = p.curToken.Literal

	// Optional WHERE clause
	if p.peekTokenIs(lexer.TokenWhere) {
		p.nextToken() // move to WHERE
		p.nextToken() // move past WHERE
		stmt.Where = p.parseExpression(PrecedenceLowest)
	}

	return stmt
}

// parseCreateStatement parses CREATE TABLE ...
func (p *Parser) parseCreateStatement() Statement {
	// Check for UNIQUE INDEX or INDEX or TABLE
	if p.peekTokenIs(lexer.TokenUnique) {
		p.nextToken() // move to UNIQUE
		if !p.expectPeek(lexer.TokenIndex) {
			return nil
		}
		return p.parseCreateIndexStatement(true)
	}

	if p.peekTokenIs(lexer.TokenIndex) {
		p.nextToken() // move to INDEX
		return p.parseCreateIndexStatement(false)
	}

	// Expect TABLE
	if !p.expectPeek(lexer.TokenTable) {
		return nil
	}

	return p.parseCreateTableStatement()
}

// parseCreateTableStatement parses: CREATE TABLE name (column_definitions)
func (p *Parser) parseCreateTableStatement() *CreateTableStatement {
	stmt := &CreateTableStatement{}

	// Parse table name
	if !p.expectPeek(lexer.TokenIdent) {
		return nil
	}
	stmt.Table = p.curToken.Literal

	// Expect (
	if !p.expectPeek(lexer.TokenLeftParen) {
		return nil
	}

	// Parse column definitions
	stmt.Columns = p.parseColumnDefinitions()

	// Expect )
	if !p.expectPeek(lexer.TokenRightParen) {
		return nil
	}

	// Find primary key
	for _, col := range stmt.Columns {
		if col.PrimaryKey {
			stmt.PrimaryKey = col.Name
			break
		}
	}

	return stmt
}

// parseColumnDefinitions parses column definitions in CREATE TABLE.
func (p *Parser) parseColumnDefinitions() []ColumnDefinition {
	var columns []ColumnDefinition

	for {
		p.nextToken()
		if !p.curTokenIs(lexer.TokenIdent) {
			p.errors = append(p.errors, "expected column name")
			return nil
		}

		col := ColumnDefinition{
			Name: p.curToken.Literal,
		}

		// Parse data type
		p.nextToken()
		col.Type = p.parseDataType()

		// Check for PRIMARY KEY
		if p.peekTokenIs(lexer.TokenPrimaryKey) {
			p.nextToken()
			// Check for KEY after PRIMARY
			if p.peekTokenIs(lexer.TokenIdent) && strings.ToUpper(p.peekToken.Literal) == "KEY" {
				p.nextToken()
			}
			col.PrimaryKey = true
		}

		// Check for NOT NULL
		if p.peekTokenIs(lexer.TokenNot) {
			p.nextToken()
			if p.peekTokenIs(lexer.TokenNull) {
				p.nextToken()
				col.NotNull = true
			}
		}

		columns = append(columns, col)

		// Check for comma or end
		if !p.peekTokenIs(lexer.TokenComma) {
			break
		}
		p.nextToken() // consume comma
	}

	return columns
}

// parseDataType parses a SQL data type.
func (p *Parser) parseDataType() DataType {
	switch p.curToken.Type {
	case lexer.TokenInt, lexer.TokenInteger:
		return TypeInteger
	case lexer.TokenReal:
		return TypeReal
	case lexer.TokenText, lexer.TokenVarchar:
		// Handle VARCHAR(n)
		if p.peekTokenIs(lexer.TokenLeftParen) {
			p.nextToken() // (
			p.nextToken() // size
			p.nextToken() // )
		}
		return TypeText
	case lexer.TokenBool:
		return TypeBoolean
	default:
		// Try to match common type names as identifiers
		switch strings.ToUpper(p.curToken.Literal) {
		case "INT", "INTEGER":
			return TypeInteger
		case "REAL", "FLOAT", "DOUBLE":
			return TypeReal
		case "TEXT", "VARCHAR", "STRING":
			return TypeText
		case "BOOL", "BOOLEAN":
			return TypeBoolean
		default:
			p.errors = append(p.errors, fmt.Sprintf("unknown data type: %s", p.curToken.Literal))
			return TypeUnknown
		}
	}
}

// parseCreateIndexStatement parses: CREATE [UNIQUE] INDEX name ON table (columns)
func (p *Parser) parseCreateIndexStatement(unique bool) *CreateIndexStatement {
	stmt := &CreateIndexStatement{
		Unique: unique,
	}

	// Parse index name
	if !p.expectPeek(lexer.TokenIdent) {
		return nil
	}
	stmt.IndexName = p.curToken.Literal

	// Expect ON
	if !p.expectPeek(lexer.TokenOn) {
		return nil
	}

	// Parse table name
	if !p.expectPeek(lexer.TokenIdent) {
		return nil
	}
	stmt.Table = p.curToken.Literal

	// Expect (
	if !p.expectPeek(lexer.TokenLeftParen) {
		return nil
	}

	// Parse column names
	stmt.Columns = p.parseIndexColumnList()

	// Expect )
	if !p.expectPeek(lexer.TokenRightParen) {
		return nil
	}

	return stmt
}

// parseIndexColumnList parses a list of column names for an index.
func (p *Parser) parseIndexColumnList() []string {
	var columns []string

	for {
		if !p.expectPeek(lexer.TokenIdent) {
			return nil
		}
		columns = append(columns, p.curToken.Literal)

		// Check for comma or end
		if !p.peekTokenIs(lexer.TokenComma) {
			break
		}
		p.nextToken() // consume comma
	}

	return columns
}

// parseDropStatement parses DROP TABLE ... or DROP INDEX ...
func (p *Parser) parseDropStatement() Statement {
	// Check for INDEX
	if p.peekTokenIs(lexer.TokenIndex) {
		p.nextToken() // move to INDEX
		return p.parseDropIndexStatement()
	}

	// Expect TABLE
	if !p.expectPeek(lexer.TokenTable) {
		return nil
	}

	stmt := &DropTableStatement{}

	// Parse table name
	if !p.expectPeek(lexer.TokenIdent) {
		return nil
	}
	stmt.Table = p.curToken.Literal

	return stmt
}

// parseExplainStatement parses: EXPLAIN <statement>
//
// EDUCATIONAL NOTE:
// -----------------
// EXPLAIN shows the query plan without executing the query.
// It wraps another statement (SELECT, UPDATE, DELETE) to explain.
func (p *Parser) parseExplainStatement() *ExplainStatement {
	stmt := &ExplainStatement{}

	// Move past EXPLAIN
	p.nextToken()

	// Parse the inner statement
	inner := p.parseStatement()
	if inner == nil {
		return nil
	}
	stmt.Statement = inner

	return stmt
}

// parseDropIndexStatement parses: DROP INDEX name
func (p *Parser) parseDropIndexStatement() *DropIndexStatement {
	stmt := &DropIndexStatement{}

	// Parse index name
	if !p.expectPeek(lexer.TokenIdent) {
		return nil
	}
	stmt.IndexName = p.curToken.Literal

	return stmt
}

// parseAnalyzeStatement parses: ANALYZE [tablename]
//
// EDUCATIONAL NOTE:
// -----------------
// ANALYZE refreshes table statistics used by the query planner.
// Without a table name, it analyzes all tables.
func (p *Parser) parseAnalyzeStatement() Statement {
	stmt := &AnalyzeStatement{}

	// Optional table name
	if p.peekTokenIs(lexer.TokenIdent) {
		p.nextToken()
		stmt.Table = p.curToken.Literal
	}

	return stmt
}
// parseIdentifierList parses: ident, ident, ident
func (p *Parser) parseIdentifierList() []string {
	var identifiers []string

	p.nextToken() // move past (
	for !p.curTokenIs(lexer.TokenRightParen) && !p.curTokenIs(lexer.TokenEOF) {
		if p.curTokenIs(lexer.TokenIdent) {
			identifiers = append(identifiers, p.curToken.Literal)
		}
		if p.peekTokenIs(lexer.TokenComma) {
			p.nextToken() // move to comma
			p.nextToken() // move past comma
		} else {
			break
		}
	}

	return identifiers
}

// parseExpressionList parses a comma-separated list of expressions.
func (p *Parser) parseExpressionList() []Expression {
	var expressions []Expression

	// Handle * for SELECT *
	if p.curTokenIs(lexer.TokenAsterisk) {
		expressions = append(expressions, &StarExpression{})
		return expressions
	}

	for {
		expr := p.parseExpression(PrecedenceLowest)
		if expr != nil {
			expressions = append(expressions, expr)
		}

		if !p.peekTokenIs(lexer.TokenComma) {
			break
		}
		p.nextToken() // move to comma
		p.nextToken() // move past comma
	}

	return expressions
}

// ============================================================================
// Expression Parsing with Operator Precedence
// ============================================================================

// EDUCATIONAL NOTE:
// -----------------
// Operator precedence determines which operators bind more tightly.
// For example, in "1 + 2 * 3", multiplication has higher precedence,
// so it's parsed as "1 + (2 * 3)" = 7, not "(1 + 2) * 3" = 9.
//
// We use Pratt parsing (top-down operator precedence parsing):
// - Each precedence level is a number
// - Higher numbers bind more tightly
// - We recursively parse expressions, only consuming operators
//   that have precedence >= the current level

// Precedence levels
const (
	PrecedenceLowest = iota
	PrecedenceOr          // OR
	PrecedenceAnd         // AND
	PrecedenceNot         // NOT
	PrecedenceComparison  // =, !=, <, >, <=, >=
	PrecedenceAddSub      // +, -
	PrecedenceMulDiv      // *, /
	PrecedenceUnary       // -x, NOT x
	PrecedenceHighest
)

// precedences maps token types to their precedence levels.
var precedences = map[lexer.TokenType]int{
	lexer.TokenOr:             PrecedenceOr,
	lexer.TokenAnd:            PrecedenceAnd,
	lexer.TokenEquals:         PrecedenceComparison,
	lexer.TokenNotEquals:      PrecedenceComparison,
	lexer.TokenLessThan:       PrecedenceComparison,
	lexer.TokenGreaterThan:    PrecedenceComparison,
	lexer.TokenLessOrEqual:    PrecedenceComparison,
	lexer.TokenGreaterOrEqual: PrecedenceComparison,
	lexer.TokenPlus:           PrecedenceAddSub,
	lexer.TokenMinus:          PrecedenceAddSub,
	lexer.TokenAsterisk:       PrecedenceMulDiv,
	lexer.TokenSlash:          PrecedenceMulDiv,
}

// peekPrecedence returns the precedence of the next token.
func (p *Parser) peekPrecedence() int {
	if prec, ok := precedences[p.peekToken.Type]; ok {
		return prec
	}
	return PrecedenceLowest
}

// curPrecedence returns the precedence of the current token.
func (p *Parser) curPrecedence() int {
	if prec, ok := precedences[p.curToken.Type]; ok {
		return prec
	}
	return PrecedenceLowest
}

// parseExpression parses an expression using Pratt parsing.
func (p *Parser) parseExpression(precedence int) Expression {
	// Parse prefix expression (literal, identifier, unary op, etc.)
	left := p.parsePrefixExpression()
	if left == nil {
		return nil
	}

	// Parse infix expressions while we see operators with higher precedence
	for !p.peekTokenIs(lexer.TokenEOF) && precedence < p.peekPrecedence() {
		// Check if next token is an infix operator
		if _, ok := precedences[p.peekToken.Type]; !ok {
			return left
		}

		p.nextToken()
		left = p.parseInfixExpression(left)
	}

	return left
}

// parsePrefixExpression parses prefix expressions (literals, identifiers, unary ops).
func (p *Parser) parsePrefixExpression() Expression {
	switch p.curToken.Type {
	case lexer.TokenIdent:
		return &Identifier{Name: p.curToken.Literal}

	case lexer.TokenNumber:
		return p.parseNumberLiteral()

	case lexer.TokenString:
		return &StringLiteral{Value: p.curToken.Literal}

	case lexer.TokenBoolean:
		return &BooleanLiteral{Value: strings.ToUpper(p.curToken.Literal) == "TRUE"}

	case lexer.TokenNull:
		return &NullLiteral{}

	case lexer.TokenAsterisk:
		return &StarExpression{}

	case lexer.TokenMinus:
		return p.parseUnaryExpression(UnaryOpNegate)

	case lexer.TokenNot:
		return p.parseUnaryExpression(UnaryOpNot)

	case lexer.TokenLeftParen:
		return p.parseGroupedExpression()

	default:
		return nil
	}
}

// parseNumberLiteral parses an integer or real literal.
func (p *Parser) parseNumberLiteral() Expression {
	literal := p.curToken.Literal

	// Try integer first
	if intVal, err := strconv.ParseInt(literal, 10, 64); err == nil {
		return &IntegerLiteral{Value: intVal}
	}

	// Try float
	if floatVal, err := strconv.ParseFloat(literal, 64); err == nil {
		return &RealLiteral{Value: floatVal}
	}

	p.errors = append(p.errors, fmt.Sprintf("could not parse %q as number", literal))
	return nil
}

// parseUnaryExpression parses unary expressions (NOT x, -x).
func (p *Parser) parseUnaryExpression(op UnaryOp) Expression {
	p.nextToken()
	operand := p.parseExpression(PrecedenceUnary)
	return &UnaryExpression{
		Operator: op,
		Operand:  operand,
	}
}

// parseGroupedExpression parses expressions in parentheses.
func (p *Parser) parseGroupedExpression() Expression {
	p.nextToken() // consume (
	expr := p.parseExpression(PrecedenceLowest)
	if !p.expectPeek(lexer.TokenRightParen) {
		return nil
	}
	return expr
}

// parseInfixExpression parses binary expressions (a + b, a = b, etc.).
func (p *Parser) parseInfixExpression(left Expression) Expression {
	expr := &BinaryExpression{
		Left:     left,
		Operator: p.tokenToOperator(p.curToken.Type),
	}

	precedence := p.curPrecedence()
	p.nextToken()
	expr.Right = p.parseExpression(precedence)

	return expr
}

// tokenToOperator converts a token type to a binary operator.
func (p *Parser) tokenToOperator(t lexer.TokenType) BinaryOp {
	switch t {
	case lexer.TokenEquals:
		return OpEquals
	case lexer.TokenNotEquals:
		return OpNotEquals
	case lexer.TokenLessThan:
		return OpLessThan
	case lexer.TokenGreaterThan:
		return OpGreaterThan
	case lexer.TokenLessOrEqual:
		return OpLessOrEqual
	case lexer.TokenGreaterOrEqual:
		return OpGreaterOrEqual
	case lexer.TokenAnd:
		return OpAnd
	case lexer.TokenOr:
		return OpOr
	case lexer.TokenPlus:
		return OpAdd
	case lexer.TokenMinus:
		return OpSubtract
	case lexer.TokenAsterisk:
		return OpMultiply
	case lexer.TokenSlash:
		return OpDivide
	default:
		return OpUnknown
	}
}
