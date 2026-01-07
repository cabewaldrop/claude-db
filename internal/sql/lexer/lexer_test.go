package lexer

import (
	"testing"
)

func TestLexerBasicTokens(t *testing.T) {
	input := "SELECT * FROM users"

	l := New(input)
	tokens := l.Tokenize()

	expected := []struct {
		tokenType TokenType
		literal   string
	}{
		{TokenSelect, "SELECT"},
		{TokenAsterisk, "*"},
		{TokenFrom, "FROM"},
		{TokenIdent, "users"},
		{TokenEOF, ""},
	}

	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}

	for i, exp := range expected {
		if tokens[i].Type != exp.tokenType {
			t.Errorf("token %d: expected type %d, got %d", i, exp.tokenType, tokens[i].Type)
		}
		if tokens[i].Literal != exp.literal {
			t.Errorf("token %d: expected literal %q, got %q", i, exp.literal, tokens[i].Literal)
		}
	}
}

func TestLexerComplexQuery(t *testing.T) {
	input := "SELECT name, age FROM users WHERE age >= 18 AND name != 'admin'"

	l := New(input)
	tokens := l.Tokenize()

	expected := []TokenType{
		TokenSelect,
		TokenIdent,  // name
		TokenComma,
		TokenIdent,  // age
		TokenFrom,
		TokenIdent,  // users
		TokenWhere,
		TokenIdent,  // age
		TokenGreaterOrEqual,
		TokenNumber, // 18
		TokenAnd,
		TokenIdent,  // name
		TokenNotEquals,
		TokenString, // 'admin'
		TokenEOF,
	}

	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}

	for i, exp := range expected {
		if tokens[i].Type != exp {
			t.Errorf("token %d: expected type %d, got %d (literal: %q)",
				i, exp, tokens[i].Type, tokens[i].Literal)
		}
	}
}

func TestLexerCreateTable(t *testing.T) {
	input := "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"

	l := New(input)
	tokens := l.Tokenize()

	expectedTypes := []TokenType{
		TokenCreate,
		TokenTable,
		TokenIdent, // users
		TokenLeftParen,
		TokenIdent,   // id
		TokenInteger, // INTEGER
		TokenPrimaryKey,
		TokenIdent,      // KEY (as part of PRIMARY KEY)
		TokenComma,
		TokenIdent, // name
		TokenText,
		TokenRightParen,
		TokenEOF,
	}

	if len(tokens) != len(expectedTypes) {
		t.Fatalf("expected %d tokens, got %d", len(expectedTypes), len(tokens))
		for i, tok := range tokens {
			t.Logf("  %d: %v", i, tok)
		}
	}
}

func TestLexerInsert(t *testing.T) {
	input := "INSERT INTO users (name, age) VALUES ('Alice', 30)"

	l := New(input)
	tokens := l.Tokenize()

	expectedTypes := []TokenType{
		TokenInsert,
		TokenInto,
		TokenIdent, // users
		TokenLeftParen,
		TokenIdent, // name
		TokenComma,
		TokenIdent, // age
		TokenRightParen,
		TokenValues,
		TokenLeftParen,
		TokenString, // 'Alice'
		TokenComma,
		TokenNumber, // 30
		TokenRightParen,
		TokenEOF,
	}

	if len(tokens) != len(expectedTypes) {
		t.Fatalf("expected %d tokens, got %d", len(expectedTypes), len(tokens))
	}

	for i, exp := range expectedTypes {
		if tokens[i].Type != exp {
			t.Errorf("token %d: expected type %d, got %d (literal: %q)",
				i, exp, tokens[i].Type, tokens[i].Literal)
		}
	}
}

func TestLexerNumbers(t *testing.T) {
	tests := []struct {
		input   string
		literal string
	}{
		{"123", "123"},
		{"45.67", "45.67"},
		{"-42", "-42"},
		{"0.5", "0.5"},
	}

	for _, tt := range tests {
		l := New(tt.input)
		tok := l.NextToken()
		if tok.Type != TokenNumber && tok.Type != TokenMinus {
			t.Errorf("expected NUMBER or MINUS for %q, got %d", tt.input, tok.Type)
		}
	}
}

func TestLexerStrings(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"'hello'", "hello"},
		{"'world'", "world"},
		{"'it''s'", "it's"}, // Escaped quote
	}

	for _, tt := range tests {
		l := New(tt.input)
		tok := l.NextToken()
		if tok.Type != TokenString {
			t.Errorf("expected STRING for %q, got %d", tt.input, tok.Type)
		}
		if tok.Literal != tt.expected {
			t.Errorf("expected literal %q, got %q", tt.expected, tok.Literal)
		}
	}
}

func TestLexerOperators(t *testing.T) {
	input := "= != < > <= >= + - * /"

	l := New(input)
	expectedTypes := []TokenType{
		TokenEquals,
		TokenNotEquals,
		TokenLessThan,
		TokenGreaterThan,
		TokenLessOrEqual,
		TokenGreaterOrEqual,
		TokenPlus,
		TokenMinus,
		TokenAsterisk,
		TokenSlash,
		TokenEOF,
	}

	for _, exp := range expectedTypes {
		tok := l.NextToken()
		if tok.Type != exp {
			t.Errorf("expected %d, got %d (literal: %q)", exp, tok.Type, tok.Literal)
		}
	}
}

func TestLexerPositionTracking(t *testing.T) {
	input := "SELECT\nname"

	l := New(input)

	// SELECT on line 1
	tok := l.NextToken()
	if tok.Line != 1 {
		t.Errorf("SELECT should be on line 1, got %d", tok.Line)
	}

	// name on line 2
	tok = l.NextToken()
	if tok.Line != 2 {
		t.Errorf("name should be on line 2, got %d", tok.Line)
	}
}
