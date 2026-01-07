// Package lexer implements a lexical analyzer (tokenizer) for SQL.
//
// EDUCATIONAL NOTES:
// ------------------
// A lexer (also called tokenizer or scanner) is the first phase of parsing.
// It reads the raw input string and converts it into a stream of tokens.
//
// For example, the input:
//   SELECT name FROM users WHERE id = 1
//
// Becomes these tokens:
//   [SELECT] [IDENT:name] [FROM] [IDENT:users] [WHERE] [IDENT:id] [EQUALS] [NUMBER:1]
//
// The lexer handles:
// - Keywords (SELECT, INSERT, FROM, WHERE, etc.)
// - Identifiers (table names, column names)
// - Literals (strings, numbers)
// - Operators (=, <, >, !=, etc.)
// - Punctuation (commas, parentheses)
// - Whitespace (which we skip)

package lexer

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenType represents the type of a token.
type TokenType int

const (
	// Special tokens
	TokenEOF TokenType = iota
	TokenError
	TokenIllegal

	// Literals
	TokenIdent   // column names, table names
	TokenNumber  // 123, 45.67
	TokenString  // 'hello'
	TokenBoolean // TRUE, FALSE

	// Keywords
	TokenSelect
	TokenInsert
	TokenUpdate
	TokenDelete
	TokenCreate
	TokenDrop
	TokenInto
	TokenValues
	TokenFrom
	TokenWhere
	TokenAnd
	TokenOr
	TokenNot
	TokenSet
	TokenTable
	TokenPrimaryKey
	TokenNull
	TokenOrder
	TokenBy
	TokenAsc
	TokenDesc
	TokenLimit
	TokenOffset
	TokenExplain
	TokenAnalyze

	// Data types
	TokenInt
	TokenInteger
	TokenText
	TokenVarchar
	TokenBool
	TokenReal

	// Operators
	TokenEquals         // =
	TokenNotEquals      // != or <>
	TokenLessThan       // <
	TokenGreaterThan    // >
	TokenLessOrEqual    // <=
	TokenGreaterOrEqual // >=
	TokenPlus           // +
	TokenMinus          // -
	TokenAsterisk       // *
	TokenSlash          // /

	// Punctuation
	TokenComma       // ,
	TokenSemicolon   // ;
	TokenLeftParen   // (
	TokenRightParen  // )
	TokenLeftBrace   // {
	TokenRightBrace  // }
	TokenLeftSquare  // [
	TokenRightSquare // ]
)

// Token represents a lexical token.
type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Column  int
}

// String returns a human-readable representation of the token.
func (t Token) String() string {
	return fmt.Sprintf("Token{%s, %q, line:%d, col:%d}",
		tokenTypeName(t.Type), t.Literal, t.Line, t.Column)
}

// tokenTypeName returns the name of a token type.
func tokenTypeName(t TokenType) string {
	names := map[TokenType]string{
		TokenEOF:            "EOF",
		TokenError:          "ERROR",
		TokenIllegal:        "ILLEGAL",
		TokenIdent:          "IDENT",
		TokenNumber:         "NUMBER",
		TokenString:         "STRING",
		TokenBoolean:        "BOOLEAN",
		TokenSelect:         "SELECT",
		TokenInsert:         "INSERT",
		TokenUpdate:         "UPDATE",
		TokenDelete:         "DELETE",
		TokenCreate:         "CREATE",
		TokenDrop:           "DROP",
		TokenInto:           "INTO",
		TokenValues:         "VALUES",
		TokenFrom:           "FROM",
		TokenWhere:          "WHERE",
		TokenAnd:            "AND",
		TokenOr:             "OR",
		TokenNot:            "NOT",
		TokenSet:            "SET",
		TokenTable:          "TABLE",
		TokenPrimaryKey:     "PRIMARY KEY",
		TokenNull:           "NULL",
		TokenOrder:          "ORDER",
		TokenBy:             "BY",
		TokenAsc:            "ASC",
		TokenDesc:           "DESC",
		TokenLimit:          "LIMIT",
		TokenOffset:         "OFFSET",
		TokenExplain:        "EXPLAIN",
		TokenInt:            "INT",
		TokenInteger:        "INTEGER",
		TokenText:           "TEXT",
		TokenVarchar:        "VARCHAR",
		TokenBool:           "BOOL",
		TokenReal:           "REAL",
		TokenEquals:         "EQUALS",
		TokenNotEquals:      "NOT_EQUALS",
		TokenLessThan:       "LESS_THAN",
		TokenGreaterThan:    "GREATER_THAN",
		TokenLessOrEqual:    "LESS_OR_EQUAL",
		TokenGreaterOrEqual: "GREATER_OR_EQUAL",
		TokenPlus:           "PLUS",
		TokenMinus:          "MINUS",
		TokenAsterisk:       "ASTERISK",
		TokenSlash:          "SLASH",
		TokenComma:          "COMMA",
		TokenSemicolon:      "SEMICOLON",
		TokenLeftParen:      "LEFT_PAREN",
		TokenRightParen:     "RIGHT_PAREN",
		TokenLeftBrace:      "LEFT_BRACE",
		TokenRightBrace:     "RIGHT_BRACE",
		TokenLeftSquare:     "LEFT_SQUARE",
		TokenRightSquare:    "RIGHT_SQUARE",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN(%d)", t)
}

// keywords maps SQL keywords to their token types.
// SQL is case-insensitive, so we store them in uppercase.
var keywords = map[string]TokenType{
	"SELECT":  TokenSelect,
	"INSERT":  TokenInsert,
	"UPDATE":  TokenUpdate,
	"DELETE":  TokenDelete,
	"CREATE":  TokenCreate,
	"DROP":    TokenDrop,
	"INTO":    TokenInto,
	"VALUES":  TokenValues,
	"FROM":    TokenFrom,
	"WHERE":   TokenWhere,
	"AND":     TokenAnd,
	"OR":      TokenOr,
	"NOT":     TokenNot,
	"SET":     TokenSet,
	"TABLE":   TokenTable,
	"PRIMARY": TokenPrimaryKey, // Will need special handling for "PRIMARY KEY"
	"KEY":     TokenIdent,      // Standalone KEY is just an identifier
	"NULL":    TokenNull,
	"TRUE":    TokenBoolean,
	"FALSE":   TokenBoolean,
	"ORDER":   TokenOrder,
	"BY":      TokenBy,
	"ASC":     TokenAsc,
	"DESC":    TokenDesc,
	"LIMIT":   TokenLimit,
	"OFFSET":  TokenOffset,
	"EXPLAIN": TokenExplain,
	"ANALYZE": TokenAnalyze,
	"INT":     TokenInt,
	"INTEGER": TokenInteger,
	"TEXT":    TokenText,
	"VARCHAR": TokenVarchar,
	"BOOL":    TokenBool,
	"BOOLEAN": TokenBool,
	"REAL":    TokenReal,
}

// Lexer tokenizes SQL input.
type Lexer struct {
	input   string
	pos     int  // current position in input
	readPos int  // reading position (after current char)
	ch      byte // current character
	line    int
	column  int
}

// New creates a new Lexer for the given input.
func New(input string) *Lexer {
	l := &Lexer{
		input:  input,
		line:   1,
		column: 0,
	}
	l.readChar() // Initialize first character
	return l
}

// readChar reads the next character and advances position.
func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0 // ASCII NUL signifies EOF
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
	l.column++

	if l.ch == '\n' {
		l.line++
		l.column = 0
	}
}

// peekChar looks at the next character without advancing.
func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

// NextToken returns the next token from the input.
//
// EDUCATIONAL NOTE:
// -----------------
// This is the main lexer function. It examines the current character
// and decides what type of token it starts. This is essentially a
// big switch statement with some helper functions.
func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	tok.Line = l.line
	tok.Column = l.column

	switch l.ch {
	case '=':
		tok = l.makeToken(TokenEquals, string(l.ch))
	case '+':
		tok = l.makeToken(TokenPlus, string(l.ch))
	case '-':
		// Could be minus or negative number
		if unicode.IsDigit(rune(l.peekChar())) {
			// Start of negative number - let readNumber handle it
			tok = l.readNumber()
			return tok
		}
		tok = l.makeToken(TokenMinus, string(l.ch))
	case '*':
		tok = l.makeToken(TokenAsterisk, string(l.ch))
	case '/':
		tok = l.makeToken(TokenSlash, string(l.ch))
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = l.makeToken(TokenLessOrEqual, string(ch)+string(l.ch))
		} else if l.peekChar() == '>' {
			ch := l.ch
			l.readChar()
			tok = l.makeToken(TokenNotEquals, string(ch)+string(l.ch))
		} else {
			tok = l.makeToken(TokenLessThan, string(l.ch))
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = l.makeToken(TokenGreaterOrEqual, string(ch)+string(l.ch))
		} else {
			tok = l.makeToken(TokenGreaterThan, string(l.ch))
		}
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = l.makeToken(TokenNotEquals, string(ch)+string(l.ch))
		} else {
			tok = l.makeToken(TokenIllegal, string(l.ch))
		}
	case ',':
		tok = l.makeToken(TokenComma, string(l.ch))
	case ';':
		tok = l.makeToken(TokenSemicolon, string(l.ch))
	case '(':
		tok = l.makeToken(TokenLeftParen, string(l.ch))
	case ')':
		tok = l.makeToken(TokenRightParen, string(l.ch))
	case '{':
		tok = l.makeToken(TokenLeftBrace, string(l.ch))
	case '}':
		tok = l.makeToken(TokenRightBrace, string(l.ch))
	case '[':
		tok = l.makeToken(TokenLeftSquare, string(l.ch))
	case ']':
		tok = l.makeToken(TokenRightSquare, string(l.ch))
	case '\'':
		tok = l.readString()
		return tok
	case 0:
		tok.Literal = ""
		tok.Type = TokenEOF
		return tok
	default:
		if isLetter(l.ch) {
			tok = l.readIdentifier()
			return tok
		} else if isDigit(l.ch) {
			tok = l.readNumber()
			return tok
		} else {
			tok = l.makeToken(TokenIllegal, string(l.ch))
		}
	}

	l.readChar()
	return tok
}

// makeToken creates a token with current position info.
func (l *Lexer) makeToken(tokenType TokenType, literal string) Token {
	return Token{
		Type:    tokenType,
		Literal: literal,
		Line:    l.line,
		Column:  l.column,
	}
}

// skipWhitespace skips spaces, tabs, and newlines.
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// readIdentifier reads an identifier or keyword.
func (l *Lexer) readIdentifier() Token {
	startLine := l.line
	startColumn := l.column
	startPos := l.pos

	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}

	literal := l.input[startPos:l.pos]
	upperLiteral := strings.ToUpper(literal)

	// Check if it's a keyword
	tokenType, isKeyword := keywords[upperLiteral]
	if !isKeyword {
		tokenType = TokenIdent
	}

	return Token{
		Type:    tokenType,
		Literal: literal,
		Line:    startLine,
		Column:  startColumn,
	}
}

// readNumber reads a numeric literal (integer or float).
func (l *Lexer) readNumber() Token {
	startLine := l.line
	startColumn := l.column
	startPos := l.pos

	// Handle optional negative sign
	if l.ch == '-' {
		l.readChar()
	}

	// Read integer part
	for isDigit(l.ch) {
		l.readChar()
	}

	// Check for decimal point
	if l.ch == '.' && isDigit(l.peekChar()) {
		l.readChar() // consume '.'
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	return Token{
		Type:    TokenNumber,
		Literal: l.input[startPos:l.pos],
		Line:    startLine,
		Column:  startColumn,
	}
}

// readString reads a string literal (enclosed in single quotes).
//
// EDUCATIONAL NOTE:
// -----------------
// SQL uses single quotes for string literals: 'hello world'
// To include a single quote in a string, you double it: 'it''s working'
func (l *Lexer) readString() Token {
	startLine := l.line
	startColumn := l.column

	var sb strings.Builder
	l.readChar() // consume opening quote

	for {
		if l.ch == '\'' {
			if l.peekChar() == '\'' {
				// Escaped quote
				sb.WriteByte('\'')
				l.readChar()
				l.readChar()
			} else {
				// End of string
				l.readChar()
				break
			}
		} else if l.ch == 0 {
			// Unexpected EOF
			return Token{
				Type:    TokenError,
				Literal: "unterminated string",
				Line:    startLine,
				Column:  startColumn,
			}
		} else {
			sb.WriteByte(l.ch)
			l.readChar()
		}
	}

	return Token{
		Type:    TokenString,
		Literal: sb.String(),
		Line:    startLine,
		Column:  startColumn,
	}
}

// Tokenize returns all tokens from the input.
// Useful for debugging and testing.
func (l *Lexer) Tokenize() []Token {
	var tokens []Token
	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == TokenEOF || tok.Type == TokenError {
			break
		}
	}
	return tokens
}

// isLetter checks if the character can start an identifier.
func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || ch == '_'
}

// isDigit checks if the character is a digit.
func isDigit(ch byte) bool {
	return unicode.IsDigit(rune(ch))
}
