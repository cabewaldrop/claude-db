// Package web - Input validation for web handlers
//
// EDUCATIONAL NOTES:
// ------------------
// Input validation is critical for security and data integrity:
//
// 1. SQL Identifier validation: Table and column names must follow SQL rules
//    to prevent syntax errors and potential SQL injection.
//
// 2. Pattern matching: Using regexp to enforce naming conventions is more
//    reliable than manual character checking.
//
// 3. Early validation: Validate input at the HTTP layer before it reaches
//    the SQL executor, providing better error messages to users.

package web

import "regexp"

// identifierPattern matches valid SQL identifiers:
// - Must start with a letter (a-z, A-Z) or underscore
// - Can contain letters, numbers, and underscores
// - No spaces, dashes, or special characters
var identifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// IsValidIdentifier checks if a string is a valid SQL identifier.
// Valid identifiers:
//   - Start with a letter or underscore
//   - Contain only letters, numbers, and underscores
//   - Are not empty
//
// Examples:
//
//	IsValidIdentifier("users")      // true
//	IsValidIdentifier("user_table") // true
//	IsValidIdentifier("_private")   // true
//	IsValidIdentifier("123start")   // false (starts with number)
//	IsValidIdentifier("has-dash")   // false (contains dash)
//	IsValidIdentifier("has space")  // false (contains space)
//	IsValidIdentifier("")           // false (empty)
func IsValidIdentifier(s string) bool {
	if s == "" {
		return false
	}
	return identifierPattern.MatchString(s)
}

// ValidTypes is the list of valid SQL column types supported by claudedb.
var ValidTypes = []string{"INTEGER", "TEXT", "REAL", "BLOB"}

// IsValidType checks if a string is a valid column type.
func IsValidType(t string) bool {
	for _, vt := range ValidTypes {
		if t == vt {
			return true
		}
	}
	return false
}
