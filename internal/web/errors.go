package web

import "strings"

// GetErrorHint returns a helpful hint for common SQL errors.
// Returns empty string if no hint is available.
func GetErrorHint(err string) string {
	errLower := strings.ToLower(err)

	switch {
	case strings.Contains(errLower, "no such table"):
		return "Check table name spelling or run SHOW TABLES to see available tables."
	case strings.Contains(errLower, "table not found"):
		return "Check table name spelling or run SHOW TABLES to see available tables."
	case strings.Contains(errLower, "no such column"):
		return "Check column name or run DESCRIBE tablename to see columns."
	case strings.Contains(errLower, "column not found"):
		return "Check column name or run DESCRIBE tablename to see columns."
	case strings.Contains(errLower, "syntax error"):
		return "Check SQL syntax near the indicated position."
	case strings.Contains(errLower, "unique constraint"):
		return "A row with this key already exists."
	case strings.Contains(errLower, "duplicate"):
		return "A row with this key already exists."
	case strings.Contains(errLower, "not null constraint"):
		return "This column requires a value."
	case strings.Contains(errLower, "cannot be null"):
		return "This column requires a value."
	case strings.Contains(errLower, "timeout") || strings.Contains(errLower, "timed out"):
		return "Consider adding WHERE clauses or LIMIT to reduce result size."
	default:
		return ""
	}
}
