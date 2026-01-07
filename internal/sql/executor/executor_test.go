package executor

import (
	"os"
	"strings"
	"testing"

	"github.com/cabewaldrop/claude-db/internal/sql/lexer"
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/storage"
)

func setupTestExecutor(t *testing.T) (*Executor, func()) {
	testFile := "test_executor.db"
	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}

	exec := New(pager)

	cleanup := func() {
		pager.Close()
		os.Remove(testFile)
	}

	return exec, cleanup
}

func executeSQL(t *testing.T, exec *Executor, sql string) *Result {
	l := lexer.New(sql)
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error for %q: %v", sql, err)
	}

	result, err := exec.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute error for %q: %v", sql, err)
	}

	return result
}

func TestCreateTable(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	result := executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")

	if !strings.Contains(result.Message, "created") {
		t.Errorf("expected 'created' in message, got %q", result.Message)
	}

	tables := exec.GetTables()
	if len(tables) != 1 || tables[0] != "users" {
		t.Errorf("expected [users], got %v", tables)
	}
}

func TestInsertAndSelect(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table
	executeSQL(t, exec, "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")

	// Insert data
	result := executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	if result.RowCount != 1 {
		t.Errorf("expected 1 row inserted, got %d", result.RowCount)
	}

	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")

	// Select all
	result = executeSQL(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}

	// Select specific columns
	result = executeSQL(t, exec, "SELECT name FROM users")
	if len(result.Columns) != 1 || result.Columns[0] != "name" {
		t.Errorf("expected [name], got %v", result.Columns)
	}
}

func TestSelectWithWhere(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")

	// Test equals
	result := executeSQL(t, exec, "SELECT * FROM users WHERE name = 'Alice'")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for name = 'Alice', got %d", len(result.Rows))
	}

	// Test greater than
	result = executeSQL(t, exec, "SELECT * FROM users WHERE age > 28")
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows for age > 28, got %d", len(result.Rows))
	}

	// Test AND
	result = executeSQL(t, exec, "SELECT * FROM users WHERE age > 25 AND age < 35")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for age > 25 AND age < 35, got %d", len(result.Rows))
	}
}

func TestSelectOrderBy(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (1, 'Charlie', 35)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (2, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (3, 'Bob', 25)")

	// Order by name ascending
	result := executeSQL(t, exec, "SELECT name FROM users ORDER BY name")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	names := []string{
		result.Rows[0][0].String(),
		result.Rows[1][0].String(),
		result.Rows[2][0].String(),
	}

	if names[0] != "Alice" || names[1] != "Bob" || names[2] != "Charlie" {
		t.Errorf("expected [Alice, Bob, Charlie], got %v", names)
	}

	// Order by age descending
	result = executeSQL(t, exec, "SELECT name FROM users ORDER BY age DESC")
	names = []string{
		result.Rows[0][0].String(),
		result.Rows[1][0].String(),
		result.Rows[2][0].String(),
	}

	if names[0] != "Charlie" || names[1] != "Alice" || names[2] != "Bob" {
		t.Errorf("expected [Charlie, Alice, Bob], got %v", names)
	}
}

func TestSelectLimit(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER, name TEXT)")
	for i := 1; i <= 10; i++ {
		executeSQL(t, exec, "INSERT INTO users (id, name) VALUES (1, 'User')")
	}

	result := executeSQL(t, exec, "SELECT * FROM users LIMIT 3")
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows with LIMIT, got %d", len(result.Rows))
	}

	result = executeSQL(t, exec, "SELECT * FROM users LIMIT 5 OFFSET 3")
	if len(result.Rows) != 5 {
		t.Errorf("expected 5 rows with LIMIT 5 OFFSET 3, got %d", len(result.Rows))
	}
}

func TestUpdate(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")

	result := executeSQL(t, exec, "UPDATE users SET age = 31 WHERE name = 'Alice'")
	if result.RowCount != 1 {
		t.Errorf("expected 1 row updated, got %d", result.RowCount)
	}

	// Note: Our simplified update doesn't persist changes yet
	// This test just checks that the update logic runs
}

func TestDelete(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 17)")

	result := executeSQL(t, exec, "DELETE FROM users WHERE age < 18")
	if result.RowCount != 1 {
		t.Errorf("expected 1 row deleted, got %d", result.RowCount)
	}
}

func TestDropTable(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER)")

	tables := exec.GetTables()
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}

	result := executeSQL(t, exec, "DROP TABLE users")
	if !strings.Contains(result.Message, "dropped") {
		t.Errorf("expected 'dropped' in message, got %q", result.Message)
	}

	tables = exec.GetTables()
	if len(tables) != 0 {
		t.Errorf("expected 0 tables after drop, got %d", len(tables))
	}
}

func TestResultFormatting(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER, name TEXT)")
	executeSQL(t, exec, "INSERT INTO users (id, name) VALUES (1, 'Alice')")
	executeSQL(t, exec, "INSERT INTO users (id, name) VALUES (2, 'Bob')")

	result := executeSQL(t, exec, "SELECT * FROM users")
	output := result.String()

	// Check for table formatting
	if !strings.Contains(output, "+") {
		t.Error("expected + border characters in output")
	}
	if !strings.Contains(output, "|") {
		t.Error("expected | separators in output")
	}
	if !strings.Contains(output, "id") {
		t.Error("expected column 'id' in output")
	}
	if !strings.Contains(output, "name") {
		t.Error("expected column 'name' in output")
	}
	if !strings.Contains(output, "(2 rows)") {
		t.Error("expected '(2 rows)' in output")
	}
}

func TestArithmeticExpressions(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE nums (a INTEGER, b INTEGER)")
	executeSQL(t, exec, "INSERT INTO nums (a, b) VALUES (10, 3)")

	// Test WHERE with arithmetic
	result := executeSQL(t, exec, "SELECT * FROM nums WHERE a + b = 13")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for a + b = 13, got %d", len(result.Rows))
	}

	result = executeSQL(t, exec, "SELECT * FROM nums WHERE a * b = 30")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for a * b = 30, got %d", len(result.Rows))
	}
}

func TestTableNotExists(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	l := lexer.New("SELECT * FROM nonexistent")
	p := parser.New(l)
	stmt, _ := p.Parse()

	_, err := exec.Execute(stmt)
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("expected 'does not exist' in error, got %v", err)
	}
}

func TestDuplicateTable(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER)")

	l := lexer.New("CREATE TABLE users (id INTEGER)")
	p := parser.New(l)
	stmt, _ := p.Parse()

	_, err := exec.Execute(stmt)
	if err == nil {
		t.Error("expected error for duplicate table")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("expected 'already exists' in error, got %v", err)
	}
}
