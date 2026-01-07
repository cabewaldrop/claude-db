package executor

import (
	"fmt"
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

func TestSelectWithPrimaryKeyIndex(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table with PRIMARY KEY
	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")

	// Insert multiple rows
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (10, 'David', 40)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (100, 'Eve', 28)")

	// Test PK equality lookup - this should use the index
	result := executeSQL(t, exec, "SELECT * FROM users WHERE id = 2")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for id = 2, got %d", len(result.Rows))
	}
	if result.Rows[0][1].Text != "Bob" {
		t.Errorf("expected Bob, got %s", result.Rows[0][1].Text)
	}

	// Test PK lookup for non-existent key
	result = executeSQL(t, exec, "SELECT * FROM users WHERE id = 999")
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows for id = 999, got %d", len(result.Rows))
	}

	// Test PK lookup with larger value
	result = executeSQL(t, exec, "SELECT name FROM users WHERE id = 100")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for id = 100, got %d", len(result.Rows))
	}
	if result.Rows[0][0].Text != "Eve" {
		t.Errorf("expected Eve, got %s", result.Rows[0][0].Text)
	}
}

func TestSelectWithPrimaryKeyAndAdditionalConditions(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)")
	executeSQL(t, exec, "INSERT INTO users (id, name, active) VALUES (1, 'Alice', 1)")
	executeSQL(t, exec, "INSERT INTO users (id, name, active) VALUES (2, 'Bob', 0)")

	// PK lookup with additional condition that passes
	result := executeSQL(t, exec, "SELECT * FROM users WHERE id = 1 AND active = 1")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}

	// PK lookup with additional condition that fails
	result = executeSQL(t, exec, "SELECT * FROM users WHERE id = 2 AND active = 1")
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows (PK found but active != 1), got %d", len(result.Rows))
	}
}

func TestSelectNonPKFallsBackToTableScan(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
	executeSQL(t, exec, "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)")

	// Non-PK condition should still work (via table scan)
	result := executeSQL(t, exec, "SELECT * FROM users WHERE name = 'Bob'")
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for name = 'Bob', got %d", len(result.Rows))
	}

	// Range condition on PK should fall back to table scan (not implemented yet)
	result = executeSQL(t, exec, "SELECT * FROM users WHERE id > 1")
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows for id > 1, got %d", len(result.Rows))
	}
}

func TestExplainSelect(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	executeSQL(t, exec, "INSERT INTO users (id, name) VALUES (1, 'Alice')")

	// EXPLAIN SELECT with PK equality should show index lookup
	result := executeSQL(t, exec, "EXPLAIN SELECT * FROM users WHERE id = 5")
	if len(result.Rows) == 0 {
		t.Error("expected EXPLAIN to return plan information")
	}

	// Check that we have column headers
	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns in EXPLAIN output, got %d", len(result.Columns))
	}

	// The result should contain plan information
	output := result.String()
	if !strings.Contains(output, "Query Plan") && !strings.Contains(output, "Access Method") {
		t.Errorf("expected EXPLAIN output to contain plan info, got: %s", output)
	}
}

func TestExplainTableScan(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

	// EXPLAIN SELECT on non-indexed column should show table scan
	result := executeSQL(t, exec, "EXPLAIN SELECT * FROM users WHERE name = 'Alice'")

	output := result.String()
	// Should indicate a full table scan since 'name' is not indexed
	if !strings.Contains(output, "FULL_TABLE_SCAN") {
		t.Errorf("expected FULL_TABLE_SCAN for non-indexed column query, got: %s", output)
	}
}

func TestExplainNonExistentTable(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	l := lexer.New("EXPLAIN SELECT * FROM nosuchtable")
	p := parser.New(l)
	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	_, err = exec.Execute(stmt)
	if err == nil {
		t.Error("expected error for EXPLAIN on non-existent table")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("expected 'does not exist' error, got: %v", err)
	}
}

func TestExplainDoesNotExecute(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	executeSQL(t, exec, "INSERT INTO users (id, name) VALUES (1, 'Alice')")

	// Verify the row exists
	result := executeSQL(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row before EXPLAIN DELETE, got %d", len(result.Rows))
	}

	// EXPLAIN DELETE should NOT actually delete (it will error since we only support SELECT for now)
	// But we need to check the row still exists after attempting
	l := lexer.New("EXPLAIN DELETE FROM users WHERE id = 1")
	p := parser.New(l)
	stmt, _ := p.Parse()

	// This may error (since EXPLAIN only supports SELECT), but we need to verify
	// the row wasn't deleted
	exec.Execute(stmt)

	// Row should still exist
	result = executeSQL(t, exec, "SELECT * FROM users")
	if len(result.Rows) != 1 {
		t.Errorf("EXPLAIN should not execute the statement - expected 1 row, got %d", len(result.Rows))
	}
}

func TestExplainParsing(t *testing.T) {
	// Test that EXPLAIN parses correctly for various statement types
	testCases := []string{
		"EXPLAIN SELECT * FROM users",
		"EXPLAIN SELECT id, name FROM users WHERE id = 1",
		"EXPLAIN SELECT * FROM users WHERE name = 'Alice' AND id > 5",
	}

	for _, sql := range testCases {
		l := lexer.New(sql)
		p := parser.New(l)
		stmt, err := p.Parse()
		if err != nil {
			t.Errorf("Parse error for %q: %v", sql, err)
			continue
		}

		explainStmt, ok := stmt.(*parser.ExplainStatement)
		if !ok {
			t.Errorf("expected ExplainStatement for %q, got %T", sql, stmt)
			continue
		}

		if explainStmt.Statement == nil {
			t.Errorf("expected inner statement for %q", sql)
		}
	}
}

func TestOrderByLimit(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE scores (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")

	// Insert 20 rows with varying scores (score = 21 - id, so lower ids have higher scores)
	for i := 1; i <= 20; i++ {
		executeSQL(t, exec, fmt.Sprintf("INSERT INTO scores (id, name, score) VALUES (%d, 'User%d', %d)", i, i, 21-i))
	}

	// Test ORDER BY ASC with LIMIT
	result := executeSQL(t, exec, "SELECT * FROM scores ORDER BY score LIMIT 5")
	if len(result.Rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(result.Rows))
	}
	// Lowest scores should be 1, 2, 3, 4, 5 (from ids 20, 19, 18, 17, 16)
	expectedScores := []int64{1, 2, 3, 4, 5}
	for i, row := range result.Rows {
		if row[2].Integer != expectedScores[i] {
			t.Errorf("row %d: expected score %d, got %d", i, expectedScores[i], row[2].Integer)
		}
	}

	// Test ORDER BY DESC with LIMIT
	result = executeSQL(t, exec, "SELECT * FROM scores ORDER BY score DESC LIMIT 3")
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}
	// Highest scores should be 20, 19, 18 (from ids 1, 2, 3)
	expectedDescScores := []int64{20, 19, 18}
	for i, row := range result.Rows {
		if row[2].Integer != expectedDescScores[i] {
			t.Errorf("row %d DESC: expected score %d, got %d", i, expectedDescScores[i], row[2].Integer)
		}
	}
}

func TestOrderByLimitOffset(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	executeSQL(t, exec, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)")
	for i := 1; i <= 10; i++ {
		executeSQL(t, exec, fmt.Sprintf("INSERT INTO items (id, value) VALUES (%d, %d)", i, i*10))
	}

	// LIMIT 3 OFFSET 2: values should be 30, 40, 50
	result := executeSQL(t, exec, "SELECT * FROM items ORDER BY value LIMIT 3 OFFSET 2")
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}
	expectedValues := []int64{30, 40, 50}
	for i, row := range result.Rows {
		if row[1].Integer != expectedValues[i] {
			t.Errorf("row %d: expected value %d, got %d", i, expectedValues[i], row[1].Integer)
		}
	}
}

func BenchmarkOrderByLimit(b *testing.B) {
	exec, cleanup := setupBenchExecutor(b)
	defer cleanup()

	// Helper to execute SQL in benchmark
	executeBenchSQL := func(sql string) {
		l := lexer.New(sql)
		p := parser.New(l)
		stmt, err := p.Parse()
		if err != nil {
			b.Fatalf("Parse error: %v", err)
		}
		if _, err := exec.Execute(stmt); err != nil {
			b.Fatalf("Execute error: %v", err)
		}
	}

	// Create table with 10k rows
	executeBenchSQL("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER)")
	for i := 0; i < 10000; i++ {
		executeBenchSQL(fmt.Sprintf("INSERT INTO bench (id, value) VALUES (%d, %d)", i, 10000-i))
	}

	// Pre-parse the benchmark query
	l := lexer.New("SELECT * FROM bench ORDER BY value LIMIT 10")
	p := parser.New(l)
	selectStmt, err := p.Parse()
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(selectStmt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func setupBenchExecutor(b *testing.B) (*Executor, func()) {
	b.Helper()
	testFile := "bench_executor.db"
	pager, err := storage.NewPager(testFile)
	if err != nil {
		b.Fatalf("Failed to create pager: %v", err)
	}

	exec := New(pager)

	cleanup := func() {
		pager.Close()
		os.Remove(testFile)
	}

	return exec, cleanup
}

func TestAnalyzeCommand(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create table and insert data
	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	for i := 1; i <= 10; i++ {
		executeSQL(t, exec, "INSERT INTO users (id, name) VALUES (1, 'User')")
	}

	// Run ANALYZE
	result := executeSQL(t, exec, "ANALYZE users")
	if !strings.Contains(result.Message, "Analyzed") {
		t.Errorf("expected 'Analyzed' in message, got %q", result.Message)
	}

	// Check stats are available
	tbl, ok := exec.GetTable("users")
	if !ok {
		t.Fatal("table users not found")
	}
	stats := tbl.Stats()
	if stats.RowCount != 10 {
		t.Errorf("expected RowCount 10 after ANALYZE, got %d", stats.RowCount)
	}
}

func TestAnalyzeAllTables(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create multiple tables
	executeSQL(t, exec, "CREATE TABLE users (id INTEGER PRIMARY KEY)")
	executeSQL(t, exec, "CREATE TABLE orders (id INTEGER PRIMARY KEY)")
	executeSQL(t, exec, "INSERT INTO users (id) VALUES (1)")
	executeSQL(t, exec, "INSERT INTO orders (id) VALUES (1)")

	// Run ANALYZE without table name (analyze all)
	result := executeSQL(t, exec, "ANALYZE")
	if !strings.Contains(result.Message, "2 table") {
		t.Errorf("expected message about 2 tables, got %q", result.Message)
	}
}
