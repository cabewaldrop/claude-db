package table

import (
	"os"
	"testing"

	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/storage"
)

func setupTestTable(t *testing.T) (*Table, *storage.Pager, func()) {
	testFile := "test_table.db"
	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}

	schema := NewSchema([]parser.ColumnDefinition{
		{Name: "id", Type: parser.TypeInteger, PrimaryKey: true},
		{Name: "name", Type: parser.TypeText},
		{Name: "age", Type: parser.TypeInteger},
	})

	tbl, err := NewTable("users", schema, pager)
	if err != nil {
		pager.Close()
		os.Remove(testFile)
		t.Fatalf("Failed to create table: %v", err)
	}

	cleanup := func() {
		pager.Close()
		os.Remove(testFile)
	}

	return tbl, pager, cleanup
}

func TestNewSchema(t *testing.T) {
	columns := []parser.ColumnDefinition{
		{Name: "id", Type: parser.TypeInteger, PrimaryKey: true},
		{Name: "name", Type: parser.TypeText, NotNull: true},
		{Name: "score", Type: parser.TypeReal},
	}

	schema := NewSchema(columns)

	if len(schema.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(schema.Columns))
	}

	if schema.PrimaryKey != 0 {
		t.Errorf("expected primary key at index 0, got %d", schema.PrimaryKey)
	}

	// Test column lookup
	idx, ok := schema.GetColumnIndex("name")
	if !ok || idx != 1 {
		t.Errorf("expected name at index 1, got %d, ok=%v", idx, ok)
	}

	_, ok = schema.GetColumnIndex("nonexistent")
	if ok {
		t.Error("expected nonexistent column to not be found")
	}
}

func TestValueString(t *testing.T) {
	tests := []struct {
		value    Value
		expected string
	}{
		{Value{Type: parser.TypeInteger, Integer: 42}, "42"},
		{Value{Type: parser.TypeReal, Real: 3.14}, "3.14"},
		{Value{Type: parser.TypeText, Text: "hello"}, "hello"},
		{Value{Type: parser.TypeBoolean, Boolean: true}, "TRUE"},
		{Value{Type: parser.TypeBoolean, Boolean: false}, "FALSE"},
		{Value{IsNull: true}, "NULL"},
	}

	for _, tt := range tests {
		result := tt.value.String()
		if result != tt.expected {
			t.Errorf("expected %q, got %q", tt.expected, result)
		}
	}
}

func TestValueCompare(t *testing.T) {
	tests := []struct {
		a, b     Value
		expected int
	}{
		// Integer comparison
		{Value{Type: parser.TypeInteger, Integer: 5}, Value{Type: parser.TypeInteger, Integer: 10}, -1},
		{Value{Type: parser.TypeInteger, Integer: 10}, Value{Type: parser.TypeInteger, Integer: 5}, 1},
		{Value{Type: parser.TypeInteger, Integer: 5}, Value{Type: parser.TypeInteger, Integer: 5}, 0},

		// Text comparison
		{Value{Type: parser.TypeText, Text: "apple"}, Value{Type: parser.TypeText, Text: "banana"}, -1},
		{Value{Type: parser.TypeText, Text: "banana"}, Value{Type: parser.TypeText, Text: "apple"}, 1},
		{Value{Type: parser.TypeText, Text: "same"}, Value{Type: parser.TypeText, Text: "same"}, 0},

		// NULL comparison
		{Value{IsNull: true}, Value{IsNull: true}, 0},
		{Value{IsNull: true}, Value{Type: parser.TypeInteger, Integer: 5}, -1},
		{Value{Type: parser.TypeInteger, Integer: 5}, Value{IsNull: true}, 1},
	}

	for i, tt := range tests {
		result := tt.a.Compare(tt.b)
		if result != tt.expected {
			t.Errorf("test %d: Compare(%v, %v) = %d, expected %d",
				i, tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestValueEquals(t *testing.T) {
	tests := []struct {
		a, b     Value
		expected bool
	}{
		{Value{Type: parser.TypeInteger, Integer: 42}, Value{Type: parser.TypeInteger, Integer: 42}, true},
		{Value{Type: parser.TypeInteger, Integer: 42}, Value{Type: parser.TypeInteger, Integer: 43}, false},
		{Value{Type: parser.TypeText, Text: "hello"}, Value{Type: parser.TypeText, Text: "hello"}, true},
		{Value{Type: parser.TypeText, Text: "hello"}, Value{Type: parser.TypeText, Text: "world"}, false},
		{Value{IsNull: true}, Value{IsNull: true}, true},
		{Value{IsNull: true}, Value{Type: parser.TypeInteger, Integer: 0}, false},
	}

	for i, tt := range tests {
		result := tt.a.Equals(tt.b)
		if result != tt.expected {
			t.Errorf("test %d: Equals(%v, %v) = %v, expected %v",
				i, tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestTableInsert(t *testing.T) {
	tbl, _, cleanup := setupTestTable(t)
	defer cleanup()

	values := []Value{
		{Type: parser.TypeInteger, Integer: 1},
		{Type: parser.TypeText, Text: "Alice"},
		{Type: parser.TypeInteger, Integer: 30},
	}

	rowID, err := tbl.Insert(values)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if rowID != 1 {
		t.Errorf("expected rowID 1, got %d", rowID)
	}

	// Insert another row
	values[0].Integer = 2
	values[1].Text = "Bob"
	values[2].Integer = 25

	rowID, err = tbl.Insert(values)
	if err != nil {
		t.Fatalf("Second insert failed: %v", err)
	}

	if rowID != 2 {
		t.Errorf("expected rowID 2, got %d", rowID)
	}
}

func TestTableInsertValidation(t *testing.T) {
	tbl, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Wrong number of values
	values := []Value{
		{Type: parser.TypeInteger, Integer: 1},
		{Type: parser.TypeText, Text: "Alice"},
	}

	_, err := tbl.Insert(values)
	if err == nil {
		t.Error("expected error for wrong number of values")
	}
}

func TestTableScan(t *testing.T) {
	tbl, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Insert some rows
	for i := 1; i <= 3; i++ {
		values := []Value{
			{Type: parser.TypeInteger, Integer: int64(i)},
			{Type: parser.TypeText, Text: "User"},
			{Type: parser.TypeInteger, Integer: int64(20 + i)},
		}
		_, err := tbl.Insert(values)
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	rows, err := tbl.Scan()
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}
}

func TestTableScanWithFilter(t *testing.T) {
	tbl, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Insert rows with different ages
	ages := []int64{25, 30, 35, 20, 40}
	for i, age := range ages {
		values := []Value{
			{Type: parser.TypeInteger, Integer: int64(i + 1)},
			{Type: parser.TypeText, Text: "User"},
			{Type: parser.TypeInteger, Integer: age},
		}
		tbl.Insert(values)
	}

	// Filter for age > 28
	rows, err := tbl.ScanWithFilter(func(row Row) bool {
		if len(row.Values) >= 3 {
			return row.Values[2].Integer > 28
		}
		return false
	})

	if err != nil {
		t.Fatalf("ScanWithFilter failed: %v", err)
	}

	// Should get ages 30, 35, 40
	if len(rows) != 3 {
		t.Errorf("expected 3 rows with age > 28, got %d", len(rows))
	}
}

func TestRowSerialization(t *testing.T) {
	tbl, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Test that serialization/deserialization preserves values
	values := []Value{
		{Type: parser.TypeInteger, Integer: 42},
		{Type: parser.TypeText, Text: "Test String"},
		{Type: parser.TypeInteger, Integer: 100},
	}

	rowID, err := tbl.Insert(values)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	rows, err := tbl.Scan()
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	row := rows[0]
	if row.ID != rowID {
		t.Errorf("expected row ID %d, got %d", rowID, row.ID)
	}

	if row.Values[0].Integer != 42 {
		t.Errorf("expected id 42, got %d", row.Values[0].Integer)
	}

	if row.Values[1].Text != "Test String" {
		t.Errorf("expected 'Test String', got %q", row.Values[1].Text)
	}

	if row.Values[2].Integer != 100 {
		t.Errorf("expected age 100, got %d", row.Values[2].Integer)
	}
}

func TestColumnDefinitionString(t *testing.T) {
	col := parser.ColumnDefinition{
		Name:       "id",
		Type:       parser.TypeInteger,
		PrimaryKey: true,
	}

	str := Column{
		Name:       col.Name,
		Type:       col.Type,
		PrimaryKey: col.PrimaryKey,
	}

	// Just verify it doesn't panic
	_ = str
}

func TestGetRowByLocation(t *testing.T) {
	tbl, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Insert a row
	values := []Value{
		{Type: parser.TypeInteger, Integer: 42},
		{Type: parser.TypeText, Text: "Alice"},
		{Type: parser.TypeInteger, Integer: 30},
	}

	_, err := tbl.Insert(values)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Get the location from the B-tree using the primary key
	keyBytes, err := tbl.valueToBytes(values[0]) // Primary key is first column
	if err != nil {
		t.Fatalf("valueToBytes failed: %v", err)
	}
	location, found, err := tbl.btree.Search(keyBytes)
	if err != nil {
		t.Fatalf("BTree search failed: %v", err)
	}
	if !found {
		t.Fatal("Key not found in B-tree index")
	}

	// Retrieve the row by location
	row, err := tbl.GetRowByLocation(location)
	if err != nil {
		t.Fatalf("GetRowByLocation failed: %v", err)
	}

	// Verify the row data
	if row.ID != 1 {
		t.Errorf("expected row ID 1, got %d", row.ID)
	}
	if row.Values[0].Integer != 42 {
		t.Errorf("expected id 42, got %d", row.Values[0].Integer)
	}
	if row.Values[1].Text != "Alice" {
		t.Errorf("expected name 'Alice', got %q", row.Values[1].Text)
	}
	if row.Values[2].Integer != 30 {
		t.Errorf("expected age 30, got %d", row.Values[2].Integer)
	}
}

func TestGetRowByLocationMultipleRows(t *testing.T) {
	tbl, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Insert multiple rows
	testData := []struct {
		id   int64
		name string
		age  int64
	}{
		{1, "Alice", 30},
		{2, "Bob", 25},
		{3, "Charlie", 35},
	}

	for _, td := range testData {
		values := []Value{
			{Type: parser.TypeInteger, Integer: td.id},
			{Type: parser.TypeText, Text: td.name},
			{Type: parser.TypeInteger, Integer: td.age},
		}
		_, err := tbl.Insert(values)
		if err != nil {
			t.Fatalf("Insert failed for %s: %v", td.name, err)
		}
	}

	// Verify we can retrieve each row by its location
	for _, td := range testData {
		keyBytes, err := tbl.valueToBytes(Value{Type: parser.TypeInteger, Integer: td.id})
		if err != nil {
			t.Fatalf("valueToBytes failed for id %d: %v", td.id, err)
		}
		location, found, err := tbl.btree.Search(keyBytes)
		if err != nil {
			t.Fatalf("BTree search failed for id %d: %v", td.id, err)
		}
		if !found {
			t.Fatalf("Key %d not found in B-tree index", td.id)
		}

		row, err := tbl.GetRowByLocation(location)
		if err != nil {
			t.Fatalf("GetRowByLocation failed for id %d: %v", td.id, err)
		}

		if row.Values[0].Integer != td.id {
			t.Errorf("expected id %d, got %d", td.id, row.Values[0].Integer)
		}
		if row.Values[1].Text != td.name {
			t.Errorf("expected name %q, got %q", td.name, row.Values[1].Text)
		}
		if row.Values[2].Integer != td.age {
			t.Errorf("expected age %d, got %d", td.age, row.Values[2].Integer)
		}
	}
}

func TestGetRowByLocationInvalidLocation(t *testing.T) {
	tbl, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Insert a row to ensure the table has data pages
	values := []Value{
		{Type: parser.TypeInteger, Integer: 1},
		{Type: parser.TypeText, Text: "Test"},
		{Type: parser.TypeInteger, Integer: 20},
	}
	tbl.Insert(values)

	// Test with invalid page ID (page that doesn't exist)
	invalidLocation := uint64(999) << 32 // Non-existent page
	_, err := tbl.GetRowByLocation(invalidLocation)
	if err == nil {
		t.Error("expected error for invalid page ID")
	}
}

func TestGetRowByPrimaryKey(t *testing.T) {
	tbl, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Insert multiple rows
	testData := []struct {
		id   int64
		name string
		age  int64
	}{
		{1, "Alice", 30},
		{5, "Bob", 25},
		{10, "Charlie", 35},
	}

	for _, td := range testData {
		values := []Value{
			{Type: parser.TypeInteger, Integer: td.id},
			{Type: parser.TypeText, Text: td.name},
			{Type: parser.TypeInteger, Integer: td.age},
		}
		_, err := tbl.Insert(values)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Test lookup by primary key
	for _, td := range testData {
		keyValue := Value{Type: parser.TypeInteger, Integer: td.id}
		row, found, err := tbl.GetRowByPrimaryKey(keyValue)
		if err != nil {
			t.Fatalf("GetRowByPrimaryKey failed for id %d: %v", td.id, err)
		}
		if !found {
			t.Fatalf("expected to find row with id %d", td.id)
		}
		if row.Values[0].Integer != td.id {
			t.Errorf("expected id %d, got %d", td.id, row.Values[0].Integer)
		}
		if row.Values[1].Text != td.name {
			t.Errorf("expected name %q, got %q", td.name, row.Values[1].Text)
		}
	}

	// Test lookup for non-existent key
	keyValue := Value{Type: parser.TypeInteger, Integer: 999}
	_, found, err := tbl.GetRowByPrimaryKey(keyValue)
	if err != nil {
		t.Fatalf("GetRowByPrimaryKey failed: %v", err)
	}
	if found {
		t.Error("expected not to find row with id 999")
	}
}

func TestGetRowByPrimaryKeyNoPK(t *testing.T) {
	// Create a table without a primary key
	testFile := "test_table_nopk.db"
	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer func() {
		pager.Close()
		os.Remove(testFile)
	}()

	// Schema without primary key
	schema := NewSchema([]parser.ColumnDefinition{
		{Name: "id", Type: parser.TypeInteger},
		{Name: "name", Type: parser.TypeText},
	})

	tbl, err := NewTable("nopk", schema, pager)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	keyValue := Value{Type: parser.TypeInteger, Integer: 1}
	_, _, err = tbl.GetRowByPrimaryKey(keyValue)
	if err == nil {
		t.Error("expected error for table without primary key")
	}
}

func TestSerializeValueUnknownType(t *testing.T) {
	tbl, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Try to serialize a value with TypeUnknown - should fail
	unknownValue := Value{Type: parser.TypeUnknown, Integer: 42}
	_, err := tbl.valueToBytes(unknownValue)
	if err == nil {
		t.Error("expected error when serializing value with unknown type")
	}
	if err != nil && err.Error() != "unsupported type for serialization: 0" {
		// TypeUnknown has value 0
		t.Logf("Got expected error: %v", err)
	}
}

func TestSerializeValueNullBypassesTypeCheck(t *testing.T) {
	tbl, _, cleanup := setupTestTable(t)
	defer cleanup()

	// Null values should serialize successfully even with unknown type
	// because the null flag bypasses the type-specific serialization
	nullValue := Value{Type: parser.TypeUnknown, IsNull: true}
	_, err := tbl.valueToBytes(nullValue)
	if err != nil {
		t.Errorf("null value with unknown type should serialize successfully: %v", err)
	}
}
