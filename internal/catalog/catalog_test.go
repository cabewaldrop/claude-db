package catalog

import (
	"os"
	"testing"

	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/storage"
	"github.com/cabewaldrop/claude-db/internal/table"
)

func TestCatalogNewDatabase(t *testing.T) {
	testFile := "test_catalog.db"
	defer os.Remove(testFile)

	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	cat, err := NewCatalog(pager)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}

	// New database should have no tables
	tables := cat.ListTables()
	if len(tables) != 0 {
		t.Errorf("New database should have no tables, got %d", len(tables))
	}
}

func TestCatalogAddTable(t *testing.T) {
	testFile := "test_catalog_add.db"
	defer os.Remove(testFile)

	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	cat, err := NewCatalog(pager)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}

	// Create a table
	schema := table.NewSchema([]parser.ColumnDefinition{
		{Name: "id", Type: parser.TypeInteger, PrimaryKey: true},
		{Name: "name", Type: parser.TypeText},
	})

	tbl, err := table.NewTable("users", schema, pager)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Add to catalog
	if err := cat.AddTable("users", tbl); err != nil {
		t.Fatalf("Failed to add table: %v", err)
	}

	// Check table exists
	tables := cat.ListTables()
	if len(tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(tables))
	}

	info, ok := cat.GetTableInfo("users")
	if !ok {
		t.Fatal("Table 'users' not found")
	}

	if info.Name != "users" {
		t.Errorf("Expected name 'users', got %s", info.Name)
	}

	if len(info.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(info.Columns))
	}
}

func TestCatalogPersistence(t *testing.T) {
	testFile := "test_catalog_persist.db"
	defer os.Remove(testFile)

	// Create database and add table
	func() {
		pager, err := storage.NewPager(testFile)
		if err != nil {
			t.Fatalf("Failed to create pager: %v", err)
		}
		defer pager.Close()

		cat, err := NewCatalog(pager)
		if err != nil {
			t.Fatalf("Failed to create catalog: %v", err)
		}

		schema := table.NewSchema([]parser.ColumnDefinition{
			{Name: "id", Type: parser.TypeInteger, PrimaryKey: true},
			{Name: "email", Type: parser.TypeText, NotNull: true},
		})

		tbl, err := table.NewTable("accounts", schema, pager)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		if err := cat.AddTable("accounts", tbl); err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		cat.Flush()
	}()

	// Reopen and verify
	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer pager.Close()

	cat, err := NewCatalog(pager)
	if err != nil {
		t.Fatalf("Failed to reload catalog: %v", err)
	}

	tables := cat.ListTables()
	if len(tables) != 1 {
		t.Errorf("Expected 1 table after reload, got %d", len(tables))
	}

	info, ok := cat.GetTableInfo("accounts")
	if !ok {
		t.Fatal("Table 'accounts' not found after reload")
	}

	if info.Name != "accounts" {
		t.Errorf("Expected name 'accounts', got %s", info.Name)
	}

	if len(info.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(info.Columns))
	}

	if info.Columns[0].Name != "id" {
		t.Errorf("Expected first column 'id', got %s", info.Columns[0].Name)
	}

	if !info.Columns[0].PrimaryKey {
		t.Error("First column should be primary key")
	}

	if info.Columns[1].Name != "email" {
		t.Errorf("Expected second column 'email', got %s", info.Columns[1].Name)
	}

	if !info.Columns[1].NotNull {
		t.Error("Second column should be NOT NULL")
	}
}

func TestCatalogRemoveTable(t *testing.T) {
	testFile := "test_catalog_remove.db"
	defer os.Remove(testFile)

	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	cat, err := NewCatalog(pager)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}

	// Add table
	schema := table.NewSchema([]parser.ColumnDefinition{
		{Name: "id", Type: parser.TypeInteger},
	})

	tbl, _ := table.NewTable("temp", schema, pager)
	cat.AddTable("temp", tbl)

	if len(cat.ListTables()) != 1 {
		t.Error("Table should exist")
	}

	// Remove table
	if err := cat.RemoveTable("temp"); err != nil {
		t.Fatalf("Failed to remove table: %v", err)
	}

	if len(cat.ListTables()) != 0 {
		t.Error("Table should be removed")
	}

	_, ok := cat.GetTableInfo("temp")
	if ok {
		t.Error("Table info should not exist after removal")
	}
}

func TestCatalogLoadTable(t *testing.T) {
	testFile := "test_catalog_load.db"
	defer os.Remove(testFile)

	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	cat, err := NewCatalog(pager)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}

	// Add table
	schema := table.NewSchema([]parser.ColumnDefinition{
		{Name: "id", Type: parser.TypeInteger, PrimaryKey: true},
		{Name: "value", Type: parser.TypeText},
	})

	tbl, _ := table.NewTable("data", schema, pager)
	cat.AddTable("data", tbl)

	// Load table from catalog
	loaded, err := cat.LoadTable("data", pager)
	if err != nil {
		t.Fatalf("Failed to load table: %v", err)
	}

	if loaded.Name != "data" {
		t.Errorf("Expected name 'data', got %s", loaded.Name)
	}

	if len(loaded.Schema.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(loaded.Schema.Columns))
	}
}

func TestCatalogDataPageIDsPersistence(t *testing.T) {
	testFile := "test_catalog_datapages.db"
	defer os.Remove(testFile)

	// Create database, add table with data to populate dataPageIDs
	func() {
		pager, err := storage.NewPager(testFile)
		if err != nil {
			t.Fatalf("Failed to create pager: %v", err)
		}
		defer pager.Close()

		cat, err := NewCatalog(pager)
		if err != nil {
			t.Fatalf("Failed to create catalog: %v", err)
		}

		schema := table.NewSchema([]parser.ColumnDefinition{
			{Name: "id", Type: parser.TypeInteger, PrimaryKey: true},
			{Name: "data", Type: parser.TypeText},
		})

		tbl, err := table.NewTable("items", schema, pager)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert rows to create data pages
		for i := 0; i < 10; i++ {
			_, err := tbl.Insert([]table.Value{
				{Type: parser.TypeInteger, Integer: int64(i + 1)},
				{Type: parser.TypeText, Text: "test data"},
			})
			if err != nil {
				t.Fatalf("Failed to insert row: %v", err)
			}
		}

		// Verify dataPageIDs is non-empty before save
		dataPageIDs := tbl.GetDataPageIDs()
		if len(dataPageIDs) == 0 {
			t.Fatal("Expected dataPageIDs to be non-empty after inserts")
		}

		// Add to catalog
		if err := cat.AddTable("items", tbl); err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		cat.Flush()
	}()

	// Reopen and verify dataPageIDs is preserved
	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer pager.Close()

	cat, err := NewCatalog(pager)
	if err != nil {
		t.Fatalf("Failed to reload catalog: %v", err)
	}

	// Check TableInfo has dataPageIDs
	info, ok := cat.GetTableInfo("items")
	if !ok {
		t.Fatal("Table 'items' not found after reload")
	}

	if len(info.DataPageIDs) == 0 {
		t.Error("Expected DataPageIDs to be non-empty in TableInfo after reload")
	}

	// Load table and verify Scan works (uses dataPageIDs)
	tbl, err := cat.LoadTable("items", pager)
	if err != nil {
		t.Fatalf("Failed to load table: %v", err)
	}

	rows, err := tbl.Scan()
	if err != nil {
		t.Fatalf("Failed to scan table: %v", err)
	}

	if len(rows) != 10 {
		t.Errorf("Expected 10 rows after reload, got %d", len(rows))
	}
}

func TestCatalogNextRowIDPersistence(t *testing.T) {
	testFile := "test_catalog_rowid.db"
	defer os.Remove(testFile)

	// Create database, add table, insert rows to advance nextRowID
	func() {
		pager, err := storage.NewPager(testFile)
		if err != nil {
			t.Fatalf("Failed to create pager: %v", err)
		}
		defer pager.Close()

		cat, err := NewCatalog(pager)
		if err != nil {
			t.Fatalf("Failed to create catalog: %v", err)
		}

		schema := table.NewSchema([]parser.ColumnDefinition{
			{Name: "id", Type: parser.TypeInteger, PrimaryKey: true},
			{Name: "name", Type: parser.TypeText},
		})

		tbl, err := table.NewTable("users", schema, pager)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert 5 rows to advance nextRowID to 6
		for i := 0; i < 5; i++ {
			_, err := tbl.Insert([]table.Value{
				{Type: parser.TypeInteger, Integer: int64(i + 1)},
				{Type: parser.TypeText, Text: "user"},
			})
			if err != nil {
				t.Fatalf("Failed to insert row: %v", err)
			}
		}

		// Verify nextRowID is 6 before save
		if tbl.GetNextRowID() != 6 {
			t.Errorf("Expected nextRowID 6 before save, got %d", tbl.GetNextRowID())
		}

		// Add to catalog (should save nextRowID=6)
		if err := cat.AddTable("users", tbl); err != nil {
			t.Fatalf("Failed to add table: %v", err)
		}

		cat.Flush()
	}()

	// Reopen and verify nextRowID is preserved
	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer pager.Close()

	cat, err := NewCatalog(pager)
	if err != nil {
		t.Fatalf("Failed to reload catalog: %v", err)
	}

	// Check TableInfo has correct nextRowID
	info, ok := cat.GetTableInfo("users")
	if !ok {
		t.Fatal("Table 'users' not found after reload")
	}

	if info.NextRowID != 6 {
		t.Errorf("Expected NextRowID 6 in TableInfo after reload, got %d", info.NextRowID)
	}

	// Load table and verify nextRowID is restored
	tbl, err := cat.LoadTable("users", pager)
	if err != nil {
		t.Fatalf("Failed to load table: %v", err)
	}

	if tbl.GetNextRowID() != 6 {
		t.Errorf("Expected nextRowID 6 in loaded table, got %d", tbl.GetNextRowID())
	}

	// Insert new row - should get rowID 6, not 1
	rowID, err := tbl.Insert([]table.Value{
		{Type: parser.TypeInteger, Integer: 100},
		{Type: parser.TypeText, Text: "newuser"},
	})
	if err != nil {
		t.Fatalf("Failed to insert after reload: %v", err)
	}

	if rowID != 6 {
		t.Errorf("Expected new row to get rowID 6, got %d", rowID)
	}
}
