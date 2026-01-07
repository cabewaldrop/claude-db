// Package table implements table storage and row operations.
//
// EDUCATIONAL NOTES:
// ------------------
// A table is the fundamental structure for storing data in a relational database.
// Each table has:
// 1. A schema - defines the columns and their types
// 2. Rows - the actual data stored in the table
// 3. Indexes - structures like B-trees for fast lookup (optional)
//
// Our implementation stores rows as serialized byte slices in pages.
// Each row has a unique RowID that serves as its primary key for internal use.

package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/storage"
)

// Value represents a value that can be stored in a cell.
type Value struct {
	Type    parser.DataType
	IsNull  bool
	Integer int64
	Real    float64
	Text    string
	Boolean bool
}

// String returns a string representation of the value.
func (v Value) String() string {
	if v.IsNull {
		return "NULL"
	}
	switch v.Type {
	case parser.TypeInteger:
		return fmt.Sprintf("%d", v.Integer)
	case parser.TypeReal:
		return fmt.Sprintf("%g", v.Real)
	case parser.TypeText:
		return v.Text
	case parser.TypeBoolean:
		if v.Boolean {
			return "TRUE"
		}
		return "FALSE"
	default:
		return "?"
	}
}

// Compare compares two values. Returns -1, 0, or 1.
func (v Value) Compare(other Value) int {
	// NULL handling: NULL is considered less than any non-NULL value
	if v.IsNull && other.IsNull {
		return 0
	}
	if v.IsNull {
		return -1
	}
	if other.IsNull {
		return 1
	}

	switch v.Type {
	case parser.TypeInteger:
		if v.Integer < other.Integer {
			return -1
		} else if v.Integer > other.Integer {
			return 1
		}
		return 0
	case parser.TypeReal:
		if v.Real < other.Real {
			return -1
		} else if v.Real > other.Real {
			return 1
		}
		return 0
	case parser.TypeText:
		if v.Text < other.Text {
			return -1
		} else if v.Text > other.Text {
			return 1
		}
		return 0
	case parser.TypeBoolean:
		if !v.Boolean && other.Boolean {
			return -1
		} else if v.Boolean && !other.Boolean {
			return 1
		}
		return 0
	default:
		return 0
	}
}

// Equals checks if two values are equal.
func (v Value) Equals(other Value) bool {
	if v.IsNull && other.IsNull {
		return true
	}
	if v.IsNull || other.IsNull {
		return false
	}
	if v.Type != other.Type {
		return false
	}
	switch v.Type {
	case parser.TypeInteger:
		return v.Integer == other.Integer
	case parser.TypeReal:
		return v.Real == other.Real
	case parser.TypeText:
		return v.Text == other.Text
	case parser.TypeBoolean:
		return v.Boolean == other.Boolean
	default:
		return false
	}
}

// Row represents a single row in a table.
type Row struct {
	ID     uint64
	Values []Value
}

// Column represents a column definition.
type Column struct {
	Name       string
	Type       parser.DataType
	PrimaryKey bool
	NotNull    bool
}

// Schema defines the structure of a table.
type Schema struct {
	Columns      []Column
	PrimaryKey   int // Index of primary key column (-1 if none)
	ColumnLookup map[string]int
}

// NewSchema creates a new schema from column definitions.
func NewSchema(columns []parser.ColumnDefinition) *Schema {
	schema := &Schema{
		Columns:      make([]Column, len(columns)),
		PrimaryKey:   -1,
		ColumnLookup: make(map[string]int),
	}

	for i, col := range columns {
		schema.Columns[i] = Column{
			Name:       col.Name,
			Type:       col.Type,
			PrimaryKey: col.PrimaryKey,
			NotNull:    col.NotNull,
		}
		schema.ColumnLookup[col.Name] = i
		if col.PrimaryKey {
			schema.PrimaryKey = i
		}
	}

	return schema
}

// GetColumnIndex returns the index of a column by name.
func (s *Schema) GetColumnIndex(name string) (int, bool) {
	idx, ok := s.ColumnLookup[name]
	return idx, ok
}

// Table represents a database table with its schema and data.
// Table is safe for concurrent use by multiple goroutines.
type Table struct {
	mu     sync.RWMutex // Protects all fields below
	Name   string
	Schema *Schema

	// Storage
	pager        *storage.Pager
	btree        *storage.BTree
	nextRowID    uint64
	dataPageIDs  []uint32 // List of data page IDs
	metadataPage uint32   // Page storing table metadata

	// Secondary indexes
	indexes map[string]*storage.Index // Indexed by index name
}

// TableMetadata stores table information for persistence.
type TableMetadata struct {
	Name        string
	RootPage    uint32
	NextRowID   uint64
	ColumnCount int
}

// NewTable creates a new table with the given schema.
func NewTable(name string, schema *Schema, pager *storage.Pager) (*Table, error) {
	// Create B-tree for primary key index
	btree, err := storage.NewBTree(pager)
	if err != nil {
		return nil, fmt.Errorf("failed to create B-tree: %w", err)
	}

	// Allocate metadata page
	metaPage, err := pager.AllocatePage(storage.PageTypeData)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate metadata page: %w", err)
	}

	table := &Table{
		Name:         name,
		Schema:       schema,
		pager:        pager,
		btree:        btree,
		nextRowID:    1,
		dataPageIDs:  []uint32{},
		metadataPage: metaPage.ID(),
		indexes:      make(map[string]*storage.Index),
	}

	return table, nil
}

// LoadTable loads an existing table from storage.
func LoadTable(name string, schema *Schema, pager *storage.Pager, rootPage uint32, nextRowID uint64, dataPageIDs []uint32) *Table {
	return &Table{
		Name:        name,
		Schema:      schema,
		pager:       pager,
		btree:       storage.LoadBTree(pager, rootPage),
		nextRowID:   nextRowID,
		dataPageIDs: dataPageIDs,
		indexes:     make(map[string]*storage.Index),
	}
}

// Insert adds a new row to the table.
//
// EDUCATIONAL NOTE:
// -----------------
// Inserting a row involves:
// 1. Validate values against schema
// 2. Assign a row ID
// 3. Serialize the row to bytes
// 4. Store in a data page
// 5. Add to primary key index (B-tree)
// 6. Add to all secondary indexes
func (t *Table) Insert(values []Value) (uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Validate column count
	if len(values) != len(t.Schema.Columns) {
		return 0, fmt.Errorf("expected %d values, got %d", len(t.Schema.Columns), len(values))
	}

	// Validate types
	for i, val := range values {
		col := t.Schema.Columns[i]
		if !val.IsNull && val.Type != col.Type {
			return 0, fmt.Errorf("column %s expects %s, got %s", col.Name, col.Type, val.Type)
		}
		if val.IsNull && col.NotNull {
			return 0, fmt.Errorf("column %s cannot be NULL", col.Name)
		}
	}

	// Assign row ID
	rowID := t.nextRowID
	t.nextRowID++

	// Serialize row
	rowData, err := t.serializeRow(rowID, values)
	if err != nil {
		return 0, fmt.Errorf("failed to serialize row: %w", err)
	}

	// Store row data
	pageID, offset, err := t.storeRowData(rowData)
	if err != nil {
		return 0, fmt.Errorf("failed to store row data: %w", err)
	}

	// Create key for B-tree (use primary key value or row ID)
	var keyBytes []byte
	if t.Schema.PrimaryKey >= 0 {
		keyBytes, err = t.valueToBytes(values[t.Schema.PrimaryKey])
		if err != nil {
			return 0, fmt.Errorf("failed to serialize primary key: %w", err)
		}
	} else {
		keyBytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(keyBytes, rowID)
	}

	// Store location in B-tree: encode page ID and offset into uint64
	location := uint64(pageID)<<32 | uint64(offset)
	if err := t.btree.Insert(keyBytes, location); err != nil {
		return 0, fmt.Errorf("failed to insert into index: %w", err)
	}

	// Update secondary indexes
	for _, idx := range t.indexes {
		// Build index key from indexed column values
		columnIndices := make([]int, len(idx.Columns))
		for i, colName := range idx.Columns {
			colIdx, _ := t.Schema.GetColumnIndex(colName)
			columnIndices[i] = colIdx
		}

		buf := bytes.NewBuffer(nil)
		for _, colIdx := range columnIndices {
			t.serializeValue(buf, values[colIdx])
		}
		indexKey := buf.Bytes()

		if err := idx.Insert(indexKey, location); err != nil {
			return 0, fmt.Errorf("failed to update secondary index %s: %w", idx.Name, err)
		}
	}

	return rowID, nil
}

// Scan returns all rows in the table.
func (t *Table) Scan() ([]Row, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var rows []Row

	// Iterate through all data pages
	for _, pageID := range t.dataPageIDs {
		page, err := t.pager.GetPage(pageID)
		if err != nil {
			return nil, err
		}

		// Read rows from page
		pageRows, err := t.readRowsFromPage(page)
		if err != nil {
			return nil, err
		}
		rows = append(rows, pageRows...)
	}

	return rows, nil
}

// ScanWithFilter returns rows that match the filter function.
// Unlike Scan(), this filters during page iteration to reduce memory allocations.
// If limit > 0, returns at most that many matching rows (enables early exit).
//
// EDUCATIONAL NOTE:
// -----------------
// Filtering during iteration (push-down) is more efficient than loading
// all rows then filtering (pull-up) because:
// 1. Memory is only allocated for matching rows
// 2. With a LIMIT, we can stop early once satisfied
// 3. CPU cache is used more efficiently (fewer allocations)
func (t *Table) ScanWithFilter(filter func(Row) bool, limit int) ([]Row, error) {
	var rows []Row

	// Iterate through all data pages
	for _, pageID := range t.dataPageIDs {
		page, err := t.pager.GetPage(pageID)
		if err != nil {
			return nil, err
		}

		// Read and filter rows from page
		pageRows, err := t.readRowsFromPage(page)
		if err != nil {
			return nil, err
		}

		for _, row := range pageRows {
			if filter(row) {
				rows = append(rows, row)
				// Early exit if limit is satisfied
				if limit > 0 && len(rows) >= limit {
					return rows, nil
				}
			}
		}
	}

	return rows, nil
}

// serializeRow converts a row to bytes.
func (t *Table) serializeRow(rowID uint64, values []Value) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// Write row ID
	if err := binary.Write(buf, binary.LittleEndian, rowID); err != nil {
		return nil, fmt.Errorf("writing row ID: %w", err)
	}

	// Write number of values
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(values))); err != nil {
		return nil, fmt.Errorf("writing value count: %w", err)
	}

	// Write each value
	for i, val := range values {
		if err := t.serializeValue(buf, val); err != nil {
			return nil, fmt.Errorf("serializing value %d: %w", i, err)
		}
	}

	return buf.Bytes(), nil
}

// serializeValue writes a value to the buffer.
func (t *Table) serializeValue(buf *bytes.Buffer, val Value) error {
	// Write type and null flag
	buf.WriteByte(byte(val.Type))
	if val.IsNull {
		buf.WriteByte(1)
		return nil
	}
	buf.WriteByte(0)

	// Write value based on type
	switch val.Type {
	case parser.TypeInteger:
		if err := binary.Write(buf, binary.LittleEndian, val.Integer); err != nil {
			return fmt.Errorf("writing integer value: %w", err)
		}
	case parser.TypeReal:
		if err := binary.Write(buf, binary.LittleEndian, val.Real); err != nil {
			return fmt.Errorf("writing real value: %w", err)
		}
	case parser.TypeText:
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(val.Text))); err != nil {
			return fmt.Errorf("writing text length: %w", err)
		}
		buf.WriteString(val.Text)
	case parser.TypeBoolean:
		if val.Boolean {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
	default:
		return fmt.Errorf("unsupported type for serialization: %v", val.Type)
	}
	return nil
}

// deserializeRow reads a row from bytes.
func (t *Table) deserializeRow(data []byte) (Row, error) {
	buf := bytes.NewReader(data)
	row := Row{}

	// Read row ID
	if err := binary.Read(buf, binary.LittleEndian, &row.ID); err != nil {
		return row, err
	}

	// Read number of values
	var numValues uint16
	if err := binary.Read(buf, binary.LittleEndian, &numValues); err != nil {
		return row, err
	}

	// Read each value
	row.Values = make([]Value, numValues)
	for i := uint16(0); i < numValues; i++ {
		val, err := t.deserializeValue(buf)
		if err != nil {
			return row, err
		}
		row.Values[i] = val
	}

	return row, nil
}

// deserializeValue reads a value from the buffer.
func (t *Table) deserializeValue(buf *bytes.Reader) (Value, error) {
	val := Value{}

	// Read type
	typeByte, err := buf.ReadByte()
	if err != nil {
		return val, err
	}
	val.Type = parser.DataType(typeByte)

	// Read null flag
	nullByte, err := buf.ReadByte()
	if err != nil {
		return val, err
	}
	if nullByte == 1 {
		val.IsNull = true
		return val, nil
	}

	// Read value based on type
	switch val.Type {
	case parser.TypeInteger:
		if err := binary.Read(buf, binary.LittleEndian, &val.Integer); err != nil {
			return val, err
		}
	case parser.TypeReal:
		if err := binary.Read(buf, binary.LittleEndian, &val.Real); err != nil {
			return val, err
		}
	case parser.TypeText:
		var length uint16
		if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
			return val, err
		}
		textBytes := make([]byte, length)
		if _, err := buf.Read(textBytes); err != nil {
			return val, err
		}
		val.Text = string(textBytes)
	case parser.TypeBoolean:
		boolByte, err := buf.ReadByte()
		if err != nil {
			return val, err
		}
		val.Boolean = boolByte == 1
	}

	return val, nil
}

// storeRowData stores row data in a data page.
func (t *Table) storeRowData(data []byte) (uint32, uint16, error) {
	// Try to fit in existing pages
	for _, pageID := range t.dataPageIDs {
		page, err := t.pager.GetPage(pageID)
		if err != nil {
			continue
		}

		if int(page.FreeSpace()) >= len(data)+2 { // +2 for length prefix
			offset, err := t.writeRowToPage(page, data)
			if err == nil {
				return pageID, offset, nil
			}
		}
	}

	// Need new page
	page, err := t.pager.AllocatePage(storage.PageTypeData)
	if err != nil {
		return 0, 0, err
	}
	t.dataPageIDs = append(t.dataPageIDs, page.ID())

	offset, err := t.writeRowToPage(page, data)
	if err != nil {
		return 0, 0, err
	}

	return page.ID(), offset, nil
}

// writeRowToPage writes a row to a page with length prefix.
func (t *Table) writeRowToPage(page *storage.Page, data []byte) (uint16, error) {
	// Prefix with length
	lengthPrefixed := make([]byte, 2+len(data))
	binary.LittleEndian.PutUint16(lengthPrefixed, uint16(len(data)))
	copy(lengthPrefixed[2:], data)

	return page.WriteData(lengthPrefixed)
}

// readRowsFromPage reads all rows from a data page.
func (t *Table) readRowsFromPage(page *storage.Page) ([]Row, error) {
	var rows []Row
	data := page.GetData()
	offset := 0

	numSlots := int(page.NumSlots())
	for i := 0; i < numSlots && offset < len(data)-1; i++ {
		// Read length
		length := binary.LittleEndian.Uint16(data[offset:])
		if length == 0 {
			break
		}
		offset += 2

		// Read row data
		rowData := data[offset : offset+int(length)]
		row, err := t.deserializeRow(rowData)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		offset += int(length)
	}

	return rows, nil
}

// valueToBytes converts a value to bytes for use as B-tree key.
func (t *Table) valueToBytes(val Value) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := t.serializeValue(buf, val); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Update modifies rows matching the filter.
func (t *Table) Update(assignments map[string]Value, filter func(Row) bool) (int, error) {
	// This is a simplified implementation - in a real database,
	// we'd update in place or use a more sophisticated approach
	rows, err := t.Scan()
	if err != nil {
		return 0, err
	}

	count := 0
	for i := range rows {
		if filter(rows[i]) {
			for colName, newVal := range assignments {
				if colIdx, ok := t.Schema.GetColumnIndex(colName); ok {
					rows[i].Values[colIdx] = newVal
				}
			}
			count++
		}
	}

	return count, nil
}

// Delete removes rows matching the filter.
func (t *Table) Delete(filter func(Row) bool) (int, error) {
	// Simplified implementation - marks rows as deleted
	// A real implementation would handle space reclamation
	return 0, errors.New("delete not fully implemented yet")
}

// GetRootPage returns the B-tree root page for persistence.
func (t *Table) GetRootPage() uint32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.btree.RootPage()
}

// GetNextRowID returns the next row ID for persistence.
func (t *Table) GetNextRowID() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.nextRowID
}

// GetDataPageIDs returns the list of data page IDs for persistence.
func (t *Table) GetDataPageIDs() []uint32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// Return a copy to prevent caller from modifying internal state
	result := make([]uint32, len(t.dataPageIDs))
	copy(result, t.dataPageIDs)
	return result
}

// GetRowByPrimaryKey retrieves a row by its primary key value.
//
// EDUCATIONAL NOTE:
// -----------------
// This method uses the B-tree index for O(log n) lookup instead of
// scanning the entire table. This is the key optimization that makes
// indexed lookups fast:
// 1. Convert the key value to bytes
// 2. Search the B-tree to find the row's location
// 3. Fetch the row directly using GetRowByLocation
func (t *Table) GetRowByPrimaryKey(keyValue Value) (Row, bool, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Check if table has a primary key
	if t.Schema.PrimaryKey < 0 {
		return Row{}, false, errors.New("table has no primary key")
	}

	// Convert value to bytes for B-tree lookup
	keyBytes, err := t.valueToBytes(keyValue)
	if err != nil {
		return Row{}, false, fmt.Errorf("failed to serialize key: %w", err)
	}

	// Search the B-tree index
	location, found, err := t.btree.Search(keyBytes)
	if err != nil {
		return Row{}, false, fmt.Errorf("index search failed: %w", err)
	}
	if !found {
		return Row{}, false, nil
	}

	// Fetch the row by location (already holding lock)
	row, err := t.getRowByLocationLocked(location)
	if err != nil {
		return Row{}, false, fmt.Errorf("failed to fetch row: %w", err)
	}

	return row, true, nil
}

// GetRowByLocation retrieves a row by its storage location.
//
// EDUCATIONAL NOTE:
// -----------------
// The location is a uint64 that encodes both the page ID and offset:
//   - Upper 32 bits: page ID
//   - Lower 32 bits: offset within the page
//
// This allows efficient O(1) row retrieval when we know the location
// from an index lookup, avoiding a full table scan.
func (t *Table) GetRowByLocation(location uint64) (Row, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.getRowByLocationLocked(location)
}

// getRowByLocationLocked is the internal implementation that assumes the lock is held.
func (t *Table) getRowByLocationLocked(location uint64) (Row, error) {
	// Extract page ID and offset from location
	pageID := uint32(location >> 32)
	offset := uint16(location & 0xFFFFFFFF)

	// Fetch the page
	page, err := t.pager.GetPage(pageID)
	if err != nil {
		return Row{}, fmt.Errorf("failed to get page %d: %w", pageID, err)
	}

	data := page.GetData()

	// Validate offset is within bounds
	if int(offset)+2 > len(data) {
		return Row{}, fmt.Errorf("invalid offset %d: exceeds page data bounds", offset)
	}

	// Read the row length (2-byte prefix)
	length := binary.LittleEndian.Uint16(data[offset:])
	if length == 0 {
		return Row{}, errors.New("invalid row: zero length")
	}

	// Validate we have enough data for the row
	rowStart := int(offset) + 2
	rowEnd := rowStart + int(length)
	if rowEnd > len(data) {
		return Row{}, fmt.Errorf("invalid row length %d at offset %d: exceeds page bounds", length, offset)
	}

	// Read and deserialize the row
	rowData := data[rowStart:rowEnd]
	row, err := t.deserializeRow(rowData)
	if err != nil {
		return Row{}, fmt.Errorf("failed to deserialize row: %w", err)
	}

	return row, nil
}

// ============================================================================
// Secondary Index Management
// ============================================================================

// CreateIndex creates a new secondary index on the specified columns.
//
// EDUCATIONAL NOTE:
// -----------------
// Creating an index involves:
// 1. Creating a new B-tree for the index
// 2. Populating it with entries for all existing rows
// 3. Registering it so future INSERTs/UPDATEs maintain it
func (t *Table) CreateIndex(name string, columns []string, unique bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if index already exists
	if _, exists := t.indexes[name]; exists {
		return fmt.Errorf("index %s already exists", name)
	}

	// Validate columns exist
	columnIndices := make([]int, len(columns))
	for i, colName := range columns {
		idx, ok := t.Schema.GetColumnIndex(colName)
		if !ok {
			return fmt.Errorf("column %s does not exist", colName)
		}
		columnIndices[i] = idx
	}

	// Create the index
	idx, err := storage.NewIndex(name, t.Name, columns, unique, t.pager)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	// Populate index with existing rows
	for _, pageID := range t.dataPageIDs {
		page, err := t.pager.GetPage(pageID)
		if err != nil {
			return err
		}

		rows, err := t.readRowsFromPage(page)
		if err != nil {
			return err
		}

		// Find where each row is stored to get its location
		for _, row := range rows {
			// Build index key from column values
			keyBytes := t.buildIndexKey(row, columnIndices)

			// Get the row location from the primary index
			var pkKeyBytes []byte
			if t.Schema.PrimaryKey >= 0 {
				var err error
				pkKeyBytes, err = t.valueToBytes(row.Values[t.Schema.PrimaryKey])
				if err != nil {
					return fmt.Errorf("failed to serialize primary key: %w", err)
				}
			} else {
				pkKeyBytes = make([]byte, 8)
				binary.LittleEndian.PutUint64(pkKeyBytes, row.ID)
			}

			location, found, err := t.btree.Search(pkKeyBytes)
			if err != nil {
				return fmt.Errorf("failed to get row location: %w", err)
			}
			if !found {
				continue // Row might have been deleted
			}

			// Add to index
			if err := idx.Insert(keyBytes, location); err != nil {
				return fmt.Errorf("failed to add row to index: %w", err)
			}
		}
	}

	t.indexes[name] = idx
	return nil
}

// DropIndex removes a secondary index.
func (t *Table) DropIndex(name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.indexes[name]; !exists {
		return fmt.Errorf("index %s does not exist", name)
	}

	delete(t.indexes, name)
	return nil
}

// GetIndex returns an index by name.
func (t *Table) GetIndex(name string) (*storage.Index, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	idx, ok := t.indexes[name]
	return idx, ok
}

// GetIndexForColumn returns an index that covers the given column.
// Returns the first single-column index found, or nil if none exists.
func (t *Table) GetIndexForColumn(columnName string) *storage.Index {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, idx := range t.indexes {
		if len(idx.Columns) == 1 && idx.Columns[0] == columnName {
			return idx
		}
	}
	return nil
}

// ListIndexes returns the names of all indexes on this table.
func (t *Table) ListIndexes() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	names := make([]string, 0, len(t.indexes))
	for name := range t.indexes {
		names = append(names, name)
	}
	return names
}

// AddIndex adds an existing index to the table (for loading from storage).
func (t *Table) AddIndex(idx *storage.Index) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.indexes[idx.Name] = idx
}

// buildIndexKey creates a B-tree key from the specified column values of a row.
func (t *Table) buildIndexKey(row Row, columnIndices []int) []byte {
	buf := bytes.NewBuffer(nil)
	for _, colIdx := range columnIndices {
		t.serializeValue(buf, row.Values[colIdx])
	}
	return buf.Bytes()
}

// GetPager returns the pager for use by index operations.
func (t *Table) GetPager() *storage.Pager {
	return t.pager
}
