// Package catalog manages the database catalog (metadata about tables).
//
// EDUCATIONAL NOTES:
// ------------------
// Every database has a "catalog" or "system tables" that store metadata:
// - What tables exist
// - What columns each table has
// - Column types and constraints
// - Index information
//
// In production databases like PostgreSQL, this is stored in special
// system tables (pg_class, pg_attribute, etc.). SQLite stores it in
// sqlite_master.
//
// Our catalog uses page 0 as a special "catalog page" that stores:
// - Number of tables
// - For each table: name, schema, root page ID

package catalog

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/storage"
	"github.com/cabewaldrop/claude-db/internal/table"
)

const (
	// CatalogPageID is the page where catalog metadata is stored
	CatalogPageID = 0

	// Magic number to identify a valid catalog
	CatalogMagic = 0xCDB0 // "CDB" for Claude DB
)

// TableInfo stores metadata about a table for persistence.
type TableInfo struct {
	Name       string
	RootPage   uint32
	NextRowID  uint64
	Columns    []ColumnInfo
	PrimaryKey int
}

// ColumnInfo stores column metadata.
type ColumnInfo struct {
	Name       string
	Type       parser.DataType
	PrimaryKey bool
	NotNull    bool
}

// Catalog manages database metadata.
type Catalog struct {
	pager  *storage.Pager
	tables map[string]*TableInfo
}

// NewCatalog creates or loads a catalog from the pager.
func NewCatalog(pager *storage.Pager) (*Catalog, error) {
	c := &Catalog{
		pager:  pager,
		tables: make(map[string]*TableInfo),
	}

	// Check if catalog page exists
	if pager.PageCount() == 0 {
		// New database - initialize catalog
		if err := c.initializeCatalog(); err != nil {
			return nil, err
		}
	} else {
		// Existing database - load catalog
		if err := c.loadCatalog(); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// initializeCatalog creates the catalog page for a new database.
func (c *Catalog) initializeCatalog() error {
	page, err := c.pager.AllocatePage(storage.PageTypeData)
	if err != nil {
		return fmt.Errorf("failed to allocate catalog page: %w", err)
	}

	if page.ID() != CatalogPageID {
		return fmt.Errorf("catalog page should be page 0, got %d", page.ID())
	}

	return c.saveCatalog()
}

// loadCatalog reads the catalog from disk.
func (c *Catalog) loadCatalog() error {
	page, err := c.pager.GetPage(CatalogPageID)
	if err != nil {
		return fmt.Errorf("failed to read catalog page: %w", err)
	}

	data := page.GetData()
	buf := bytes.NewReader(data)

	// Read and verify magic number
	var magic uint16
	if err := binary.Read(buf, binary.LittleEndian, &magic); err != nil {
		return fmt.Errorf("failed to read catalog magic: %w", err)
	}

	if magic != CatalogMagic {
		// Not a valid catalog - might be a new or corrupted database
		// Initialize fresh catalog
		return c.saveCatalog()
	}

	// Read number of tables
	var numTables uint16
	if err := binary.Read(buf, binary.LittleEndian, &numTables); err != nil {
		return fmt.Errorf("failed to read table count: %w", err)
	}

	// Read each table's metadata
	for i := uint16(0); i < numTables; i++ {
		info, err := c.readTableInfo(buf)
		if err != nil {
			return fmt.Errorf("failed to read table %d: %w", i, err)
		}
		c.tables[info.Name] = info
	}

	return nil
}

// saveCatalog writes the catalog to disk.
func (c *Catalog) saveCatalog() error {
	page, err := c.pager.GetPage(CatalogPageID)
	if err != nil {
		return fmt.Errorf("failed to get catalog page: %w", err)
	}

	buf := bytes.NewBuffer(nil)

	// Write magic number
	binary.Write(buf, binary.LittleEndian, uint16(CatalogMagic))

	// Write number of tables
	binary.Write(buf, binary.LittleEndian, uint16(len(c.tables)))

	// Write each table's metadata
	for _, info := range c.tables {
		if err := c.writeTableInfo(buf, info); err != nil {
			return err
		}
	}

	if buf.Len() > storage.MaxDataSize {
		return fmt.Errorf("catalog too large: %d bytes", buf.Len())
	}

	return page.SetData(buf.Bytes())
}

// readTableInfo reads a TableInfo from the buffer.
func (c *Catalog) readTableInfo(buf *bytes.Reader) (*TableInfo, error) {
	info := &TableInfo{}

	// Read name length and name
	var nameLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
		return nil, err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := buf.Read(nameBytes); err != nil {
		return nil, err
	}
	info.Name = string(nameBytes)

	// Read root page and next row ID
	if err := binary.Read(buf, binary.LittleEndian, &info.RootPage); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &info.NextRowID); err != nil {
		return nil, err
	}

	// Read primary key index
	var pkIdx int32
	if err := binary.Read(buf, binary.LittleEndian, &pkIdx); err != nil {
		return nil, err
	}
	info.PrimaryKey = int(pkIdx)

	// Read columns
	var numCols uint16
	if err := binary.Read(buf, binary.LittleEndian, &numCols); err != nil {
		return nil, err
	}

	info.Columns = make([]ColumnInfo, numCols)
	for i := uint16(0); i < numCols; i++ {
		col, err := c.readColumnInfo(buf)
		if err != nil {
			return nil, err
		}
		info.Columns[i] = col
	}

	return info, nil
}

// writeTableInfo writes a TableInfo to the buffer.
func (c *Catalog) writeTableInfo(buf *bytes.Buffer, info *TableInfo) error {
	// Write name
	binary.Write(buf, binary.LittleEndian, uint16(len(info.Name)))
	buf.WriteString(info.Name)

	// Write root page and next row ID
	binary.Write(buf, binary.LittleEndian, info.RootPage)
	binary.Write(buf, binary.LittleEndian, info.NextRowID)

	// Write primary key index
	binary.Write(buf, binary.LittleEndian, int32(info.PrimaryKey))

	// Write columns
	binary.Write(buf, binary.LittleEndian, uint16(len(info.Columns)))
	for _, col := range info.Columns {
		if err := c.writeColumnInfo(buf, col); err != nil {
			return err
		}
	}

	return nil
}

// readColumnInfo reads a ColumnInfo from the buffer.
func (c *Catalog) readColumnInfo(buf *bytes.Reader) (ColumnInfo, error) {
	col := ColumnInfo{}

	// Read name
	var nameLen uint16
	if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
		return col, err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := buf.Read(nameBytes); err != nil {
		return col, err
	}
	col.Name = string(nameBytes)

	// Read type
	var colType uint8
	if err := binary.Read(buf, binary.LittleEndian, &colType); err != nil {
		return col, err
	}
	col.Type = parser.DataType(colType)

	// Read flags
	var flags uint8
	if err := binary.Read(buf, binary.LittleEndian, &flags); err != nil {
		return col, err
	}
	col.PrimaryKey = (flags & 0x01) != 0
	col.NotNull = (flags & 0x02) != 0

	return col, nil
}

// writeColumnInfo writes a ColumnInfo to the buffer.
func (c *Catalog) writeColumnInfo(buf *bytes.Buffer, col ColumnInfo) error {
	// Write name
	binary.Write(buf, binary.LittleEndian, uint16(len(col.Name)))
	buf.WriteString(col.Name)

	// Write type
	binary.Write(buf, binary.LittleEndian, uint8(col.Type))

	// Write flags
	var flags uint8
	if col.PrimaryKey {
		flags |= 0x01
	}
	if col.NotNull {
		flags |= 0x02
	}
	binary.Write(buf, binary.LittleEndian, flags)

	return nil
}

// AddTable registers a new table in the catalog.
func (c *Catalog) AddTable(name string, tbl *table.Table) error {
	info := &TableInfo{
		Name:       name,
		RootPage:   tbl.GetRootPage(),
		NextRowID:  1,
		PrimaryKey: tbl.Schema.PrimaryKey,
		Columns:    make([]ColumnInfo, len(tbl.Schema.Columns)),
	}

	for i, col := range tbl.Schema.Columns {
		info.Columns[i] = ColumnInfo{
			Name:       col.Name,
			Type:       col.Type,
			PrimaryKey: col.PrimaryKey,
			NotNull:    col.NotNull,
		}
	}

	c.tables[name] = info
	return c.saveCatalog()
}

// RemoveTable removes a table from the catalog.
func (c *Catalog) RemoveTable(name string) error {
	delete(c.tables, name)
	return c.saveCatalog()
}

// GetTableInfo returns info about a table.
func (c *Catalog) GetTableInfo(name string) (*TableInfo, bool) {
	info, ok := c.tables[name]
	return info, ok
}

// ListTables returns all table names.
func (c *Catalog) ListTables() []string {
	names := make([]string, 0, len(c.tables))
	for name := range c.tables {
		names = append(names, name)
	}
	return names
}

// LoadTable creates a Table object from catalog info.
func (c *Catalog) LoadTable(name string, pager *storage.Pager) (*table.Table, error) {
	info, ok := c.tables[name]
	if !ok {
		return nil, fmt.Errorf("table %s not found", name)
	}

	// Convert ColumnInfo to parser.ColumnDefinition
	columns := make([]parser.ColumnDefinition, len(info.Columns))
	for i, col := range info.Columns {
		columns[i] = parser.ColumnDefinition{
			Name:       col.Name,
			Type:       col.Type,
			PrimaryKey: col.PrimaryKey,
			NotNull:    col.NotNull,
		}
	}

	schema := table.NewSchema(columns)
	return table.LoadTable(name, schema, pager, info.RootPage), nil
}

// Flush ensures all catalog changes are written to disk.
func (c *Catalog) Flush() error {
	return c.pager.FlushAll()
}
