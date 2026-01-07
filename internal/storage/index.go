// Package storage - Secondary Index implementation
//
// EDUCATIONAL NOTES:
// ------------------
// Secondary indexes allow efficient lookups on non-primary-key columns.
// Without an index on a column, finding rows requires a full table scan.
// With an index, the database can use a B-tree to find matching rows directly.
//
// For example, if we have a table users(id, name, age) with primary key id,
// a secondary index on 'age' would allow:
//   SELECT * FROM users WHERE age = 25
// to be answered in O(log n) time instead of O(n).
//
// Secondary indexes map: column_value -> primary_key (or row location)
// This allows the database to:
// 1. Look up the column value in the secondary index B-tree
// 2. Get the primary key(s) or row location(s)
// 3. Fetch the full row(s) using the primary key index

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// Index represents a secondary index on one or more columns.
//
// EDUCATIONAL NOTE:
// -----------------
// Each secondary index maintains a B-tree where:
// - Keys are the indexed column value(s), serialized to bytes
// - Values are the row locations (same format as primary key index)
//
// For non-unique indexes, the same key can have multiple values.
// We handle this by appending the row location to the key, making
// each entry unique while still allowing efficient range scans.
type Index struct {
	Name      string   // Index name (e.g., idx_users_age)
	Table     string   // Table this index belongs to
	Columns   []string // Columns included in the index (in order)
	Unique    bool     // Whether the index enforces uniqueness
	btree     *BTree   // The B-tree storing the index entries
	rootPage  uint32   // Root page of the B-tree
}

// IndexMetadata stores index information for persistence.
type IndexMetadata struct {
	Name     string
	Table    string
	Columns  []string
	Unique   bool
	RootPage uint32
}

// NewIndex creates a new secondary index.
func NewIndex(name, tableName string, columns []string, unique bool, pager *Pager) (*Index, error) {
	btree, err := NewBTree(pager)
	if err != nil {
		return nil, fmt.Errorf("failed to create B-tree for index: %w", err)
	}

	return &Index{
		Name:     name,
		Table:    tableName,
		Columns:  columns,
		Unique:   unique,
		btree:    btree,
		rootPage: btree.RootPage(),
	}, nil
}

// LoadIndex loads an existing index from storage.
func LoadIndex(name, tableName string, columns []string, unique bool, pager *Pager, rootPage uint32) *Index {
	return &Index{
		Name:     name,
		Table:    tableName,
		Columns:  columns,
		Unique:   unique,
		btree:    LoadBTree(pager, rootPage),
		rootPage: rootPage,
	}
}

// Insert adds an entry to the index.
// keyBytes is the serialized column value(s).
// location is the row location (same format as primary key index).
func (idx *Index) Insert(keyBytes []byte, location uint64) error {
	// For non-unique indexes, append location to key to make it unique
	var indexKey []byte
	if idx.Unique {
		indexKey = keyBytes
	} else {
		// Append location to make key unique
		indexKey = make([]byte, len(keyBytes)+8)
		copy(indexKey, keyBytes)
		binary.BigEndian.PutUint64(indexKey[len(keyBytes):], location)
	}

	// For unique indexes, check if key already exists
	if idx.Unique {
		_, found, err := idx.btree.Search(indexKey)
		if err != nil {
			return fmt.Errorf("failed to check uniqueness: %w", err)
		}
		if found {
			return fmt.Errorf("duplicate key value violates unique constraint %q", idx.Name)
		}
	}

	return idx.btree.Insert(indexKey, location)
}

// Delete removes an entry from the index.
// For non-unique indexes, the location is needed to identify the specific entry.
func (idx *Index) Delete(keyBytes []byte, location uint64) error {
	// For non-unique indexes, we need to include location in the key
	var indexKey []byte
	if idx.Unique {
		indexKey = keyBytes
	} else {
		indexKey = make([]byte, len(keyBytes)+8)
		copy(indexKey, keyBytes)
		binary.BigEndian.PutUint64(indexKey[len(keyBytes):], location)
	}

	// Note: B-tree doesn't currently support delete, so we mark this as a TODO
	// In a production system, we'd implement B-tree delete or use tombstones
	_ = indexKey
	return nil // TODO: Implement delete when B-tree supports it
}

// Lookup finds all row locations matching the exact key value.
func (idx *Index) Lookup(keyBytes []byte) ([]uint64, error) {
	if idx.Unique {
		// For unique indexes, there's at most one match
		location, found, err := idx.btree.Search(keyBytes)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, nil
		}
		return []uint64{location}, nil
	}

	// For non-unique indexes, scan range with key prefix
	return idx.scanWithPrefix(keyBytes)
}

// scanWithPrefix finds all entries with keys starting with the given prefix.
// Used for non-unique indexes where multiple rows can have the same column value.
func (idx *Index) scanWithPrefix(prefix []byte) ([]uint64, error) {
	// Create start and end keys for range scan
	startKey := prefix

	// End key is prefix with last byte incremented (or nil if would overflow)
	endKey := make([]byte, len(prefix))
	copy(endKey, prefix)

	// Increment the end key to get exclusive upper bound
	carry := true
	for i := len(endKey) - 1; i >= 0 && carry; i-- {
		if endKey[i] < 255 {
			endKey[i]++
			carry = false
		} else {
			endKey[i] = 0
		}
	}
	if carry {
		// All bytes were 255, scan to end
		endKey = nil
	}

	// Use range scan
	iter := idx.btree.NewRangeIteratorWithOptions(startKey, endKey, RangeScanOptions{
		StartInclusive: true,
		EndInclusive:   false, // End is exclusive
		Limit:          0,
	})
	defer iter.Close()

	var locations []uint64
	for iter.Next() {
		key := iter.Key()
		// Verify the key actually starts with our prefix
		if len(key) >= len(prefix) && bytes.Equal(key[:len(prefix)], prefix) {
			locations = append(locations, iter.Value())
		} else {
			// Past our prefix range
			break
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return locations, nil
}

// RangeScan returns all row locations where the indexed column is in [start, end].
// Pass nil for start to scan from the beginning.
// Pass nil for end to scan to the end.
func (idx *Index) RangeScan(startKey, endKey []byte) ([]uint64, error) {
	iter := idx.btree.NewRangeIterator(startKey, endKey)
	defer iter.Close()

	var locations []uint64
	for iter.Next() {
		locations = append(locations, iter.Value())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return locations, nil
}

// RootPage returns the root page ID for persistence.
func (idx *Index) RootPage() uint32 {
	return idx.rootPage
}

// IndexManager manages all secondary indexes for a table.
//
// EDUCATIONAL NOTE:
// -----------------
// The IndexManager is responsible for:
// 1. Creating and dropping indexes
// 2. Maintaining indexes when rows are inserted/updated/deleted
// 3. Providing access to indexes for query planning and execution
type IndexManager struct {
	pager   *Pager
	indexes map[string]*Index // Indexed by index name
}

// NewIndexManager creates a new index manager.
func NewIndexManager(pager *Pager) *IndexManager {
	return &IndexManager{
		pager:   pager,
		indexes: make(map[string]*Index),
	}
}

// CreateIndex creates a new secondary index.
func (im *IndexManager) CreateIndex(name, tableName string, columns []string, unique bool) (*Index, error) {
	if _, exists := im.indexes[name]; exists {
		return nil, fmt.Errorf("index %s already exists", name)
	}

	idx, err := NewIndex(name, tableName, columns, unique, im.pager)
	if err != nil {
		return nil, err
	}

	im.indexes[name] = idx
	return idx, nil
}

// DropIndex removes an index.
func (im *IndexManager) DropIndex(name string) error {
	if _, exists := im.indexes[name]; !exists {
		return fmt.Errorf("index %s does not exist", name)
	}

	delete(im.indexes, name)
	// Note: In a real system, we'd also free the B-tree pages
	return nil
}

// GetIndex returns an index by name.
func (im *IndexManager) GetIndex(name string) (*Index, bool) {
	idx, ok := im.indexes[name]
	return idx, ok
}

// GetIndexesForTable returns all indexes for a given table.
func (im *IndexManager) GetIndexesForTable(tableName string) []*Index {
	var result []*Index
	for _, idx := range im.indexes {
		if idx.Table == tableName {
			result = append(result, idx)
		}
	}
	return result
}

// GetIndexForColumn returns an index that covers the given column.
// Returns the first single-column index found, or nil if none exists.
func (im *IndexManager) GetIndexForColumn(tableName, columnName string) *Index {
	for _, idx := range im.indexes {
		if idx.Table == tableName && len(idx.Columns) == 1 && idx.Columns[0] == columnName {
			return idx
		}
	}
	return nil
}

// ListIndexes returns all index names.
func (im *IndexManager) ListIndexes() []string {
	names := make([]string, 0, len(im.indexes))
	for name := range im.indexes {
		names = append(names, name)
	}
	return names
}

// AddIndex adds an existing index to the manager (for loading from storage).
func (im *IndexManager) AddIndex(idx *Index) {
	im.indexes[idx.Name] = idx
}
