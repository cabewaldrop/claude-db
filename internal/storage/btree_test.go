package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func setupTestBTree(t *testing.T) (*BTree, *Pager, func()) {
	testFile := "test_btree.db"
	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}

	btree, err := NewBTree(pager)
	if err != nil {
		pager.Close()
		os.Remove(testFile)
		t.Fatalf("Failed to create B-tree: %v", err)
	}

	cleanup := func() {
		pager.Close()
		os.Remove(testFile)
	}

	return btree, pager, cleanup
}

func TestBTreeNewAndLoad(t *testing.T) {
	btree, pager, cleanup := setupTestBTree(t)
	defer cleanup()

	rootPage := btree.RootPage()
	if rootPage == 0 {
		// Page 0 is valid, just check it exists
		page, err := pager.GetPage(rootPage)
		if err != nil {
			t.Errorf("Root page should exist: %v", err)
		}
		if page.Type() != PageTypeBTreeLeaf {
			t.Errorf("New B-tree root should be a leaf")
		}
	}

	// Test loading existing tree
	loaded := LoadBTree(pager, rootPage)
	if loaded.RootPage() != rootPage {
		t.Errorf("Loaded tree should have same root page")
	}
}

func TestBTreeInsertAndSearch(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert some key-value pairs
	testData := []struct {
		key   []byte
		value uint64
	}{
		{[]byte("apple"), 1},
		{[]byte("banana"), 2},
		{[]byte("cherry"), 3},
		{[]byte("date"), 4},
		{[]byte("elderberry"), 5},
	}

	for _, td := range testData {
		if err := btree.Insert(td.key, td.value); err != nil {
			t.Fatalf("Insert %s failed: %v", td.key, err)
		}
	}

	// Search for each key
	for _, td := range testData {
		value, found, err := btree.Search(td.key)
		if err != nil {
			t.Fatalf("Search %s failed: %v", td.key, err)
		}
		if !found {
			t.Errorf("Key %s should be found", td.key)
		}
		if value != td.value {
			t.Errorf("Key %s: expected value %d, got %d", td.key, td.value, value)
		}
	}

	// Search for non-existent key
	_, found, err := btree.Search([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Search for nonexistent key failed: %v", err)
	}
	if found {
		t.Error("Nonexistent key should not be found")
	}
}

func TestBTreeInsertDuplicate(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	key := []byte("key")

	// Insert initial value
	if err := btree.Insert(key, 100); err != nil {
		t.Fatalf("Initial insert failed: %v", err)
	}

	// Insert duplicate (should update value)
	if err := btree.Insert(key, 200); err != nil {
		t.Fatalf("Duplicate insert failed: %v", err)
	}

	// Verify updated value
	value, found, err := btree.Search(key)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if !found {
		t.Error("Key should be found")
	}
	if value != 200 {
		t.Errorf("Expected updated value 200, got %d", value)
	}
}

func TestBTreeManyInserts(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert many keys to trigger node splits
	numKeys := 200

	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Verify all keys can be found
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		value, found, err := btree.Search(key)
		if err != nil {
			t.Fatalf("Search %d failed: %v", i, err)
		}
		if !found {
			t.Errorf("Key %d should be found", i)
		}
		if value != uint64(i) {
			t.Errorf("Key %d: expected value %d, got %d", i, i, value)
		}
	}
}

func TestBTreeScan(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys in non-sorted order
	keys := []string{"cherry", "apple", "banana", "date"}
	for i, k := range keys {
		if err := btree.Insert([]byte(k), uint64(i+1)); err != nil {
			t.Fatalf("Insert %s failed: %v", k, err)
		}
	}

	// Scan should return all keys
	scannedKeys, scannedValues, err := btree.Scan()
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(scannedKeys) != len(keys) {
		t.Errorf("Expected %d keys, got %d", len(keys), len(scannedKeys))
	}

	if len(scannedValues) != len(keys) {
		t.Errorf("Expected %d values, got %d", len(keys), len(scannedValues))
	}
}

func TestBTreeNodeSerialization(t *testing.T) {
	// Create a test node
	node := &BTreeNode{
		pageID:  42,
		isLeaf:  true,
		numKeys: 3,
		keys: [][]byte{
			[]byte("alpha"),
			[]byte("beta"),
			[]byte("gamma"),
		},
		values:   []uint64{1, 2, 3},
		children: nil,
	}

	// Create a page and serialize
	page := NewPage(42, PageTypeBTreeLeaf)
	if err := serializeNode(page, node); err != nil {
		t.Fatalf("serializeNode failed: %v", err)
	}

	// Deserialize and verify
	restored, err := deserializeNode(page)
	if err != nil {
		t.Fatalf("deserializeNode failed: %v", err)
	}

	if restored.isLeaf != node.isLeaf {
		t.Errorf("isLeaf mismatch: expected %v, got %v", node.isLeaf, restored.isLeaf)
	}

	if restored.numKeys != node.numKeys {
		t.Errorf("numKeys mismatch: expected %d, got %d", node.numKeys, restored.numKeys)
	}

	for i := 0; i < int(node.numKeys); i++ {
		if !bytes.Equal(restored.keys[i], node.keys[i]) {
			t.Errorf("key %d mismatch", i)
		}
		if restored.values[i] != node.values[i] {
			t.Errorf("value %d mismatch", i)
		}
	}
}

func TestBTreeInternalNode(t *testing.T) {
	// Test internal node serialization
	node := &BTreeNode{
		pageID:  10,
		isLeaf:  false,
		numKeys: 2,
		keys: [][]byte{
			[]byte("middle"),
			[]byte("right"),
		},
		values:   nil,
		children: []uint32{1, 2, 3}, // Child page IDs (numKeys + 1)
	}

	page := NewPage(10, PageTypeBTreeInternal)
	if err := serializeNode(page, node); err != nil {
		t.Fatalf("serializeNode failed: %v", err)
	}

	restored, err := deserializeNode(page)
	if err != nil {
		t.Fatalf("deserializeNode failed: %v", err)
	}

	if restored.isLeaf {
		t.Error("restored node should not be a leaf")
	}

	if len(restored.children) != 3 {
		t.Errorf("children count mismatch: expected 3, got %d", len(restored.children))
	}
}

func TestBTreeIntegerKeys(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Use integer keys (as bytes)
	for i := 100; i >= 1; i-- {
		key := []byte(fmt.Sprintf("%010d", i)) // Padded for proper ordering
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Verify all keys
	for i := 1; i <= 100; i++ {
		key := []byte(fmt.Sprintf("%010d", i))
		value, found, err := btree.Search(key)
		if err != nil {
			t.Fatalf("Search %d failed: %v", i, err)
		}
		if !found {
			t.Errorf("Key %d should be found", i)
		}
		if value != uint64(i) {
			t.Errorf("Key %d: expected value %d, got %d", i, i, value)
		}
	}
}

func TestBTreeEmptySearch(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Search in empty tree
	_, found, err := btree.Search([]byte("anything"))
	if err != nil {
		t.Fatalf("Search in empty tree failed: %v", err)
	}
	if found {
		t.Error("Should not find anything in empty tree")
	}
}

func TestBTreeLongKeys(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Test with longer keys
	longKey := bytes.Repeat([]byte("x"), 100)
	if err := btree.Insert(longKey, 42); err != nil {
		t.Fatalf("Insert long key failed: %v", err)
	}

	value, found, err := btree.Search(longKey)
	if err != nil {
		t.Fatalf("Search long key failed: %v", err)
	}
	if !found {
		t.Error("Long key should be found")
	}
	if value != 42 {
		t.Errorf("Expected value 42, got %d", value)
	}
}
