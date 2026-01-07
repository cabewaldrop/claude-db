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

func TestBTreeScanRange(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert many keys to trigger node splits and test sibling pointers
	numKeys := 200
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test range scan: keys 50-99
	startKey := []byte("key_0050")
	endKey := []byte("key_0099")
	keys, values, err := btree.ScanRange(startKey, endKey)
	if err != nil {
		t.Fatalf("ScanRange failed: %v", err)
	}

	expectedCount := 50 // keys 50 through 99 inclusive
	if len(keys) != expectedCount {
		t.Errorf("Expected %d keys, got %d", expectedCount, len(keys))
	}

	// Verify first and last keys
	if !bytes.Equal(keys[0], []byte("key_0050")) {
		t.Errorf("First key should be key_0050, got %s", keys[0])
	}
	if !bytes.Equal(keys[len(keys)-1], []byte("key_0099")) {
		t.Errorf("Last key should be key_0099, got %s", keys[len(keys)-1])
	}

	// Verify values match
	for i, v := range values {
		expected := uint64(50 + i)
		if v != expected {
			t.Errorf("Value at index %d: expected %d, got %d", i, expected, v)
		}
	}
}

func TestBTreeScanRangeOpenEnd(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test range scan with no end key (scan to end)
	startKey := []byte("key_0090")
	keys, values, err := btree.ScanRange(startKey, nil)
	if err != nil {
		t.Fatalf("ScanRange failed: %v", err)
	}

	expectedCount := 10 // keys 90-99
	if len(keys) != expectedCount {
		t.Errorf("Expected %d keys, got %d", expectedCount, len(keys))
	}

	if len(values) != expectedCount {
		t.Errorf("Expected %d values, got %d", expectedCount, len(values))
	}
}

func TestBTreeLeafSiblingPointers(t *testing.T) {
	btree, pager, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert enough keys to cause at least one split
	numKeys := 150
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Find the first leaf and traverse using sibling pointers
	firstLeafID, err := btree.FirstLeaf()
	if err != nil {
		t.Fatalf("FirstLeaf failed: %v", err)
	}

	// Traverse leaves and count total keys
	// Note: Page ID 0 is valid, so we use a do-while pattern
	totalKeys := 0
	leafCount := 0
	currentPageID := firstLeafID
	hasMore := true

	for hasMore {
		page, err := pager.GetPage(currentPageID)
		if err != nil {
			t.Fatalf("GetPage failed: %v", err)
		}

		node, err := deserializeNode(page)
		if err != nil {
			t.Fatalf("deserializeNode failed: %v", err)
		}

		if !node.isLeaf {
			t.Error("Expected leaf node during traversal")
		}

		totalKeys += int(node.numKeys)
		leafCount++

		if node.nextLeaf == 0 && currentPageID != 0 {
			// nextLeaf is 0 and we're not at page 0 - end of list
			hasMore = false
		} else if node.nextLeaf == 0 && currentPageID == 0 {
			// We're at page 0 and nextLeaf is 0 - could be single node or end
			// Check if this is actually the end by checking if we've seen multiple leaves
			hasMore = false
		} else {
			currentPageID = node.nextLeaf
		}
	}

	if totalKeys != numKeys {
		t.Errorf("Expected %d total keys, got %d", numKeys, totalKeys)
	}

	if leafCount < 2 {
		t.Errorf("Expected at least 2 leaf nodes after %d inserts, got %d", numKeys, leafCount)
	}
}

func TestBTreeIterator(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test full iteration
	iter := btree.NewIterator()
	defer iter.Close()

	count := 0
	for iter.Next() {
		expectedKey := fmt.Sprintf("key_%04d", count)
		if string(iter.Key()) != expectedKey {
			t.Errorf("Key %d: expected %s, got %s", count, expectedKey, iter.Key())
		}
		if iter.Value() != uint64(count) {
			t.Errorf("Value %d: expected %d, got %d", count, count, iter.Value())
		}
		count++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	if count != numKeys {
		t.Errorf("Expected %d keys, iterated %d", numKeys, count)
	}
}

func TestBTreeIteratorRange(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys 0-99
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test range iteration: keys 25-74 (inclusive)
	startKey := []byte("key_0025")
	endKey := []byte("key_0074")
	iter := btree.NewRangeIterator(startKey, endKey)
	defer iter.Close()

	count := 0
	for iter.Next() {
		expectedIdx := 25 + count
		expectedKey := fmt.Sprintf("key_%04d", expectedIdx)
		if string(iter.Key()) != expectedKey {
			t.Errorf("Key %d: expected %s, got %s", count, expectedKey, iter.Key())
		}
		if iter.Value() != uint64(expectedIdx) {
			t.Errorf("Value %d: expected %d, got %d", count, expectedIdx, iter.Value())
		}
		count++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	expectedCount := 50 // keys 25-74 inclusive
	if count != expectedCount {
		t.Errorf("Expected %d keys, iterated %d", expectedCount, count)
	}
}

func TestBTreeIteratorRangeExclusive(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test exclusive end bound
	startKey := []byte("key_0025")
	endKey := []byte("key_0030")
	opts := RangeScanOptions{
		StartInclusive: true,
		EndInclusive:   false,
	}
	iter := btree.NewRangeIteratorWithOptions(startKey, endKey, opts)
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	expectedCount := 5 // keys 25-29 (30 excluded)
	if count != expectedCount {
		t.Errorf("Expected %d keys (exclusive end), iterated %d", expectedCount, count)
	}
}

func TestBTreeIteratorCollect(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys
	numKeys := 50
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test Collect method
	iter := btree.NewIterator()
	keys, values, err := iter.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if len(keys) != numKeys {
		t.Errorf("Expected %d keys, got %d", numKeys, len(keys))
	}

	if len(values) != numKeys {
		t.Errorf("Expected %d values, got %d", numKeys, len(values))
	}

	// Verify keys are copies
	for i, key := range keys {
		expected := fmt.Sprintf("key_%04d", i)
		if string(key) != expected {
			t.Errorf("Key %d: expected %s, got %s", i, expected, key)
		}
	}
}

func TestBTreeIteratorEmpty(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Iterate over empty tree
	iter := btree.NewIterator()
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error on empty tree: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 keys in empty tree, got %d", count)
	}
}

func TestBTreeIteratorEarlyTermination(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert many keys
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Only read first 5 keys then close
	iter := btree.NewIterator()
	count := 0
	for iter.Next() {
		count++
		if count >= 5 {
			break
		}
	}
	iter.Close()

	// Verify we can stop early without error
	if count != 5 {
		t.Errorf("Expected to read 5 keys, got %d", count)
	}
}

func TestBTreeIteratorRangeOpenEnd(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test open-ended range (no end key)
	startKey := []byte("key_0090")
	iter := btree.NewRangeIterator(startKey, nil)
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	expectedCount := 10 // keys 90-99
	if count != expectedCount {
		t.Errorf("Expected %d keys, got %d", expectedCount, count)
	}
}

// --- Edge case tests from spec ---

func TestRangeScanWithLimit(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys 0-99
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test with limit
	opts := RangeScanOptions{
		StartInclusive: true,
		EndInclusive:   true,
		Limit:          5,
	}
	iter := btree.RangeScan(nil, nil, opts)
	defer iter.Close()

	var results []int
	for iter.Next() {
		// Extract number from key
		var n int
		fmt.Sscanf(string(iter.Key()), "key_%04d", &n)
		results = append(results, n)
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("Expected 5 results with limit, got %d", len(results))
	}

	// Should be first 5 keys: 0, 1, 2, 3, 4
	expected := []int{0, 1, 2, 3, 4}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("Result %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

func TestRangeScanEmptyRange(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys 0-99
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test with start > end (should return no results)
	startKey := []byte("key_0050")
	endKey := []byte("key_0040")
	iter := btree.RangeScan(startKey, endKey, DefaultRangeScanOptions())
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 results for empty range (start > end), got %d", count)
	}
}

func TestRangeScanNoMatches(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys 0-99
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test range outside data (keys 200-300 don't exist)
	startKey := []byte("key_0200")
	endKey := []byte("key_0300")
	iter := btree.RangeScan(startKey, endKey, DefaultRangeScanOptions())
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 results for range outside data, got %d", count)
	}
}

func TestRangeScanSingleResult(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys 0-99
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test single key range (50 to 50, inclusive)
	key := []byte("key_0050")
	iter := btree.RangeScan(key, key, DefaultRangeScanOptions())
	defer iter.Close()

	count := 0
	for iter.Next() {
		if string(iter.Key()) != "key_0050" {
			t.Errorf("Expected key_0050, got %s", iter.Key())
		}
		if iter.Value() != 50 {
			t.Errorf("Expected value 50, got %d", iter.Value())
		}
		count++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 result for single key range, got %d", count)
	}
}

func TestRangeScanExclusiveStart(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys 0-99
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test exclusive start: (10, 20] should give 11-20 (10 results)
	startKey := []byte("key_0010")
	endKey := []byte("key_0020")
	opts := RangeScanOptions{
		StartInclusive: false,
		EndInclusive:   true,
	}
	iter := btree.RangeScan(startKey, endKey, opts)
	defer iter.Close()

	count := 0
	firstKey := ""
	lastKey := ""
	for iter.Next() {
		if count == 0 {
			firstKey = string(iter.Key())
		}
		lastKey = string(iter.Key())
		count++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	if count != 10 {
		t.Errorf("Expected 10 results (exclusive start), got %d", count)
	}
	if firstKey != "key_0011" {
		t.Errorf("Expected first key key_0011 (exclusive start), got %s", firstKey)
	}
	if lastKey != "key_0020" {
		t.Errorf("Expected last key key_0020 (inclusive end), got %s", lastKey)
	}
}

func TestRangeScanBothExclusive(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys 0-99
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test both exclusive: (10, 20) should give 11-19 (9 results)
	startKey := []byte("key_0010")
	endKey := []byte("key_0020")
	opts := RangeScanOptions{
		StartInclusive: false,
		EndInclusive:   false,
	}
	iter := btree.RangeScan(startKey, endKey, opts)
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	if count != 9 {
		t.Errorf("Expected 9 results (both exclusive), got %d", count)
	}
}

func TestRangeScanIteratorAfterClose(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert some keys
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	iter := btree.RangeScan(nil, nil, DefaultRangeScanOptions())

	// Read a few
	iter.Next()
	iter.Next()

	// Close early
	iter.Close()

	// Next should return false after close
	if iter.Next() {
		t.Error("Next() should return false after Close()")
	}

	// Err should be nil (clean close, not error)
	if iter.Err() != nil {
		t.Errorf("Err() should be nil after Close(), got %v", iter.Err())
	}
}

func TestRangeScanOpenStart(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert keys 0-99
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Test nil start, end at 9 (inclusive): should get 0-9 (10 results)
	endKey := []byte("key_0009")
	iter := btree.RangeScan(nil, endKey, DefaultRangeScanOptions())
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	if count != 10 {
		t.Errorf("Expected 10 results for open start, got %d", count)
	}
}

func TestRangeScanUsesSiblingPointers(t *testing.T) {
	btree, _, cleanup := setupTestBTree(t)
	defer cleanup()

	// Insert enough keys to have multiple leaves
	numKeys := 300
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Range scan 50-250 inclusive
	startKey := []byte("key_0050")
	endKey := []byte("key_0250")
	iter := btree.RangeScan(startKey, endKey, DefaultRangeScanOptions())
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}

	expectedCount := 201 // 50 to 250 inclusive
	if count != expectedCount {
		t.Errorf("Expected %d results, got %d", expectedCount, count)
	}
}

func BenchmarkRangeScan(b *testing.B) {
	testFile := "bench_btree.db"
	pager, err := NewPager(testFile)
	if err != nil {
		b.Fatalf("Failed to create pager: %v", err)
	}
	defer func() {
		pager.Close()
		os.Remove(testFile)
	}()

	btree, err := NewBTree(pager)
	if err != nil {
		b.Fatalf("Failed to create B-tree: %v", err)
	}

	// Insert 100k keys
	for i := 0; i < 100000; i++ {
		key := []byte(fmt.Sprintf("key_%06d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			b.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	startKey := []byte("key_001000")
	endKey := []byte("key_002000")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := btree.RangeScan(startKey, endKey, DefaultRangeScanOptions())
		for iter.Next() {
		}
		iter.Close()
	}
}
