package storage

import (
	"fmt"
	"os"
	"testing"
)

// TestBTreeVisualization is a helper test to visualize B-tree structure.
// Run with: go test -v -run TestBTreeVisualization ./internal/storage
func TestBTreeVisualization(t *testing.T) {
	testFile := "test_btree_viz.db"
	defer os.Remove(testFile)

	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	btree, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create B-tree: %v", err)
	}

	// Insert enough keys to create a multi-level tree
	numKeys := 150

	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		if err := btree.Insert(key, uint64(i)); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	t.Log("B-tree structure after inserting 150 keys:")
	t.Log("==========================================")
	visualizeNode(t, pager, btree.rootPage, 0)

	// Verify all keys can be found
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		value, found, err := btree.Search(key)
		if err != nil {
			t.Errorf("Search error for key %d: %v", i, err)
		}
		if !found {
			t.Errorf("Key %d not found", i)
		}
		if value != uint64(i) {
			t.Errorf("Key %d: expected value %d, got %d", i, i, value)
		}
	}
}

func visualizeNode(t *testing.T, pager *Pager, pageID uint32, depth int) {
	page, err := pager.GetPage(pageID)
	if err != nil {
		t.Logf("%sError getting page %d: %v", indentStr(depth), pageID, err)
		return
	}

	node, err := deserializeNode(page)
	if err != nil {
		t.Logf("%sError deserializing page %d: %v", indentStr(depth), pageID, err)
		return
	}

	if node.isLeaf {
		t.Logf("%sLeaf[page=%d, keys=%d]: %s ... %s",
			indentStr(depth), pageID, node.numKeys,
			node.keys[0], node.keys[node.numKeys-1])
	} else {
		t.Logf("%sInternal[page=%d, keys=%d, children=%d]:",
			indentStr(depth), pageID, node.numKeys, len(node.children))

		// Show separator keys
		for i := 0; i < int(node.numKeys); i++ {
			t.Logf("%s  separator[%d]: %s", indentStr(depth), i, node.keys[i])
		}

		// Recurse into children
		for i, childID := range node.children {
			t.Logf("%s  child[%d]:", indentStr(depth), i)
			visualizeNode(t, pager, childID, depth+2)
		}
	}
}

func indentStr(depth int) string {
	s := ""
	for i := 0; i < depth; i++ {
		s += "  "
	}
	return s
}
