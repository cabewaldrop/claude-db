// Package storage - B-tree implementation
//
// EDUCATIONAL NOTES:
// ------------------
// B-trees are the backbone of database indexing. They're self-balancing tree
// structures that maintain sorted data and allow searches, sequential access,
// insertions, and deletions in logarithmic time.
//
// Key properties of B-trees:
// 1. All leaves are at the same depth (perfectly balanced)
// 2. Nodes can have multiple keys and children (high branching factor)
// 3. Designed for storage systems with large block sizes
//
// Why B-trees for databases?
// - Minimize disk I/O by having high branching factor (fewer tree levels)
// - Each node fits in one disk page
// - Sequential access is efficient (leaves are linked)
//
// Our B-tree stores (key, rowID) pairs, where rowID points to actual data.
// This is a B+ tree variant where all data is stored in leaves.

package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// MaxKeys is the maximum number of keys in a node.
	// This is chosen so that a node fits comfortably in a page.
	MaxKeys = 100

	// MinKeys is the minimum number of keys in a non-root node.
	MinKeys = MaxKeys / 2
)

// BTreeNode represents a node in the B-tree.
//
// For leaf nodes:
//   - keys[i] is paired with values[i] (the actual data)
//   - children is empty
//   - nextLeaf/prevLeaf form a doubly-linked list for efficient range scans
//
// For internal nodes:
//   - keys[i] is a separator key
//   - children[i] points to keys < keys[i]
//   - children[len(keys)] points to keys >= keys[len(keys)-1]
type BTreeNode struct {
	pageID   uint32
	isLeaf   bool
	numKeys  uint16
	keys     [][]byte
	values   []uint64  // For leaves: actual values; for internal: unused
	children []uint32  // For internal nodes: child page IDs
	nextLeaf uint32    // For leaves: page ID of next leaf (0 if none)
	prevLeaf uint32    // For leaves: page ID of previous leaf (0 if none)
}

// BTree represents a B-tree index.
type BTree struct {
	pager    *Pager
	rootPage uint32
}

// NewBTree creates a new B-tree with an empty root.
func NewBTree(pager *Pager) (*BTree, error) {
	rootPage, err := pager.AllocatePage(PageTypeBTreeLeaf)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate root page: %w", err)
	}

	root := &BTreeNode{
		pageID:   rootPage.ID(),
		isLeaf:   true,
		numKeys:  0,
		keys:     make([][]byte, 0, MaxKeys),
		values:   make([]uint64, 0, MaxKeys),
		children: nil,
	}

	if err := serializeNode(rootPage, root); err != nil {
		return nil, err
	}

	return &BTree{
		pager:    pager,
		rootPage: rootPage.ID(),
	}, nil
}

// LoadBTree loads an existing B-tree from the given root page.
func LoadBTree(pager *Pager, rootPage uint32) *BTree {
	return &BTree{
		pager:    pager,
		rootPage: rootPage,
	}
}

// RootPage returns the root page ID.
func (bt *BTree) RootPage() uint32 {
	return bt.rootPage
}

// Search finds a key in the B-tree and returns its value.
//
// EDUCATIONAL NOTE:
// -----------------
// Search in a B-tree works by:
// 1. Start at the root
// 2. Binary search keys in current node
// 3. If found in leaf, return the value
// 4. If internal node, follow appropriate child pointer and repeat
//
// Time complexity: O(log n) where n is the number of keys
func (bt *BTree) Search(key []byte) (uint64, bool, error) {
	return bt.searchNode(bt.rootPage, key)
}

func (bt *BTree) searchNode(pageID uint32, key []byte) (uint64, bool, error) {
	page, err := bt.pager.GetPage(pageID)
	if err != nil {
		return 0, false, err
	}

	node, err := deserializeNode(page)
	if err != nil {
		return 0, false, err
	}

	// Find the appropriate position using binary search
	idx := bt.findKeyIndex(node, key)

	if node.isLeaf {
		// Check for exact match in leaf
		if idx < int(node.numKeys) && bytes.Equal(node.keys[idx], key) {
			return node.values[idx], true, nil
		}
		return 0, false, nil
	}

	// Internal node - follow child pointer
	// In a B+ tree with separator keys:
	// - children[i] contains keys < keys[i]
	// - children[i+1] contains keys >= keys[i]
	// So if key >= keys[idx], we need to go to children[idx+1]
	childIdx := idx
	if idx < int(node.numKeys) && bytes.Compare(key, node.keys[idx]) >= 0 {
		childIdx = idx + 1
	}
	if childIdx >= len(node.children) {
		childIdx = len(node.children) - 1
	}

	return bt.searchNode(node.children[childIdx], key)
}

// Insert adds a key-value pair to the B-tree.
//
// EDUCATIONAL NOTE:
// -----------------
// B-tree insertion uses a top-down approach with proactive splitting:
// 1. If we encounter a full node on the way down, split it first
// 2. This ensures we can always insert without backtracking
// 3. If root is full, create new root first (tree grows upward)
func (bt *BTree) Insert(key []byte, value uint64) error {
	rootPage, err := bt.pager.GetPage(bt.rootPage)
	if err != nil {
		return err
	}

	root, err := deserializeNode(rootPage)
	if err != nil {
		return err
	}

	// If root is full, split it first
	if root.numKeys >= MaxKeys {
		// Create new root
		newRootPage, err := bt.pager.AllocatePage(PageTypeBTreeInternal)
		if err != nil {
			return err
		}

		newRoot := &BTreeNode{
			pageID:   newRootPage.ID(),
			isLeaf:   false,
			numKeys:  0,
			keys:     make([][]byte, 0, MaxKeys),
			values:   nil,
			children: []uint32{bt.rootPage},
		}

		// Split old root as child of new root
		if err := bt.splitChild(newRoot, newRootPage, 0); err != nil {
			return err
		}

		bt.rootPage = newRootPage.ID()
		root = newRoot
		rootPage = newRootPage
	}

	return bt.insertNonFull(root, rootPage, key, value)
}

// insertNonFull inserts into a node that is guaranteed not to be full.
func (bt *BTree) insertNonFull(node *BTreeNode, page *Page, key []byte, value uint64) error {
	idx := bt.findKeyIndex(node, key)

	if node.isLeaf {
		// Check for update of existing key
		if idx < int(node.numKeys) && bytes.Equal(node.keys[idx], key) {
			node.values[idx] = value
			return serializeNode(page, node)
		}

		// Insert new key-value pair
		node.keys = append(node.keys, nil)
		node.values = append(node.values, 0)

		// Shift elements right to make room
		copy(node.keys[idx+1:], node.keys[idx:len(node.keys)-1])
		copy(node.values[idx+1:], node.values[idx:len(node.values)-1])

		node.keys[idx] = key
		node.values[idx] = value
		node.numKeys++

		return serializeNode(page, node)
	}

	// Internal node - find child to descend into
	childIdx := idx
	if childIdx >= len(node.children) {
		childIdx = len(node.children) - 1
	}

	childPageID := node.children[childIdx]
	childPage, err := bt.pager.GetPage(childPageID)
	if err != nil {
		return err
	}

	child, err := deserializeNode(childPage)
	if err != nil {
		return err
	}

	// If child is full, split it first
	if child.numKeys >= MaxKeys {
		if err := bt.splitChild(node, page, childIdx); err != nil {
			return err
		}

		// Reload node after split (it was modified)
		node, err = deserializeNode(page)
		if err != nil {
			return err
		}

		// Decide which child to follow after split
		if idx < int(node.numKeys) && bytes.Compare(key, node.keys[idx]) >= 0 {
			childIdx++
		}

		if childIdx >= len(node.children) {
			childIdx = len(node.children) - 1
		}

		childPageID = node.children[childIdx]
		childPage, err = bt.pager.GetPage(childPageID)
		if err != nil {
			return err
		}
		child, err = deserializeNode(childPage)
		if err != nil {
			return err
		}
	}

	return bt.insertNonFull(child, childPage, key, value)
}

// splitChild splits the child at childIdx into two nodes.
// The median key is promoted to the parent.
func (bt *BTree) splitChild(parent *BTreeNode, parentPage *Page, childIdx int) error {
	childPageID := parent.children[childIdx]
	childPage, err := bt.pager.GetPage(childPageID)
	if err != nil {
		return err
	}

	child, err := deserializeNode(childPage)
	if err != nil {
		return err
	}

	mid := int(child.numKeys) / 2

	// Create new sibling for the right half
	var siblingPageType PageType
	if child.isLeaf {
		siblingPageType = PageTypeBTreeLeaf
	} else {
		siblingPageType = PageTypeBTreeInternal
	}

	siblingPage, err := bt.pager.AllocatePage(siblingPageType)
	if err != nil {
		return err
	}

	sibling := &BTreeNode{
		pageID: siblingPage.ID(),
		isLeaf: child.isLeaf,
	}

	// Get median key (will be promoted to parent)
	medianKey := make([]byte, len(child.keys[mid]))
	copy(medianKey, child.keys[mid])

	if child.isLeaf {
		// For leaves: keep median in right child (B+ tree style)
		// Right sibling gets keys[mid:] and values[mid:]
		sibling.keys = make([][]byte, len(child.keys[mid:]))
		copy(sibling.keys, child.keys[mid:])
		sibling.numKeys = uint16(len(sibling.keys))

		sibling.values = make([]uint64, len(child.values[mid:]))
		copy(sibling.values, child.values[mid:])

		// Left child keeps keys[:mid] and values[:mid]
		child.keys = child.keys[:mid]
		child.values = child.values[:mid]
		child.numKeys = uint16(mid)

		// Maintain sibling pointers for the leaf linked list
		// New sibling inherits child's next pointer
		sibling.nextLeaf = child.nextLeaf
		sibling.prevLeaf = child.pageID

		// If there was a node after child, update its prevLeaf
		if child.nextLeaf != 0 {
			nextPage, err := bt.pager.GetPage(child.nextLeaf)
			if err != nil {
				return err
			}
			nextNode, err := deserializeNode(nextPage)
			if err != nil {
				return err
			}
			nextNode.prevLeaf = sibling.pageID
			if err := serializeNode(nextPage, nextNode); err != nil {
				return err
			}
		}

		// Child now points to new sibling
		child.nextLeaf = sibling.pageID
	} else {
		// For internal nodes: median goes up, right sibling gets keys after median
		sibling.keys = make([][]byte, len(child.keys[mid+1:]))
		copy(sibling.keys, child.keys[mid+1:])
		sibling.numKeys = uint16(len(sibling.keys))

		sibling.children = make([]uint32, len(child.children[mid+1:]))
		copy(sibling.children, child.children[mid+1:])

		// Left child keeps keys[:mid]
		child.keys = child.keys[:mid]
		child.numKeys = uint16(mid)
		child.children = child.children[:mid+1]
	}

	// Save child and sibling
	if err := serializeNode(childPage, child); err != nil {
		return err
	}
	if err := serializeNode(siblingPage, sibling); err != nil {
		return err
	}

	// Insert median key and new child pointer into parent
	parent.keys = append(parent.keys, nil)
	copy(parent.keys[childIdx+1:], parent.keys[childIdx:len(parent.keys)-1])
	parent.keys[childIdx] = medianKey
	parent.numKeys++

	parent.children = append(parent.children, 0)
	copy(parent.children[childIdx+2:], parent.children[childIdx+1:len(parent.children)-1])
	parent.children[childIdx+1] = siblingPage.ID()

	return serializeNode(parentPage, parent)
}

// findKeyIndex finds the index where key should be inserted (or exists).
func (bt *BTree) findKeyIndex(node *BTreeNode, key []byte) int {
	low, high := 0, int(node.numKeys)
	for low < high {
		mid := (low + high) / 2
		if bytes.Compare(node.keys[mid], key) < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}

// Scan returns all key-value pairs in sorted order.
func (bt *BTree) Scan() ([][]byte, []uint64, error) {
	var keys [][]byte
	var values []uint64
	err := bt.scanNode(bt.rootPage, &keys, &values)
	return keys, values, err
}

func (bt *BTree) scanNode(pageID uint32, keys *[][]byte, values *[]uint64) error {
	page, err := bt.pager.GetPage(pageID)
	if err != nil {
		return err
	}

	node, err := deserializeNode(page)
	if err != nil {
		return err
	}

	if node.isLeaf {
		*keys = append(*keys, node.keys...)
		*values = append(*values, node.values...)
		return nil
	}

	// Internal node - traverse children in order
	for i := 0; i < len(node.children); i++ {
		if err := bt.scanNode(node.children[i], keys, values); err != nil {
			return err
		}
	}

	return nil
}

// ScanRange returns key-value pairs within [startKey, endKey] using leaf sibling pointers.
// This is more efficient than a full tree traversal for range queries.
//
// EDUCATIONAL NOTE:
// -----------------
// Range scans are a common database operation (e.g., WHERE age BETWEEN 20 AND 30).
// By linking leaf nodes, we can:
// 1. Find the starting leaf with a single tree traversal (O(log n))
// 2. Follow sibling pointers to scan the range (O(k) where k is result size)
// This avoids repeatedly traversing from the root for each key.
func (bt *BTree) ScanRange(startKey, endKey []byte) ([][]byte, []uint64, error) {
	var keys [][]byte
	var values []uint64

	// Find the leaf containing startKey
	leafPageID, err := bt.findLeaf(bt.rootPage, startKey)
	if err != nil {
		return nil, nil, err
	}

	// Scan through leaves using sibling pointers
	// Note: Page ID 0 is valid, so we use hasMore flag instead of checking for 0
	hasMore := true
	for hasMore {
		page, err := bt.pager.GetPage(leafPageID)
		if err != nil {
			return nil, nil, err
		}

		node, err := deserializeNode(page)
		if err != nil {
			return nil, nil, err
		}

		// Find starting index in this leaf
		startIdx := 0
		if len(keys) == 0 {
			// First leaf - find where startKey would be
			startIdx = bt.findKeyIndex(node, startKey)
		}

		// Collect keys within range
		for i := startIdx; i < int(node.numKeys); i++ {
			if endKey != nil && bytes.Compare(node.keys[i], endKey) > 0 {
				// Past end of range
				return keys, values, nil
			}
			keys = append(keys, node.keys[i])
			values = append(values, node.values[i])
		}

		// Move to next leaf (0 means end of list, unless we're currently at page 0)
		if node.nextLeaf == 0 && leafPageID != 0 {
			hasMore = false
		} else if node.nextLeaf == 0 && leafPageID == 0 {
			// At page 0 with no next - single leaf tree or end of traversal
			hasMore = false
		} else {
			leafPageID = node.nextLeaf
		}
	}

	return keys, values, nil
}

// findLeaf finds the leaf node that would contain the given key.
func (bt *BTree) findLeaf(pageID uint32, key []byte) (uint32, error) {
	page, err := bt.pager.GetPage(pageID)
	if err != nil {
		return 0, err
	}

	node, err := deserializeNode(page)
	if err != nil {
		return 0, err
	}

	if node.isLeaf {
		return pageID, nil
	}

	// Find the appropriate child
	idx := bt.findKeyIndex(node, key)
	childIdx := idx
	if idx < int(node.numKeys) && bytes.Compare(key, node.keys[idx]) >= 0 {
		childIdx = idx + 1
	}
	if childIdx >= len(node.children) {
		childIdx = len(node.children) - 1
	}

	return bt.findLeaf(node.children[childIdx], key)
}

// FirstLeaf returns the page ID of the leftmost leaf node.
// Useful for full scans starting from the beginning.
func (bt *BTree) FirstLeaf() (uint32, error) {
	return bt.findLeftmostLeaf(bt.rootPage)
}

func (bt *BTree) findLeftmostLeaf(pageID uint32) (uint32, error) {
	page, err := bt.pager.GetPage(pageID)
	if err != nil {
		return 0, err
	}

	node, err := deserializeNode(page)
	if err != nil {
		return 0, err
	}

	if node.isLeaf {
		return pageID, nil
	}

	// Go to leftmost child
	return bt.findLeftmostLeaf(node.children[0])
}

// serializeNode writes a BTreeNode to a page.
func serializeNode(page *Page, node *BTreeNode) error {
	buf := bytes.NewBuffer(nil)

	// Write metadata: isLeaf (1 byte), numKeys (2 bytes)
	if node.isLeaf {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	binary.Write(buf, binary.LittleEndian, node.numKeys)

	// Write number of children (for internal nodes)
	numChildren := uint16(len(node.children))
	binary.Write(buf, binary.LittleEndian, numChildren)

	// Write sibling pointers (for leaf nodes)
	binary.Write(buf, binary.LittleEndian, node.nextLeaf)
	binary.Write(buf, binary.LittleEndian, node.prevLeaf)

	// Write keys
	for i := 0; i < int(node.numKeys); i++ {
		binary.Write(buf, binary.LittleEndian, uint16(len(node.keys[i])))
		buf.Write(node.keys[i])
	}

	// Write values (for leaves) or children (for internal)
	if node.isLeaf {
		for i := 0; i < int(node.numKeys); i++ {
			binary.Write(buf, binary.LittleEndian, node.values[i])
		}
	} else {
		for i := 0; i < len(node.children); i++ {
			binary.Write(buf, binary.LittleEndian, node.children[i])
		}
	}

	if buf.Len() > MaxDataSize {
		return errors.New("node data exceeds page size")
	}

	return page.SetData(buf.Bytes())
}

// deserializeNode reads a BTreeNode from a page.
func deserializeNode(page *Page) (*BTreeNode, error) {
	data := page.GetData()
	buf := bytes.NewReader(data)

	node := &BTreeNode{
		pageID: page.ID(),
	}

	// Read metadata
	isLeafByte, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	node.isLeaf = isLeafByte == 1

	if err := binary.Read(buf, binary.LittleEndian, &node.numKeys); err != nil {
		return nil, err
	}

	var numChildren uint16
	if err := binary.Read(buf, binary.LittleEndian, &numChildren); err != nil {
		return nil, err
	}

	// Read sibling pointers
	if err := binary.Read(buf, binary.LittleEndian, &node.nextLeaf); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &node.prevLeaf); err != nil {
		return nil, err
	}

	// Read keys
	node.keys = make([][]byte, node.numKeys)
	for i := uint16(0); i < node.numKeys; i++ {
		var keyLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}
		node.keys[i] = make([]byte, keyLen)
		if _, err := buf.Read(node.keys[i]); err != nil {
			return nil, err
		}
	}

	// Read values or children
	if node.isLeaf {
		node.values = make([]uint64, node.numKeys)
		for i := uint16(0); i < node.numKeys; i++ {
			if err := binary.Read(buf, binary.LittleEndian, &node.values[i]); err != nil {
				return nil, err
			}
		}
	} else {
		node.children = make([]uint32, numChildren)
		for i := uint16(0); i < numChildren; i++ {
			if err := binary.Read(buf, binary.LittleEndian, &node.children[i]); err != nil {
				return nil, err
			}
		}
	}

	return node, nil
}
