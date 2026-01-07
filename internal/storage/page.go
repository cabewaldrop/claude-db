// Package storage implements a page-based storage engine for the database.
//
// EDUCATIONAL NOTES:
// ------------------
// Real databases store data in fixed-size blocks called "pages" (typically 4KB or 8KB).
// This approach has several advantages:
// 1. Efficient disk I/O - reading/writing fixed-size blocks is optimal for disk access
// 2. Memory management - pages can be cached and managed in a buffer pool
// 3. Crash recovery - pages can be atomically written to disk
//
// Our implementation uses a simplified page structure for clarity.

package storage

import (
	"encoding/binary"
	"errors"
)

const (
	// PageSize is the size of each page in bytes.
	// Real databases use 4KB-16KB. We use 4KB for simplicity.
	PageSize = 4096

	// PageHeaderSize is the size of the page header in bytes.
	// The header contains metadata about the page.
	PageHeaderSize = 16

	// MaxDataSize is the maximum amount of data a page can hold.
	MaxDataSize = PageSize - PageHeaderSize
)

// PageType indicates what kind of data a page holds.
type PageType uint8

const (
	// PageTypeFree indicates an unused page.
	PageTypeFree PageType = iota
	// PageTypeData indicates a page containing table row data.
	PageTypeData
	// PageTypeBTreeInternal indicates an internal B-tree node.
	PageTypeBTreeInternal
	// PageTypeBTreeLeaf indicates a B-tree leaf node.
	PageTypeBTreeLeaf
)

// Page represents a fixed-size block of storage.
//
// Page Layout (4096 bytes total):
// +------------------+
// | Header (16 bytes)|
// |   - PageID (4)   |
// |   - Type (1)     |
// |   - NumSlots (2) |
// |   - FreeSpace (2)|
// |   - Reserved (7) |
// +------------------+
// | Data Area        |
// | (4080 bytes)     |
// +------------------+
type Page struct {
	// id uniquely identifies this page within the database file.
	id uint32

	// pageType indicates what kind of data this page holds.
	pageType PageType

	// numSlots is the number of slots/records in this page.
	numSlots uint16

	// freeSpaceOffset points to the start of free space in the data area.
	freeSpaceOffset uint16

	// data holds the actual page content.
	data [MaxDataSize]byte

	// dirty indicates if the page has been modified since last flush.
	dirty bool
}

// NewPage creates a new empty page with the given ID and type.
func NewPage(id uint32, pageType PageType) *Page {
	return &Page{
		id:              id,
		pageType:        pageType,
		numSlots:        0,
		freeSpaceOffset: 0, // Start of data area
		dirty:           true,
	}
}

// ID returns the page's unique identifier.
func (p *Page) ID() uint32 {
	return p.id
}

// Type returns the page type.
func (p *Page) Type() PageType {
	return p.pageType
}

// NumSlots returns the number of slots/records in this page.
func (p *Page) NumSlots() uint16 {
	return p.numSlots
}

// FreeSpace returns the amount of free space available in the page.
func (p *Page) FreeSpace() uint16 {
	return MaxDataSize - p.freeSpaceOffset
}

// IsDirty returns true if the page has been modified.
func (p *Page) IsDirty() bool {
	return p.dirty
}

// MarkClean marks the page as not dirty (after flushing to disk).
func (p *Page) MarkClean() {
	p.dirty = false
}

// WriteData writes data to the page at the current free space offset.
// Returns the offset where data was written, or an error if not enough space.
func (p *Page) WriteData(data []byte) (uint16, error) {
	if len(data) > int(p.FreeSpace()) {
		return 0, errors.New("not enough space in page")
	}

	offset := p.freeSpaceOffset
	copy(p.data[offset:], data)
	p.freeSpaceOffset += uint16(len(data))
	p.numSlots++
	p.dirty = true

	return offset, nil
}

// ReadData reads data from the page at the given offset and length.
func (p *Page) ReadData(offset, length uint16) []byte {
	if offset+length > MaxDataSize {
		return nil
	}
	result := make([]byte, length)
	copy(result, p.data[offset:offset+length])
	return result
}

// GetData returns direct access to the page's data area.
// Use with caution - modifications will not automatically mark the page dirty.
func (p *Page) GetData() []byte {
	return p.data[:]
}

// SetData sets the page's data directly and marks it dirty.
func (p *Page) SetData(data []byte) error {
	if len(data) > MaxDataSize {
		return errors.New("data exceeds maximum page data size")
	}
	copy(p.data[:], data)
	p.dirty = true
	return nil
}

// Serialize converts the page to a byte slice for disk storage.
//
// EDUCATIONAL NOTE:
// -----------------
// Serialization is the process of converting in-memory structures to bytes.
// We use little-endian byte order (least significant byte first) because
// it's the native format on most modern CPUs (x86, ARM).
func (p *Page) Serialize() []byte {
	buf := make([]byte, PageSize)

	// Write header
	binary.LittleEndian.PutUint32(buf[0:4], p.id)
	buf[4] = byte(p.pageType)
	binary.LittleEndian.PutUint16(buf[5:7], p.numSlots)
	binary.LittleEndian.PutUint16(buf[7:9], p.freeSpaceOffset)
	// Bytes 9-15 are reserved for future use

	// Write data
	copy(buf[PageHeaderSize:], p.data[:])

	return buf
}

// Deserialize reads a page from a byte slice.
func Deserialize(buf []byte) (*Page, error) {
	if len(buf) != PageSize {
		return nil, errors.New("invalid page size")
	}

	p := &Page{
		id:              binary.LittleEndian.Uint32(buf[0:4]),
		pageType:        PageType(buf[4]),
		numSlots:        binary.LittleEndian.Uint16(buf[5:7]),
		freeSpaceOffset: binary.LittleEndian.Uint16(buf[7:9]),
		dirty:           false,
	}

	copy(p.data[:], buf[PageHeaderSize:])

	return p, nil
}
