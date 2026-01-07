// Package storage - Pager component
//
// EDUCATIONAL NOTES:
// ------------------
// The Pager is responsible for managing the database file and reading/writing pages.
// It acts as a layer between the higher-level database operations and the file system.
//
// Key responsibilities:
// 1. Opening/closing the database file
// 2. Reading pages from disk into memory
// 3. Writing pages back to disk
// 4. Allocating new pages
// 5. Managing a simple page cache (buffer pool)
//
// In production databases, the pager would also handle:
// - Write-ahead logging (WAL) for crash recovery
// - Page checksums for corruption detection
// - Background flushing of dirty pages

package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// Pager manages reading and writing pages to the database file.
type Pager struct {
	file     *os.File
	filePath string

	// pageCount is the total number of pages in the file.
	pageCount uint32

	// cache is a simple in-memory cache of pages.
	// A real database would use a more sophisticated buffer pool
	// with LRU eviction, pin counting, etc.
	cache map[uint32]*Page

	// mu protects concurrent access to the pager.
	mu sync.RWMutex
}

// NewPager creates a new pager for the given file path.
// If the file doesn't exist, it will be created.
func NewPager(filePath string) (*Pager, error) {
	// Open file with read/write permissions, create if doesn't exist
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open database file: %w", err)
	}

	// Get file size to determine page count
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat database file: %w", err)
	}

	pageCount := uint32(stat.Size() / PageSize)

	return &Pager{
		file:      file,
		filePath:  filePath,
		pageCount: pageCount,
		cache:     make(map[uint32]*Page),
	}, nil
}

// Close flushes all dirty pages and closes the database file.
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Flush all dirty pages before closing
	for _, page := range p.cache {
		if page.IsDirty() {
			if err := p.flushPageLocked(page); err != nil {
				return fmt.Errorf("failed to flush page %d: %w", page.ID(), err)
			}
		}
	}

	return p.file.Close()
}

// GetPage retrieves a page from cache or disk.
//
// EDUCATIONAL NOTE:
// -----------------
// This is where the caching magic happens. We first check if the page
// is already in memory (cache hit). If not, we read it from disk (cache miss).
// This is similar to how CPU caches work - frequently accessed data stays
// in fast memory.
func (p *Pager) GetPage(pageID uint32) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check cache first (cache hit)
	if page, ok := p.cache[pageID]; ok {
		return page, nil
	}

	// Cache miss - need to read from disk
	if pageID >= p.pageCount {
		return nil, fmt.Errorf("page %d does not exist (only %d pages)", pageID, p.pageCount)
	}

	// Read page from disk
	page, err := p.readPageFromDisk(pageID)
	if err != nil {
		return nil, err
	}

	// Add to cache
	p.cache[pageID] = page

	return page, nil
}

// AllocatePage creates a new page and returns it.
//
// EDUCATIONAL NOTE:
// -----------------
// When we need a new page, we allocate space at the end of the file.
// A more sophisticated implementation would maintain a free list of
// previously deleted pages to reuse.
func (p *Pager) AllocatePage(pageType PageType) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Create new page with the next available ID
	page := NewPage(p.pageCount, pageType)
	p.pageCount++

	// Add to cache
	p.cache[page.ID()] = page

	return page, nil
}

// FlushPage writes a page to disk if it's dirty.
func (p *Pager) FlushPage(pageID uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	page, ok := p.cache[pageID]
	if !ok {
		return nil // Page not in cache, nothing to flush
	}

	return p.flushPageLocked(page)
}

// FlushAll writes all dirty pages to disk.
func (p *Pager) FlushAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, page := range p.cache {
		if page.IsDirty() {
			if err := p.flushPageLocked(page); err != nil {
				return err
			}
		}
	}

	return nil
}

// PageCount returns the total number of pages in the database.
func (p *Pager) PageCount() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.pageCount
}

// readPageFromDisk reads a page from the database file.
func (p *Pager) readPageFromDisk(pageID uint32) (*Page, error) {
	// Calculate file offset for this page
	offset := int64(pageID) * PageSize

	// Read page data
	buf := make([]byte, PageSize)
	n, err := p.file.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}
	if n != PageSize {
		return nil, fmt.Errorf("short read for page %d: got %d bytes, expected %d", pageID, n, PageSize)
	}

	return Deserialize(buf)
}

// flushPageLocked writes a page to disk. Caller must hold the lock.
func (p *Pager) flushPageLocked(page *Page) error {
	if !page.IsDirty() {
		return nil
	}

	// Calculate file offset
	offset := int64(page.ID()) * PageSize

	// Serialize and write
	data := page.Serialize()
	n, err := p.file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", page.ID(), err)
	}
	if n != PageSize {
		return fmt.Errorf("short write for page %d: wrote %d bytes, expected %d", page.ID(), n, PageSize)
	}

	// Sync to ensure data is on disk
	if err := p.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync after writing page %d: %w", page.ID(), err)
	}

	page.MarkClean()
	return nil
}

// DeleteFile removes the database file. Used for testing.
func DeleteFile(filePath string) error {
	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return os.Remove(filePath)
}
