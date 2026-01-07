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
	"container/list"
	"errors"
	"fmt"
	"os"
	"sync"
)

// DefaultMaxCacheSize is the default maximum number of pages in the cache.
// Can be overridden using PagerOption functions.
const DefaultMaxCacheSize = 1000

// Pager manages reading and writing pages to the database file.
type Pager struct {
	file     *os.File
	filePath string

	// pageCount is the total number of pages in the file.
	pageCount uint32

	// cache is an in-memory cache of pages with LRU eviction.
	cache map[uint32]*Page

	// lruList maintains pages in LRU order (most recently used at front).
	// Each element's Value is the page ID (uint32).
	lruList *list.List

	// lruMap provides O(1) access from page ID to its list element.
	lruMap map[uint32]*list.Element

	// maxCacheSize is the maximum number of pages to keep in cache.
	maxCacheSize int

	// mu protects concurrent access to the pager.
	mu sync.RWMutex
}

// PagerOption is a functional option for configuring the Pager.
type PagerOption func(*Pager)

// WithMaxCacheSize sets the maximum cache size for the pager.
func WithMaxCacheSize(size int) PagerOption {
	return func(p *Pager) {
		if size > 0 {
			p.maxCacheSize = size
		}
	}
}

// NewPager creates a new pager for the given file path.
// If the file doesn't exist, it will be created.
// Optional PagerOption functions can be passed to configure the pager.
func NewPager(filePath string, opts ...PagerOption) (*Pager, error) {
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

	p := &Pager{
		file:         file,
		filePath:     filePath,
		pageCount:    pageCount,
		cache:        make(map[uint32]*Page),
		lruList:      list.New(),
		lruMap:       make(map[uint32]*list.Element),
		maxCacheSize: DefaultMaxCacheSize,
	}

	// Apply options
	for _, opt := range opts {
		opt(p)
	}

	return p, nil
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
// in fast memory. We use LRU (Least Recently Used) eviction to bound memory.
func (p *Pager) GetPage(pageID uint32) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check cache first (cache hit)
	if page, ok := p.cache[pageID]; ok {
		// Move to front of LRU list (most recently used)
		if elem, exists := p.lruMap[pageID]; exists {
			p.lruList.MoveToFront(elem)
		}
		return page, nil
	}

	// Cache miss - need to read from disk
	if pageID >= p.pageCount {
		return nil, fmt.Errorf("page %d does not exist (only %d pages)", pageID, p.pageCount)
	}

	// Evict if cache is full before adding new page
	if err := p.evictIfNeededLocked(); err != nil {
		return nil, fmt.Errorf("failed to evict page: %w", err)
	}

	// Read page from disk
	page, err := p.readPageFromDisk(pageID)
	if err != nil {
		return nil, err
	}

	// Add to cache and LRU list
	p.cache[pageID] = page
	elem := p.lruList.PushFront(pageID)
	p.lruMap[pageID] = elem

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

	// Evict if cache is full before adding new page
	if err := p.evictIfNeededLocked(); err != nil {
		return nil, fmt.Errorf("failed to evict page: %w", err)
	}

	// Create new page with the next available ID
	page := NewPage(p.pageCount, pageType)
	p.pageCount++

	// Add to cache and LRU list
	p.cache[page.ID()] = page
	elem := p.lruList.PushFront(page.ID())
	p.lruMap[page.ID()] = elem

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

// CacheSize returns the current number of pages in the cache.
func (p *Pager) CacheSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.cache)
}

// MaxCacheSize returns the maximum cache size.
func (p *Pager) MaxCacheSize() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.maxCacheSize
}

// evictIfNeededLocked evicts the least recently used page if cache is at capacity.
// Caller must hold the lock.
//
// EDUCATIONAL NOTE:
// -----------------
// LRU (Least Recently Used) eviction removes pages that haven't been accessed recently.
// We track access order with a doubly-linked list for O(1) operations:
// - Access a page: move to front (O(1))
// - Evict: remove from back (O(1))
// - Find in list: use map for O(1) lookup
// Before evicting a dirty page, we must write it back to disk to preserve changes.
func (p *Pager) evictIfNeededLocked() error {
	// Only evict if we're at capacity
	if len(p.cache) < p.maxCacheSize {
		return nil
	}

	// Get the least recently used page (back of list)
	back := p.lruList.Back()
	if back == nil {
		return nil
	}

	pageID := back.Value.(uint32)
	page, exists := p.cache[pageID]
	if !exists {
		// Inconsistent state - remove from LRU anyway
		p.lruList.Remove(back)
		delete(p.lruMap, pageID)
		return nil
	}

	// Write dirty page to disk before eviction
	if page.IsDirty() {
		if err := p.flushPageLocked(page); err != nil {
			return fmt.Errorf("failed to flush dirty page %d before eviction: %w", pageID, err)
		}
	}

	// Remove from cache and LRU tracking
	delete(p.cache, pageID)
	p.lruList.Remove(back)
	delete(p.lruMap, pageID)

	return nil
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
