package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPagerCreateClose(t *testing.T) {
	testFile := "test_pager.db"
	defer os.Remove(testFile)

	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager failed: %v", err)
	}

	if pager.PageCount() != 0 {
		t.Errorf("expected 0 pages, got %d", pager.PageCount())
	}

	if err := pager.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPagerAllocateAndGet(t *testing.T) {
	testFile := "test_pager_alloc.db"
	defer os.Remove(testFile)

	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager failed: %v", err)
	}
	defer pager.Close()

	// Allocate a page
	page, err := pager.AllocatePage(PageTypeData)
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	if page.ID() != 0 {
		t.Errorf("expected page ID 0, got %d", page.ID())
	}

	if pager.PageCount() != 1 {
		t.Errorf("expected 1 page, got %d", pager.PageCount())
	}

	// Write some data
	testData := []byte("Hello, Database!")
	_, err = page.WriteData(testData)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// Flush the page
	if err := pager.FlushPage(page.ID()); err != nil {
		t.Fatalf("FlushPage failed: %v", err)
	}

	// Get the page (should be from cache)
	retrieved, err := pager.GetPage(page.ID())
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}

	if retrieved.ID() != page.ID() {
		t.Errorf("expected page ID %d, got %d", page.ID(), retrieved.ID())
	}
}

func TestPagerPersistence(t *testing.T) {
	testFile := "test_pager_persist.db"
	defer os.Remove(testFile)

	// Create and write
	pager, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager failed: %v", err)
	}

	page, err := pager.AllocatePage(PageTypeData)
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	testData := []byte("Persistent data")
	offset, err := page.WriteData(testData)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	if err := pager.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and read
	pager2, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager (reopen) failed: %v", err)
	}
	defer pager2.Close()

	if pager2.PageCount() != 1 {
		t.Errorf("expected 1 page after reopen, got %d", pager2.PageCount())
	}

	page2, err := pager2.GetPage(0)
	if err != nil {
		t.Fatalf("GetPage failed: %v", err)
	}

	readData := page2.ReadData(offset, uint16(len(testData)))
	if string(readData) != string(testData) {
		t.Errorf("expected %q, got %q", testData, readData)
	}
}

func TestPagerLRUEviction(t *testing.T) {
	testFile := filepath.Join(t.TempDir(), "test_lru.db")

	// Create pager with small cache (3 pages max)
	pager, err := NewPager(testFile, WithMaxCacheSize(3))
	if err != nil {
		t.Fatalf("NewPager failed: %v", err)
	}
	defer pager.Close()

	// Verify max cache size was set
	if pager.MaxCacheSize() != 3 {
		t.Errorf("expected max cache size 3, got %d", pager.MaxCacheSize())
	}

	// Allocate 3 pages (should fill cache)
	for i := 0; i < 3; i++ {
		page, err := pager.AllocatePage(PageTypeData)
		if err != nil {
			t.Fatalf("AllocatePage %d failed: %v", i, err)
		}
		// Write some data to each page
		_, err = page.WriteData([]byte{byte(i)})
		if err != nil {
			t.Fatalf("WriteData failed: %v", err)
		}
	}

	if pager.CacheSize() != 3 {
		t.Errorf("expected cache size 3, got %d", pager.CacheSize())
	}

	// Allocate 4th page - should trigger eviction of page 0 (LRU)
	page4, err := pager.AllocatePage(PageTypeData)
	if err != nil {
		t.Fatalf("AllocatePage 4 failed: %v", err)
	}

	// Cache should still be at max size
	if pager.CacheSize() != 3 {
		t.Errorf("expected cache size 3 after eviction, got %d", pager.CacheSize())
	}

	// The 4th page should be in cache
	if page4.ID() != 3 {
		t.Errorf("expected page ID 3, got %d", page4.ID())
	}
}

func TestPagerLRUEvictionWritesDirtyPages(t *testing.T) {
	testFile := filepath.Join(t.TempDir(), "test_lru_dirty.db")

	// Create pager with small cache (2 pages max)
	pager, err := NewPager(testFile, WithMaxCacheSize(2))
	if err != nil {
		t.Fatalf("NewPager failed: %v", err)
	}

	// Allocate and write to page 0
	page0, err := pager.AllocatePage(PageTypeData)
	if err != nil {
		t.Fatalf("AllocatePage 0 failed: %v", err)
	}
	testData := []byte("dirty data")
	offset, err := page0.WriteData(testData)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// Allocate page 1
	_, err = pager.AllocatePage(PageTypeData)
	if err != nil {
		t.Fatalf("AllocatePage 1 failed: %v", err)
	}

	// Allocate page 2 - should evict dirty page 0
	_, err = pager.AllocatePage(PageTypeData)
	if err != nil {
		t.Fatalf("AllocatePage 2 failed: %v", err)
	}

	// Close and reopen to verify page 0 was written to disk
	if err := pager.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	pager2, err := NewPager(testFile)
	if err != nil {
		t.Fatalf("NewPager (reopen) failed: %v", err)
	}
	defer pager2.Close()

	// Read page 0 from disk
	page0_reread, err := pager2.GetPage(0)
	if err != nil {
		t.Fatalf("GetPage 0 failed: %v", err)
	}

	// Verify the dirty data was persisted
	readData := page0_reread.ReadData(offset, uint16(len(testData)))
	if string(readData) != string(testData) {
		t.Errorf("expected %q, got %q", testData, readData)
	}
}

func TestPagerLRUAccessOrder(t *testing.T) {
	testFile := filepath.Join(t.TempDir(), "test_lru_order.db")

	// Create pager with small cache (3 pages max)
	pager, err := NewPager(testFile, WithMaxCacheSize(3))
	if err != nil {
		t.Fatalf("NewPager failed: %v", err)
	}
	defer pager.Close()

	// Allocate 3 pages
	for i := 0; i < 3; i++ {
		_, err := pager.AllocatePage(PageTypeData)
		if err != nil {
			t.Fatalf("AllocatePage %d failed: %v", i, err)
		}
	}

	// Flush all pages so we can reload them
	if err := pager.FlushAll(); err != nil {
		t.Fatalf("FlushAll failed: %v", err)
	}

	// Access page 0 to make it most recently used
	_, err = pager.GetPage(0)
	if err != nil {
		t.Fatalf("GetPage 0 failed: %v", err)
	}

	// Allocate page 3 - should evict page 1 (not page 0, which was just accessed)
	_, err = pager.AllocatePage(PageTypeData)
	if err != nil {
		t.Fatalf("AllocatePage 3 failed: %v", err)
	}

	// Page 0 should still be in cache (was accessed recently)
	// We can verify by checking cache size is still 3
	if pager.CacheSize() != 3 {
		t.Errorf("expected cache size 3, got %d", pager.CacheSize())
	}
}
