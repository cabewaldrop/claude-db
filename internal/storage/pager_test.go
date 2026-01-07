package storage

import (
	"os"
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
