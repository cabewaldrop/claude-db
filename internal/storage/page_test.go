package storage

import (
	"testing"
)

func TestNewPage(t *testing.T) {
	page := NewPage(1, PageTypeData)

	if page.ID() != 1 {
		t.Errorf("expected ID 1, got %d", page.ID())
	}

	if page.Type() != PageTypeData {
		t.Errorf("expected PageTypeData, got %d", page.Type())
	}

	if page.NumSlots() != 0 {
		t.Errorf("expected 0 slots, got %d", page.NumSlots())
	}

	if page.FreeSpace() != MaxDataSize {
		t.Errorf("expected %d free space, got %d", MaxDataSize, page.FreeSpace())
	}

	if !page.IsDirty() {
		t.Error("new page should be dirty")
	}
}

func TestPageWriteRead(t *testing.T) {
	page := NewPage(1, PageTypeData)

	testData := []byte("Hello, World!")
	offset, err := page.WriteData(testData)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	if offset != 0 {
		t.Errorf("expected offset 0, got %d", offset)
	}

	if page.NumSlots() != 1 {
		t.Errorf("expected 1 slot, got %d", page.NumSlots())
	}

	readData := page.ReadData(offset, uint16(len(testData)))
	if string(readData) != string(testData) {
		t.Errorf("expected %q, got %q", testData, readData)
	}
}

func TestPageSerializeDeserialize(t *testing.T) {
	original := NewPage(42, PageTypeBTreeLeaf)

	testData := []byte("Test data for serialization")
	_, err := original.WriteData(testData)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// Serialize
	serialized := original.Serialize()
	if len(serialized) != PageSize {
		t.Errorf("serialized size should be %d, got %d", PageSize, len(serialized))
	}

	// Deserialize
	restored, err := Deserialize(serialized)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Verify fields
	if restored.ID() != original.ID() {
		t.Errorf("ID mismatch: expected %d, got %d", original.ID(), restored.ID())
	}

	if restored.Type() != original.Type() {
		t.Errorf("Type mismatch: expected %d, got %d", original.Type(), restored.Type())
	}

	if restored.NumSlots() != original.NumSlots() {
		t.Errorf("NumSlots mismatch: expected %d, got %d", original.NumSlots(), restored.NumSlots())
	}

	// Verify data
	originalData := original.GetData()
	restoredData := restored.GetData()
	for i := range originalData {
		if originalData[i] != restoredData[i] {
			t.Errorf("data mismatch at byte %d", i)
			break
		}
	}
}

func TestPageWriteOverflow(t *testing.T) {
	page := NewPage(1, PageTypeData)

	// Try to write more than MaxDataSize
	hugeData := make([]byte, MaxDataSize+1)
	_, err := page.WriteData(hugeData)
	if err == nil {
		t.Error("expected error when writing too much data")
	}
}
