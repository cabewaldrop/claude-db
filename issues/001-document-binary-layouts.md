# Issue #001: Document Binary Layouts

## Summary

Create comprehensive documentation for all binary data layouts used in claude-db. This documentation should include ASCII diagrams showing byte-level layouts for all storage structures.

## Motivation

Understanding the binary format is essential for:
- Debugging storage issues
- Implementing compatible tools
- Future format migrations
- Contributor onboarding

## Scope

The following binary layouts need to be documented:

### 1. Page Layout (4096 bytes)

```
+------------------+------------------+------------------+------------------+
|     Byte 0-3     |     Byte 4       |    Byte 5-6      |    Byte 7-8      |
+------------------+------------------+------------------+------------------+
|     PageID       |    PageType      |    NumSlots      | FreeSpaceOffset  |
|    (uint32)      |     (uint8)      |    (uint16)      |    (uint16)      |
+------------------+------------------+------------------+------------------+

+------------------+--------------------------------------------------+
|    Byte 9-15     |                   Byte 16-4095                   |
+------------------+--------------------------------------------------+
|    Reserved      |                    Data Area                     |
|    (7 bytes)     |                  (4080 bytes)                    |
+------------------+--------------------------------------------------+
```

**PageType values:**
| Value | Name              | Description                    |
|-------|-------------------|--------------------------------|
| 0     | PageTypeFree      | Unused/available page          |
| 1     | PageTypeData      | Contains table row data        |
| 2     | PageTypeBTreeInternal | Internal B-tree node       |
| 3     | PageTypeBTreeLeaf | Leaf B-tree node               |

**Source:** `internal/storage/page.go`

---

### 2. B-Tree Node Layout

#### Leaf Node
```
+----------+----------+------------+
|  Byte 0  | Byte 1-2 |  Byte 3-4  |
+----------+----------+------------+
|  IsLeaf  | NumKeys  | NumChildren|
|  (0x01)  | (uint16) |  (uint16)  |
+----------+----------+------------+

+-------------------+-------------------+-----+-------------------+
|      Key 0        |      Key 1        | ... |      Key N        |
+-------------------+-------------------+-----+-------------------+
| Len    | Data     | Len    | Data     |     | Len    | Data     |
| uint16 | variable | uint16 | variable |     | uint16 | variable |
+-------------------+-------------------+-----+-------------------+

+-------------------+-------------------+-----+-------------------+
|     Value 0       |     Value 1       | ... |     Value N       |
+-------------------+-------------------+-----+-------------------+
|  Location uint64  |  Location uint64  |     |  Location uint64  |
+-------------------+-------------------+-----+-------------------+
```

#### Internal Node
```
+----------+----------+------------+
|  Byte 0  | Byte 1-2 |  Byte 3-4  |
+----------+----------+------------+
|  IsLeaf  | NumKeys  | NumChildren|
|  (0x00)  | (uint16) |  (uint16)  |
+----------+----------+------------+

+-------------------+-------------------+-----+-------------------+
|      Key 0        |      Key 1        | ... |      Key N        |
+-------------------+-------------------+-----+-------------------+
| Len    | Data     | Len    | Data     |     | Len    | Data     |
| uint16 | variable | uint16 | variable |     | uint16 | variable |
+-------------------+-------------------+-----+-------------------+

+------------+------------+-----+--------------+
|  Child 0   |  Child 1   | ... |  Child N+1   |
+------------+------------+-----+--------------+
|   uint32   |   uint32   |     |    uint32    |
|  (PageID)  |  (PageID)  |     |   (PageID)   |
+------------+------------+-----+--------------+
```

**Source:** `internal/storage/btree.go`

---

### 3. Table Row Layout

```
+------------------+--------------------+--------------------+
|    Byte 0-1      |      Byte 2-9      |     Byte 10-11     |
+------------------+--------------------+--------------------+
|   RowDataLen     |       RowID        |     NumValues      |
|    (uint16)      |      (uint64)      |      (uint16)      |
+------------------+--------------------+--------------------+

+---------------------------------------------------------------+
|                         Values Array                          |
+---------------------------------------------------------------+
| Value 0 | Value 1 | Value 2 | ... | Value N                   |
+---------------------------------------------------------------+
```

#### Value Encoding

```
+----------+----------+------------------------------------------+
|  Byte 0  |  Byte 1  |              Byte 2+                     |
+----------+----------+------------------------------------------+
| DataType |  IsNull  |           Type-specific data             |
|  (uint8) |  (uint8) |             (variable)                   |
+----------+----------+------------------------------------------+
```

**DataType values:**
| Value | Type    | Data Layout                              |
|-------|---------|------------------------------------------|
| 0     | Unknown | (none)                                   |
| 1     | Integer | int64 (8 bytes, little-endian)           |
| 2     | Real    | float64 (8 bytes, little-endian)         |
| 3     | Text    | uint16 length + variable bytes           |
| 4     | Boolean | uint8 (0=false, 1=true)                  |

**Source:** `internal/table/table.go`

---

### 4. Database Catalog Layout (Page 0)

```
+------------------+------------------+
|    Byte 0-1      |    Byte 2-3      |
+------------------+------------------+
|   CatalogMagic   |   NumTables      |
|    (0xCDB0)      |    (uint16)      |
+------------------+------------------+

+---------------------------------------------------------------+
|                        Tables Array                           |
+---------------------------------------------------------------+
| TableInfo 0 | TableInfo 1 | ... | TableInfo N                 |
+---------------------------------------------------------------+
```

#### TableInfo Structure
```
+-------------------+------------+----------------+--------------+
|    Table Name     |  RootPage  |   NextRowID    |  PrimaryKey  |
+-------------------+------------+----------------+--------------+
| len(u16) + bytes  |  (uint32)  |    (uint64)    |   (int32)    |
+-------------------+------------+----------------+--------------+

+------------------+-----------------------------------------------+
|   NumColumns     |              Columns Array                    |
+------------------+-----------------------------------------------+
|    (uint16)      | ColumnInfo 0 | ColumnInfo 1 | ... | Col N    |
+------------------+-----------------------------------------------+
```

#### ColumnInfo Structure
```
+-------------------+------------+----------+
|   Column Name     | ColumnType |  Flags   |
+-------------------+------------+----------+
| len(u16) + bytes  |  (uint8)   | (uint8)  |
+-------------------+------------+----------+
```

**Flags byte:**
| Bit | Mask | Meaning     |
|-----|------|-------------|
| 0   | 0x01 | PrimaryKey  |
| 1   | 0x02 | NotNull     |

**Source:** `internal/catalog/catalog.go`

---

### 5. Row Location Encoding

Used in B-tree leaf values to point to row data:

```
+--------------------------------+--------------------------------+
|          Bits 32-63            |           Bits 0-31            |
+--------------------------------+--------------------------------+
|            PageID              |           PageOffset           |
|           (uint32)             |            (uint32)            |
+--------------------------------+--------------------------------+

Encoding:  location = (pageID << 32) | offset
Decoding:  pageID   = location >> 32
           offset   = location & 0xFFFFFFFF
```

**Source:** `internal/table/table.go:290-291`

---

## Tasks

- [ ] Create `docs/binary-format.md` with all layouts
- [ ] Add byte offset annotations to each diagram
- [ ] Include endianness notes (all little-endian)
- [ ] Add real-world examples with hex dumps
- [ ] Document size constraints and limits
- [ ] Add version/compatibility notes for future migrations

## Additional Notes

### Constants Reference

| Constant        | Value  | Location                    |
|-----------------|--------|-----------------------------|
| PageSize        | 4096   | internal/storage/page.go    |
| PageHeaderSize  | 16     | internal/storage/page.go    |
| MaxDataSize     | 4080   | internal/storage/page.go    |
| MaxKeys         | 100    | internal/storage/btree.go   |
| MinKeys         | 50     | internal/storage/btree.go   |
| CatalogPageID   | 0      | internal/catalog/catalog.go |
| CatalogMagic    | 0xCDB0 | internal/catalog/catalog.go |

### File Format Overview

```
+------------------------------------------------------------------+
|                         Database File                            |
+------------------------------------------------------------------+
| Page 0      | Page 1      | Page 2      | ... | Page N           |
| (Catalog)   | (Data/BTree)| (Data/BTree)| ... | (Data/BTree)     |
+------------------------------------------------------------------+
     |              |             |                    |
     v              v             v                    v
  4096 bytes    4096 bytes   4096 bytes          4096 bytes
```

## Priority

Medium

## Labels

`documentation`, `storage`, `binary-format`
