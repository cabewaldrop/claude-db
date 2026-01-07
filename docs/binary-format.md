# Binary Format Specification

This document describes the binary layout of all storage structures in claude-db.

All multi-byte integers use **little-endian** byte order.

## Table of Contents

1. [Page Layout](#1-page-layout)
2. [B-Tree Node Layout](#2-b-tree-node-layout)
3. [Table Row Layout](#3-table-row-layout)
4. [Database Catalog](#4-database-catalog)
5. [Row Location Encoding](#5-row-location-encoding)
6. [Constants Reference](#6-constants-reference)

---

## 1. Page Layout

All data is stored in fixed-size 4096-byte pages. Each page has a 16-byte header followed by a 4080-byte data area.

**Source:** `internal/storage/page.go`

### Structure

```
 Offset   Size   Field
+--------+------+------------------+
|   0    |  4   | PageID (uint32)  |
+--------+------+------------------+
|   4    |  1   | PageType (uint8) |
+--------+------+------------------+
|   5    |  2   | NumSlots (uint16)|
+--------+------+------------------+
|   7    |  2   | FreeSpaceOffset  |
|        |      | (uint16)         |
+--------+------+------------------+
|   9    |  7   | Reserved         |
+--------+------+------------------+
|  16    | 4080 | Data Area        |
+--------+------+------------------+
Total: 4096 bytes
```

### Visual Layout

```
0                   1                   2                   3
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           PageID                              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   PageType    |           NumSlots            |  FreeSpace... |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| ...Offset     |                 Reserved (7 bytes)            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
|                        Data Area                              |
|                       (4080 bytes)                            |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### PageType Values

| Value | Constant             | Description                |
|-------|----------------------|----------------------------|
| 0     | `PageTypeFree`       | Unused/available page      |
| 1     | `PageTypeData`       | Contains table row data    |
| 2     | `PageTypeBTreeInternal` | Internal B-tree node    |
| 3     | `PageTypeBTreeLeaf`  | Leaf B-tree node           |

### Example Hex Dump

```
Page 1, Type=Data, 2 slots, FreeSpaceOffset=64:

00000000: 01 00 00 00 01 02 00 40  00 00 00 00 00 00 00 00  |.......@........|
          ├─PageID─┤ ││├Slots┤├Offs┤├────Reserved────────┤
                    │└PageType(Data)
```

---

## 2. B-Tree Node Layout

B-tree nodes are stored in the data area of pages with `PageType` 2 (internal) or 3 (leaf).

**Source:** `internal/storage/btree.go`

### Header Structure

```
 Offset   Size   Field
+--------+------+--------------------+
|   0    |  1   | IsLeaf (uint8)     |
|        |      | 0=internal, 1=leaf |
+--------+------+--------------------+
|   1    |  2   | NumKeys (uint16)   |
+--------+------+--------------------+
|   3    |  2   | NumChildren        |
|        |      | (uint16)           |
+--------+------+--------------------+
```

### Leaf Node Layout

```
+-------------------------------------------------------------------+
| Header (5 bytes)                                                  |
+-------+----------+------------------------------------------------+
| Byte  | Field    | Value                                          |
+-------+----------+------------------------------------------------+
|   0   | IsLeaf   | 0x01                                           |
|  1-2  | NumKeys  | N (little-endian uint16)                       |
|  3-4  | NumChild | 0 (unused for leaves)                          |
+-------+----------+------------------------------------------------+

+-------------------------------------------------------------------+
| Keys Section (variable length)                                    |
+-------------------------------------------------------------------+
| For each key i (0 to N-1):                                        |
|   +--------+------+---------------------------------------------+ |
|   | Offset | Size | Field                                       | |
|   +--------+------+---------------------------------------------+ |
|   | varies |  2   | KeyLength[i] (uint16)                       | |
|   | varies | var  | KeyData[i] (KeyLength bytes)                | |
|   +--------+------+---------------------------------------------+ |
+-------------------------------------------------------------------+

+-------------------------------------------------------------------+
| Values Section (8 bytes per key)                                  |
+-------------------------------------------------------------------+
| For each value i (0 to N-1):                                      |
|   +--------+------+---------------------------------------------+ |
|   | varies |  8   | Location[i] (uint64) - see Row Location     | |
|   +--------+------+---------------------------------------------+ |
+-------------------------------------------------------------------+
```

### Internal Node Layout

```
+-------------------------------------------------------------------+
| Header (5 bytes)                                                  |
+-------+----------+------------------------------------------------+
| Byte  | Field    | Value                                          |
+-------+----------+------------------------------------------------+
|   0   | IsLeaf   | 0x00                                           |
|  1-2  | NumKeys  | N (little-endian uint16)                       |
|  3-4  | NumChild | N+1 (little-endian uint16)                     |
+-------+----------+------------------------------------------------+

+-------------------------------------------------------------------+
| Keys Section (variable length) - same as leaf                     |
+-------------------------------------------------------------------+

+-------------------------------------------------------------------+
| Children Section (4 bytes per child)                              |
+-------------------------------------------------------------------+
| For each child i (0 to N):                                        |
|   +--------+------+---------------------------------------------+ |
|   | varies |  4   | ChildPageID[i] (uint32)                     | |
|   +--------+------+---------------------------------------------+ |
+-------------------------------------------------------------------+
```

### Visual Example - Leaf Node

```
+---+-------+-------+-------+-------+-------+-----------+-----------+
| 1 | NumK  |NumCh=0| KLen0 | Key0  | KLen1 |   Key1    |  Value0   |
+---+-------+-------+-------+-------+-------+-----------+-----------+
 0   1     3       5       7      11      13          17          25

Example with 2 keys ("id"=1, "name"=2):
01 02 00 00 00 02 00 69 64 04 00 6E 61 6D 65 01 00 00 00 00 00 00 00 02 00 00 00 00 00 00 00
└┘ └───┘ └───┘ └───┘ └──┘ └───┘ └────────┘ └──────────────────────┘ └──────────────────────┘
IsLeaf=1  NumCh  Len=2 "id" Len=4  "name"        Value0=1                   Value1=2
    NumKeys=2
```

---

## 3. Table Row Layout

Rows are stored in data pages with a length prefix for variable-length support.

**Source:** `internal/table/table.go`

### On-Page Format

```
 Offset   Size   Field
+--------+------+------------------------+
|   0    |  2   | RowDataLength (uint16) |
+--------+------+------------------------+
|   2    | var  | RowData (see below)    |
+--------+------+------------------------+
```

### Row Data Structure

```
 Offset   Size   Field
+--------+------+-------------------+
|   0    |  8   | RowID (uint64)    |
+--------+------+-------------------+
|   8    |  2   | NumValues (uint16)|
+--------+------+-------------------+
|  10    | var  | Values[]          |
+--------+------+-------------------+
```

### Value Encoding

Each value is encoded with a type tag and null flag:

```
 Offset   Size   Field
+--------+------+------------------------+
|   0    |  1   | DataType (uint8)       |
+--------+------+------------------------+
|   1    |  1   | IsNull (uint8)         |
|        |      | 0=not null, 1=null     |
+--------+------+------------------------+
|   2    | var  | TypeData (if not null) |
+--------+------+------------------------+
```

### DataType Values and Encoding

| Value | Type      | TypeData Encoding                          |
|-------|-----------|-------------------------------------------|
| 0     | Unknown   | (none)                                    |
| 1     | Integer   | int64 (8 bytes, little-endian)            |
| 2     | Real      | float64 (8 bytes, IEEE 754 little-endian) |
| 3     | Text      | uint16 length + UTF-8 bytes               |
| 4     | Boolean   | uint8 (0=false, 1=true)                   |

### Visual Layout

```
+-------------------------------------------------------------------+
|                         Row on Page                               |
+-------------------------------------------------------------------+
| RowDataLen | RowID (8 bytes) | NumValues |  Value0  |  Value1  |..|
+-------------------------------------------------------------------+
    2 bytes                        2 bytes    variable   variable

Value Encoding Examples:
+-----------+-----------+-------------------------------------------+
| Type=1    | IsNull=0  | 42 00 00 00 00 00 00 00                   |
| (Integer) |           | (int64 = 42)                              |
+-----------+-----------+-------------------------------------------+
| Type=3    | IsNull=0  | 05 00 48 65 6C 6C 6F                      |
| (Text)    |           | (len=5, "Hello")                          |
+-----------+-----------+-------------------------------------------+
| Type=4    | IsNull=1  | (no data - null value)                    |
| (Boolean) |           |                                           |
+-----------+-----------+-------------------------------------------+
```

### Complete Row Example

Row with ID=1, values: INTEGER 42, TEXT "Hi", BOOLEAN true

```
Hex: 12 00 01 00 00 00 00 00 00 00 03 00 01 00 2A 00 00 00 00 00 00 00 03 00 02 00 48 69 04 00 01
     └───┘ └──────────────────────┘ └───┘ └──┘ └──────────────────────┘ └──┘ └───┘ └──┘ └──┘ └──┘
     Len=18   RowID=1              NumVal  Type IsNull  Integer=42       Type Len=2 "Hi" Type IsNull
                                    =3     =INT =0                       =TXT            =BOOL =0
                                                                                               Bool=1
```

---

## 4. Database Catalog

The catalog is stored in Page 0 and contains metadata about all tables.

**Source:** `internal/catalog/catalog.go`

### Catalog Header

```
 Offset   Size   Field
+--------+------+---------------------+
|   0    |  2   | Magic (0xCDB0)      |
+--------+------+---------------------+
|   2    |  2   | NumTables (uint16)  |
+--------+------+---------------------+
|   4    | var  | TableInfo[]         |
+--------+------+---------------------+
```

### TableInfo Structure

```
 Offset   Size   Field
+--------+------+------------------------+
|   0    |  2   | NameLength (uint16)    |
+--------+------+------------------------+
|   2    | var  | Name (UTF-8 bytes)     |
+--------+------+------------------------+
| varies |  4   | RootPage (uint32)      |
+--------+------+------------------------+
| varies |  8   | NextRowID (uint64)     |
+--------+------+------------------------+
| varies |  4   | PrimaryKey (int32)     |
|        |      | -1 if none             |
+--------+------+------------------------+
| varies |  2   | NumColumns (uint16)    |
+--------+------+------------------------+
| varies | var  | ColumnInfo[]           |
+--------+------+------------------------+
```

### ColumnInfo Structure

```
 Offset   Size   Field
+--------+------+------------------------+
|   0    |  2   | NameLength (uint16)    |
+--------+------+------------------------+
|   2    | var  | Name (UTF-8 bytes)     |
+--------+------+------------------------+
| varies |  1   | ColumnType (uint8)     |
+--------+------+------------------------+
| varies |  1   | Flags (uint8)          |
+--------+------+------------------------+
```

### Column Flags

```
Bit 0 (0x01): PrimaryKey
Bit 1 (0x02): NotNull

Examples:
  0x00 = neither
  0x01 = PrimaryKey only
  0x02 = NotNull only
  0x03 = PrimaryKey AND NotNull
```

### Visual Layout

```
+-------------------------------------------------------------------+
|                       Catalog Page (Page 0)                       |
+-------------------------------------------------------------------+
| Magic  | NumTables |        TableInfo[0]        | TableInfo[1]..  |
| 0xCDB0 |           |                            |                 |
+-------------------------------------------------------------------+
  2 bytes   2 bytes          variable                  variable

TableInfo:
+-------+------+---------+-----------+--------+----------+----------+
|NameLen| Name |RootPage |NextRowID  |  PK    |NumColumns| Columns  |
+-------+------+---------+-----------+--------+----------+----------+
 2 bytes  var    4 bytes   8 bytes    4 bytes   2 bytes    variable

ColumnInfo:
+----------+--------+----------+-------+
| NameLen  |  Name  | ColType  | Flags |
+----------+--------+----------+-------+
  2 bytes    var     1 byte    1 byte
```

### Example: Catalog with "users" Table

```
Catalog with 1 table "users" (id INTEGER PRIMARY KEY, name TEXT NOT NULL):

B0 CD 01 00 05 00 75 73 65 72 73 01 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00 02 00
└───┘ └───┘ └───┘ └──────────────┘ └─────────┘ └──────────────────────┘ └─────────┘ └───┘
Magic NumTbl Len=5  "users"        RootPage=1       NextRowID=1          PK=0      NumCols=2
0xCDB0  =1

03 00 69 64 01 03 04 00 6E 61 6D 65 03 02
└───┘ └──┘ │  │  └───┘ └────────┘ │  │
Len=2 "id" │  │  Len=4  "name"    │  └─Flags=0x02 (NotNull)
           │  └─Flags=0x03 (PK+NotNull)    └─Type=3 (Text)
           └─Type=1 (Integer)
```

---

## 5. Row Location Encoding

B-tree leaf values store row locations as a 64-bit packed value.

**Source:** `internal/table/table.go:290-291`

### Format

```
+--------------------------------+--------------------------------+
|          Bits 63-32            |           Bits 31-0            |
+--------------------------------+--------------------------------+
|       PageID (uint32)          |      PageOffset (uint32)       |
+--------------------------------+--------------------------------+
```

### Encoding/Decoding

```go
// Encode
location := uint64(pageID)<<32 | uint64(offset)

// Decode
pageID := uint32(location >> 32)
offset := uint32(location & 0xFFFFFFFF)
```

### Example

Row at Page 5, Offset 128:

```
PageID = 5      = 0x00000005
Offset = 128    = 0x00000080

Location = (5 << 32) | 128
         = 0x0000000500000080
         = 21474836608 (decimal)

Little-endian bytes: 80 00 00 00 05 00 00 00
```

---

## 6. Constants Reference

| Constant          | Value   | Source                        |
|-------------------|---------|-------------------------------|
| `PageSize`        | 4096    | `internal/storage/page.go`    |
| `PageHeaderSize`  | 16      | `internal/storage/page.go`    |
| `MaxDataSize`     | 4080    | `internal/storage/page.go`    |
| `MaxKeys`         | 100     | `internal/storage/btree.go`   |
| `MinKeys`         | 50      | `internal/storage/btree.go`   |
| `CatalogPageID`   | 0       | `internal/catalog/catalog.go` |
| `CatalogMagic`    | 0xCDB0  | `internal/catalog/catalog.go` |

### DataType Constants

| Value | Constant      | Source                        |
|-------|---------------|-------------------------------|
| 0     | `TypeUnknown` | `internal/sql/parser`         |
| 1     | `TypeInteger` | `internal/sql/parser`         |
| 2     | `TypeReal`    | `internal/sql/parser`         |
| 3     | `TypeText`    | `internal/sql/parser`         |
| 4     | `TypeBoolean` | `internal/sql/parser`         |

### PageType Constants

| Value | Constant              | Source                     |
|-------|-----------------------|----------------------------|
| 0     | `PageTypeFree`        | `internal/storage/page.go` |
| 1     | `PageTypeData`        | `internal/storage/page.go` |
| 2     | `PageTypeBTreeInternal` | `internal/storage/page.go` |
| 3     | `PageTypeBTreeLeaf`   | `internal/storage/page.go` |

---

## File Layout Overview

```
+-------------------------------------------------------------------+
|                       Database File                               |
+-------------------------------------------------------------------+
| Page 0        | Page 1        | Page 2        | ... | Page N      |
| (Catalog)     | (BTree/Data)  | (BTree/Data)  |     | (BTree/Data)|
+-------------------------------------------------------------------+
|<-- 4096 -->|<-- 4096 -->|<-- 4096 -->|     |<-- 4096 -->|

File size = PageCount * 4096 bytes
Page offset = PageID * 4096
```
