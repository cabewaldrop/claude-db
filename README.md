# Claude DB - An Educational SQL Database

A simple SQL database built from scratch in Go, designed for learning how databases work internally.

## Features

- **Page-based storage engine** with 4KB fixed-size pages
- **B+ tree indexing** for efficient key lookups
- **SQL parser** with lexer and recursive descent parser
- **Query executor** supporting common SQL operations
- **Interactive REPL** for executing queries
- **Data persistence** - tables and data survive restarts
- **Catalog system** - stores table metadata for recovery

## Supported SQL

```sql
-- Data Definition
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
DROP TABLE users;

-- Data Manipulation
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
UPDATE users SET age = 31 WHERE name = 'Alice';
DELETE FROM users WHERE age < 18;

-- Queries
SELECT * FROM users;
SELECT name, age FROM users WHERE age > 25;
SELECT * FROM users ORDER BY age DESC;
SELECT * FROM users LIMIT 10 OFFSET 5;
SELECT * FROM users WHERE age > 18 AND name != 'Admin';
```

## Building and Running

```bash
# Build
go build -o claude-db ./cmd/claude-db

# Run (data persists in mydata.db)
./claude-db -db mydata.db

# Run tests
go test ./...
```

## REPL Commands

```
.help    - Show help message
.tables  - List all tables
.schema  - Show schema for all tables
.quit    - Exit (data is automatically saved)
```

## Project Structure

```
claude-db/
├── cmd/claude-db/          # CLI entry point with REPL
├── internal/
│   ├── catalog/            # Table metadata persistence
│   ├── storage/            # Storage engine
│   │   ├── page.go         # Fixed-size page implementation
│   │   ├── pager.go        # Page cache and file I/O
│   │   └── btree.go        # B+ tree index
│   ├── sql/
│   │   ├── lexer/          # SQL tokenizer
│   │   ├── parser/         # Recursive descent parser + AST
│   │   └── executor/       # Query execution engine
│   └── table/              # Table schema and row operations
```

## How It Works

### 1. Storage Engine (internal/storage/)

The storage engine manages how data is persisted to disk:

**Pages** are fixed-size blocks (4KB) that serve as the fundamental unit of I/O:
- Reading/writing fixed-size blocks is optimal for disk access
- Pages can be cached in memory for faster access
- Each page has a header with metadata and a data area

**The Pager** manages the database file:
- Allocates new pages when needed
- Caches frequently accessed pages in memory
- Flushes dirty (modified) pages to disk

**B+ Trees** provide efficient key-value lookup:
- All data is stored in leaf nodes
- Internal nodes contain separator keys for navigation
- Supports O(log n) search, insert, and scan operations

### 2. Catalog System (internal/catalog/)

The catalog stores metadata about tables:
- Uses page 0 as a special "catalog page"
- Stores table names, column definitions, and root page IDs
- Enables table recovery when database is reopened

### 3. SQL Parser (internal/sql/)

The parser converts SQL text into an Abstract Syntax Tree (AST):

**Lexer** breaks input into tokens:
```
"SELECT name FROM users" → [SELECT] [IDENT:name] [FROM] [IDENT:users]
```

**Parser** builds a tree structure:
```
SelectStatement
├── Columns: [name]
├── From: users
└── Where: nil
```

The parser uses **recursive descent** with **operator precedence parsing** (Pratt parsing) for expressions.

### 4. Query Executor (internal/sql/executor/)

The executor walks the AST and performs operations:

1. **CREATE TABLE**: Creates schema, allocates storage, updates catalog
2. **INSERT**: Validates data, serializes row, stores in page, updates index
3. **SELECT**: Scans table, applies filters (WHERE), projects columns, sorts (ORDER BY)
4. **UPDATE/DELETE**: Finds matching rows and modifies/removes them

### 5. Table Management (internal/table/)

Tables combine schema (column definitions) with data storage:
- **Schema**: Column names, types, constraints (PRIMARY KEY, NOT NULL)
- **Rows**: Serialized values stored in data pages
- **Index**: B+ tree for primary key lookups

## Educational Notes

### Why Pages?

Databases use fixed-size pages because:
1. **Disk alignment**: Hard drives and SSDs work best with aligned blocks
2. **Caching**: Fixed sizes make memory management predictable
3. **Atomicity**: A page can be written atomically (crash safety)

### Why B+ Trees?

B+ trees are ideal for databases because:
1. **High fanout**: Each node holds many keys, reducing tree depth
2. **Sequential access**: Leaves can be linked for range scans
3. **Balanced**: All leaves at same depth, predictable performance

### Why a Catalog?

The catalog solves a chicken-and-egg problem:
1. To read table data, we need to know the schema
2. The schema must be stored somewhere persistent
3. The catalog stores this metadata in a known location (page 0)

### Why Separate Lexer and Parser?

Separating concerns makes the code cleaner:
1. **Lexer**: Handles character-level details (whitespace, string escaping)
2. **Parser**: Focuses on grammar and structure
3. **Executor**: Deals with data operations

## Code Statistics

- ~6,600 lines of Go code
- 19 source files
- Comprehensive test coverage

## Limitations

This is an educational implementation. It lacks:
- Transactions and ACID guarantees
- Concurrent access control
- Crash recovery (WAL)
- Query optimization
- JOINs and subqueries
- Indexes beyond primary key

## License

MIT License - Use freely for learning!
