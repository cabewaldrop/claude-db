// Package main implements the CLI interface for claude-db.
//
// EDUCATIONAL NOTES:
// ------------------
// This is the entry point for our database CLI. It provides:
// 1. A REPL (Read-Eval-Print Loop) for interactive SQL queries
// 2. Command-line arguments for configuration
// 3. Special commands for database administration
// 4. Persistence of data across restarts via the catalog system
//
// The REPL pattern is common in interactive tools:
// - Read: Get input from user
// - Eval: Parse and execute the input
// - Print: Display the result
// - Loop: Repeat until user exits

package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/cabewaldrop/claude-db/internal/catalog"
	"github.com/cabewaldrop/claude-db/internal/sql/executor"
	"github.com/cabewaldrop/claude-db/internal/sql/lexer"
	"github.com/cabewaldrop/claude-db/internal/sql/parser"
	"github.com/cabewaldrop/claude-db/internal/storage"
)

const (
	version = "0.2.0"
	banner  = `
   _____ _                 _          _____  ____
  / ____| |               | |        |  __ \|  _ \
 | |    | | __ _ _   _  __| | ___    | |  | | |_) |
 | |    | |/ _' | | | |/ _' |/ _ \   | |  | |  _ <
 | |____| | (_| | |_| | (_| |  __/   | |__| | |_) |
  \_____|_|\__,_|\__,_|\__,_|\___|   |_____/|____/

  An Educational SQL Database - Version %s
  Type '.help' for usage hints or '.quit' to exit.
`
)

// dotCommands are special commands starting with '.'
var dotCommands = map[string]string{
	".help":   "Show this help message",
	".quit":   "Exit the program",
	".exit":   "Exit the program (alias for .quit)",
	".tables": "List all tables",
	".schema": "Show schema for all tables or a specific table",
	".clear":  "Clear the screen",
}

func main() {
	// Parse command line flags
	dbPath := flag.String("db", "claude.db", "Path to database file")
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("claude-db version %s\n", version)
		return
	}

	// Print banner
	fmt.Printf(banner, version)

	// Initialize pager (storage layer)
	pager, err := storage.NewPager(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer pager.Close()

	// Initialize catalog for persistence
	cat, err := catalog.NewCatalog(pager)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing catalog: %v\n", err)
		os.Exit(1)
	}

	// Initialize executor with catalog
	exec, err := executor.NewWithCatalog(pager, cat)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading database: %v\n", err)
		os.Exit(1)
	}

	// Show loaded tables
	tables := exec.GetTables()
	if len(tables) > 0 {
		fmt.Printf("Loaded %d table(s): %s\n\n", len(tables), strings.Join(tables, ", "))
	}

	// Start REPL
	repl(exec)
}

// repl implements the Read-Eval-Print Loop.
func repl(exec *executor.Executor) {
	reader := bufio.NewReader(os.Stdin)
	var inputBuffer strings.Builder

	for {
		// Print prompt
		if inputBuffer.Len() == 0 {
			fmt.Print("claude-db> ")
		} else {
			fmt.Print("       ...> ")
		}

		// Read line
		line, err := reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				// Flush changes before exit
				exec.Flush()
				fmt.Println("\nGoodbye!")
				return
			}
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			continue
		}

		line = strings.TrimRight(line, "\n\r")

		// Handle empty line
		if strings.TrimSpace(line) == "" {
			continue
		}

		// Handle dot commands
		if strings.HasPrefix(strings.TrimSpace(line), ".") {
			handleDotCommand(strings.TrimSpace(line), exec)
			continue
		}

		// Accumulate input
		inputBuffer.WriteString(line)

		// Check if statement is complete (ends with semicolon)
		input := strings.TrimSpace(inputBuffer.String())
		if !strings.HasSuffix(input, ";") {
			inputBuffer.WriteString(" ")
			continue
		}

		// Remove trailing semicolon for parsing
		input = strings.TrimSuffix(input, ";")
		inputBuffer.Reset()

		// Execute the statement
		executeSQL(input, exec)
	}
}

// handleDotCommand processes special dot commands.
func handleDotCommand(cmd string, exec *executor.Executor) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return
	}

	switch parts[0] {
	case ".help":
		fmt.Println("\nAvailable commands:")
		for cmd, desc := range dotCommands {
			fmt.Printf("  %-12s %s\n", cmd, desc)
		}
		fmt.Println("\nSQL Commands:")
		fmt.Println("  CREATE TABLE name (column definitions)")
		fmt.Println("  DROP TABLE name")
		fmt.Println("  INSERT INTO table (columns) VALUES (values)")
		fmt.Println("  SELECT columns FROM table [WHERE condition] [ORDER BY ...] [LIMIT n]")
		fmt.Println("  UPDATE table SET column = value [WHERE condition]")
		fmt.Println("  DELETE FROM table [WHERE condition]")
		fmt.Println()

	case ".quit", ".exit":
		// Flush changes before exit
		exec.Flush()
		fmt.Println("Goodbye!")
		os.Exit(0)

	case ".tables":
		tables := exec.GetTables()
		if len(tables) == 0 {
			fmt.Println("No tables found.")
		} else {
			fmt.Println("Tables:")
			for _, name := range tables {
				fmt.Printf("  %s\n", name)
			}
		}

	case ".schema":
		if len(parts) > 1 {
			// Show schema for specific table
			tableName := parts[1]
			showTableSchema(tableName, exec)
		} else {
			// Show schema for all tables
			tables := exec.GetTables()
			for _, name := range tables {
				showTableSchema(name, exec)
			}
		}

	case ".clear":
		// ANSI escape code to clear screen
		fmt.Print("\033[H\033[2J")

	default:
		fmt.Printf("Unknown command: %s\n", parts[0])
		fmt.Println("Type '.help' for available commands.")
	}
}

// showTableSchema displays the schema for a table.
func showTableSchema(name string, exec *executor.Executor) {
	tbl, ok := exec.GetTable(name)
	if !ok {
		fmt.Printf("Table '%s' not found.\n", name)
		return
	}

	fmt.Printf("CREATE TABLE %s (\n", name)
	for i, col := range tbl.Schema.Columns {
		suffix := ""
		if col.PrimaryKey {
			suffix = " PRIMARY KEY"
		} else if col.NotNull {
			suffix = " NOT NULL"
		}
		comma := ","
		if i == len(tbl.Schema.Columns)-1 {
			comma = ""
		}
		fmt.Printf("  %s %s%s%s\n", col.Name, col.Type, suffix, comma)
	}
	fmt.Println(");")
}

// executeSQL parses and executes a SQL statement.
func executeSQL(input string, exec *executor.Executor) {
	// Lexer
	lex := lexer.New(input)

	// Parser
	p := parser.New(lex)
	stmt, err := p.Parse()
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}

	if stmt == nil {
		fmt.Println("Error: Could not parse statement")
		return
	}

	// Execute
	result, err := exec.Execute(stmt)
	if err != nil {
		fmt.Printf("Execution error: %v\n", err)
		return
	}

	// Print result
	fmt.Print(result.String())

	// Flush after modifying operations
	exec.Flush()
}
