package web

import "testing"

func TestIsValidIdentifier(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{"users", true},
		{"user_table", true},
		{"_private", true},
		{"Table1", true},
		{"a", true},
		{"_", true},
		{"a1b2c3", true},
		{"__double__underscore__", true},
		{"123start", false},   // starts with number
		{"has-dash", false},   // contains dash
		{"has space", false},  // contains space
		{"has.dot", false},    // contains dot
		{"has@symbol", false}, // contains @
		{"", false},           // empty
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := IsValidIdentifier(tt.input)
			if got != tt.valid {
				t.Errorf("IsValidIdentifier(%q) = %v, want %v", tt.input, got, tt.valid)
			}
		})
	}
}

func TestIsValidType(t *testing.T) {
	tests := []struct {
		input string
		valid bool
	}{
		{"INTEGER", true},
		{"TEXT", true},
		{"REAL", true},
		{"BLOB", true},
		{"integer", false}, // case sensitive
		{"VARCHAR", false}, // not supported
		{"INT", false},     // not in our list
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := IsValidType(tt.input)
			if got != tt.valid {
				t.Errorf("IsValidType(%q) = %v, want %v", tt.input, got, tt.valid)
			}
		})
	}
}
