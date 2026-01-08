// Package web provides HTTP handlers and utilities for the web interface.
package web

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cabewaldrop/claude-db/internal/sql/executor"
)

// QueryPlan represents a parsed EXPLAIN output for display.
type QueryPlan struct {
	Type          string   // FULL_TABLE_SCAN, INDEX_LOOKUP, INDEX_RANGE_SCAN
	Table         string   // Table being accessed
	Index         string   // Index name (empty for table scans)
	EstimatedCost float64  // Estimated cost of the operation
	Predicates    []string // List of predicate conditions
	RawPlan       string   // Original plan description
}

// ParseExplainOutput parses an EXPLAIN result into a structured QueryPlan.
// Returns nil and an error if the result cannot be parsed.
// Falls back to RawPlan if parsing partially fails.
func ParseExplainOutput(result *executor.Result) (*QueryPlan, error) {
	if result == nil {
		return nil, fmt.Errorf("nil result")
	}

	if len(result.Rows) == 0 {
		return nil, fmt.Errorf("empty explain output")
	}

	plan := &QueryPlan{}

	// Parse each row of the EXPLAIN output
	// Format: Property (column 0), Value (column 1)
	for _, row := range result.Rows {
		if len(row) < 2 {
			continue
		}

		property := row[0].Text
		value := row[1].Text

		switch property {
		case "Query Plan":
			plan.RawPlan = value
			// Try to extract table name from plan string
			// Format: "SELECT on <table> using <method>"
			plan.Table = extractTableFromPlan(value)
		case "Access Method":
			plan.Type = value
			// Extract index info from access method if it's an index scan
			if strings.HasPrefix(value, "INDEX") {
				plan.Index = extractIndexFromPlan(plan.RawPlan)
			}
		case "Estimated Cost":
			cost, err := strconv.ParseFloat(value, 64)
			if err == nil {
				plan.EstimatedCost = cost
			}
		default:
			// Predicate rows start with "  [" or "Predicates"
			if strings.HasPrefix(property, "  [") {
				plan.Predicates = append(plan.Predicates, value)
			}
		}
	}

	// Ensure we have at least a type
	if plan.Type == "" {
		plan.Type = "UNKNOWN"
	}

	return plan, nil
}

// extractTableFromPlan extracts the table name from a plan description.
// Expected format: "SELECT on <table> using <method>"
func extractTableFromPlan(planStr string) string {
	// Look for " on " pattern
	parts := strings.Split(planStr, " on ")
	if len(parts) >= 2 {
		// Table name is after " on ", may have " using " after
		rest := parts[1]
		if idx := strings.Index(rest, " using "); idx >= 0 {
			return strings.TrimSpace(rest[:idx])
		}
		if idx := strings.Index(rest, " "); idx >= 0 {
			return strings.TrimSpace(rest[:idx])
		}
		return strings.TrimSpace(rest)
	}
	return ""
}

// extractIndexFromPlan extracts the index name from a plan description.
// Expected format: "... using <index_name>"
func extractIndexFromPlan(planStr string) string {
	parts := strings.Split(planStr, " using ")
	if len(parts) >= 2 {
		indexPart := parts[1]
		// Index name might be followed by other info
		if idx := strings.Index(indexPart, " "); idx >= 0 {
			return strings.TrimSpace(indexPart[:idx])
		}
		return strings.TrimSpace(indexPart)
	}
	return ""
}

// FormatPlanHTML formats a QueryPlan as HTML for web display.
func (p *QueryPlan) FormatPlanHTML() string {
	var sb strings.Builder

	sb.WriteString(`<div class="query-plan">`)
	sb.WriteString(`<h4>Query Plan</h4>`)

	// Access method with visual indicator
	sb.WriteString(`<div class="plan-row">`)
	sb.WriteString(`<span class="plan-label">Access Method:</span>`)
	sb.WriteString(fmt.Sprintf(`<span class="plan-value access-%s">%s</span>`,
		strings.ToLower(strings.ReplaceAll(p.Type, "_", "-")), p.Type))
	sb.WriteString(`</div>`)

	// Table
	if p.Table != "" {
		sb.WriteString(`<div class="plan-row">`)
		sb.WriteString(`<span class="plan-label">Table:</span>`)
		sb.WriteString(fmt.Sprintf(`<span class="plan-value">%s</span>`, p.Table))
		sb.WriteString(`</div>`)
	}

	// Index
	if p.Index != "" {
		sb.WriteString(`<div class="plan-row">`)
		sb.WriteString(`<span class="plan-label">Index:</span>`)
		sb.WriteString(fmt.Sprintf(`<span class="plan-value">%s</span>`, p.Index))
		sb.WriteString(`</div>`)
	}

	// Estimated cost
	sb.WriteString(`<div class="plan-row">`)
	sb.WriteString(`<span class="plan-label">Estimated Cost:</span>`)
	sb.WriteString(fmt.Sprintf(`<span class="plan-value">%.2f</span>`, p.EstimatedCost))
	sb.WriteString(`</div>`)

	// Predicates
	if len(p.Predicates) > 0 {
		sb.WriteString(`<div class="plan-predicates">`)
		sb.WriteString(`<span class="plan-label">Predicates:</span>`)
		sb.WriteString(`<ul>`)
		for _, pred := range p.Predicates {
			sb.WriteString(fmt.Sprintf(`<li>%s</li>`, pred))
		}
		sb.WriteString(`</ul>`)
		sb.WriteString(`</div>`)
	}

	sb.WriteString(`</div>`)
	return sb.String()
}

// FormatPlanText formats a QueryPlan as plain text.
func (p *QueryPlan) FormatPlanText() string {
	var sb strings.Builder

	sb.WriteString("=== Query Plan ===\n")
	sb.WriteString(fmt.Sprintf("Access Method: %s\n", p.Type))
	if p.Table != "" {
		sb.WriteString(fmt.Sprintf("Table: %s\n", p.Table))
	}
	if p.Index != "" {
		sb.WriteString(fmt.Sprintf("Index: %s\n", p.Index))
	}
	sb.WriteString(fmt.Sprintf("Estimated Cost: %.2f\n", p.EstimatedCost))

	if len(p.Predicates) > 0 {
		sb.WriteString("Predicates:\n")
		for i, pred := range p.Predicates {
			sb.WriteString(fmt.Sprintf("  [%d] %s\n", i+1, pred))
		}
	}

	return sb.String()
}
