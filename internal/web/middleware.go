// Package web - Database connection middleware
//
// EDUCATIONAL NOTES:
// ------------------
// Middleware in Go HTTP servers wraps handlers to add cross-cutting concerns.
// Context-based dependency injection is a common pattern:
//
// 1. Outer middleware injects dependencies into request context
// 2. Handlers retrieve dependencies from context when needed
// 3. Inner middleware can require dependencies and fail fast if missing
//
// This approach:
// - Keeps handlers decoupled from global state
// - Makes testing easier (inject test doubles)
// - Provides a clean way to pass request-scoped data

package web

import (
	"context"
	"net/http"

	"github.com/cabewaldrop/claude-db/internal/sql/executor"
)

// contextKey is a custom type for context keys to avoid collisions.
// Using a custom type prevents other packages from accidentally
// overwriting our context values with the same string key.
type contextKey string

// executorKey is the context key for storing the SQL executor.
const executorKey contextKey = "executor"

// WithExecutor returns middleware that injects the SQL executor into
// the request context. Handlers can retrieve it using GetExecutor.
//
// EDUCATIONAL NOTE:
// -----------------
// This middleware runs early in the chain to ensure the executor is
// available to all downstream handlers. It's a form of dependency
// injection via context.
//
// Usage:
//
//	router.Use(WithExecutor(exec))
//	router.Get("/query", func(w http.ResponseWriter, r *http.Request) {
//	    exec := GetExecutor(r)
//	    // use exec to run queries
//	})
func WithExecutor(exec *executor.Executor) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), executorKey, exec)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// GetExecutor retrieves the SQL executor from the request context.
// Returns nil if the executor was not set (middleware not applied).
//
// EDUCATIONAL NOTE:
// -----------------
// This is a helper function that handles the type assertion for you.
// Always check the return value for nil before using it, or use
// RequireExecutor middleware to guarantee availability.
func GetExecutor(r *http.Request) *executor.Executor {
	exec, ok := r.Context().Value(executorKey).(*executor.Executor)
	if !ok {
		return nil
	}
	return exec
}

// RequireExecutor returns middleware that ensures an executor is present
// in the request context. If not found, it returns 500 Internal Server Error.
//
// EDUCATIONAL NOTE:
// -----------------
// Use this middleware on routes that absolutely require database access.
// It provides a clear error message and prevents nil pointer panics in
// handlers that assume the executor is available.
//
// Usage:
//
//	router.Route("/api", func(r chi.Router) {
//	    r.Use(RequireExecutor)  // All /api routes require executor
//	    r.Get("/tables", handleListTables)
//	})
func RequireExecutor(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if GetExecutor(r) == nil {
			http.Error(w, "Database not available", http.StatusInternalServerError)
			return
		}
		next.ServeHTTP(w, r)
	})
}
