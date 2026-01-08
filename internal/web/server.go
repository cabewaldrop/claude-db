// Package web provides the HTTP server for the database web UI.
//
// EDUCATIONAL NOTES:
// ------------------
// This package sets up an HTTP server using the chi router, which is a
// lightweight, idiomatic Go router. Key concepts:
//
// 1. Middleware: Functions that wrap handlers to add cross-cutting concerns
//    like logging, recovery from panics, and request timeouts.
//
// 2. Graceful shutdown: When the server receives a termination signal,
//    it stops accepting new connections but finishes processing in-flight
//    requests before shutting down.
//
// 3. Dependency injection: The Executor is passed into the server so
//    handlers can execute SQL queries against the database.

package web

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/cabewaldrop/claude-db/internal/sql/executor"
)

// Server represents the HTTP server for the database web UI.
type Server struct {
	router   *chi.Mux
	port     int
	executor *executor.Executor
}

// NewServer creates a new HTTP server with the given port and executor.
// If executor is nil, database operations will not be available.
func NewServer(port int, exec *executor.Executor) *Server {
	r := chi.NewRouter()

	// Middleware stack
	// RequestID: Adds a unique ID to each request for tracing
	r.Use(middleware.RequestID)
	// RealIP: Extracts the real client IP from X-Forwarded-For headers
	r.Use(middleware.RealIP)
	// Logger: Logs each request (method, path, duration)
	r.Use(middleware.Logger)
	// Recoverer: Catches panics in handlers, logs stack trace, returns 500
	r.Use(middleware.Recoverer)
	// Timeout: Cancels request context after 30 seconds
	r.Use(middleware.Timeout(30 * time.Second))

	s := &Server{
		router:   r,
		port:     port,
		executor: exec,
	}

	s.routes()
	return s
}

// routes sets up all HTTP routes for the server.
func (s *Server) routes() {
	s.router.Get("/", s.handleIndex)
	s.router.Get("/health", s.handleHealth)

	// Static file serving (JS, CSS)
	s.staticRoutes()

	// Table browsing endpoints
	s.router.Get("/tables", s.handleTableList)

	// Additional routes will be added by other tasks:
	// - Query execution endpoints
	// - Individual table detail endpoints
}

// Router returns the chi router for testing purposes.
func (s *Server) Router() http.Handler {
	return s.router
}

// Run starts the HTTP server and blocks until shutdown.
// It handles graceful shutdown on SIGTERM and SIGINT.
func (s *Server) Run() error {
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", s.port),
		Handler:      s.router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Channel to receive shutdown signals
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	// Channel to receive server errors
	errChan := make(chan error, 1)

	// Start server in a goroutine
	go func() {
		fmt.Printf("Starting server on port %d\n", s.port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or server error
	select {
	case <-done:
		fmt.Println("\nShutdown signal received, gracefully shutting down...")
	case err := <-errChan:
		return fmt.Errorf("server error: %w", err)
	}

	// Graceful shutdown with 5 second timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown error: %w", err)
	}

	fmt.Println("Server stopped")
	return nil
}
