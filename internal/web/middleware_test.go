package web

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/cabewaldrop/claude-db/internal/sql/executor"
	"github.com/cabewaldrop/claude-db/internal/storage"
)

func setupTestExecutor(t *testing.T) (*executor.Executor, func()) {
	t.Helper()
	testFile := "test_middleware.db"
	pager, err := storage.NewPager(testFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}

	exec := executor.New(pager)

	cleanup := func() {
		pager.Close()
		os.Remove(testFile)
	}

	return exec, cleanup
}

func TestWithExecutorMiddleware(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Create a handler that checks for the executor in context
	var gotExec *executor.Executor
	handler := WithExecutor(exec)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotExec = GetExecutor(r)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
	if gotExec == nil {
		t.Error("expected executor in context, got nil")
	}
	if gotExec != exec {
		t.Error("expected same executor instance")
	}
}

func TestWithExecutorMiddlewareNil(t *testing.T) {
	// Test that nil executor is stored correctly
	var gotExec *executor.Executor
	gotCalled := false

	handler := WithExecutor(nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCalled = true
		gotExec = GetExecutor(r)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !gotCalled {
		t.Error("handler was not called")
	}
	if gotExec != nil {
		t.Error("expected nil executor when nil was passed to middleware")
	}
}

func TestRequireExecutorRejects(t *testing.T) {
	// Handler that should never be reached
	handlerCalled := false
	handler := RequireExecutor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		t.Fatal("should not reach handler when executor is missing")
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if handlerCalled {
		t.Error("handler should not have been called")
	}
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "Database not available") {
		t.Errorf("expected 'Database not available' in response, got %q", rec.Body.String())
	}
}

func TestRequireExecutorAllows(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Chain middlewares: WithExecutor -> RequireExecutor -> handler
	handlerCalled := false
	innerHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	handler := WithExecutor(exec)(RequireExecutor(innerHandler))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !handlerCalled {
		t.Error("handler should have been called with executor present")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestGetExecutorWithoutMiddleware(t *testing.T) {
	// Test GetExecutor on a request without any middleware applied
	req := httptest.NewRequest("GET", "/", nil)
	exec := GetExecutor(req)

	if exec != nil {
		t.Error("expected nil executor when middleware not applied")
	}
}

func TestGetExecutorReturnsNilForWrongType(t *testing.T) {
	// Test that GetExecutor handles wrong type gracefully
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Store wrong type in context using same key trick is not possible
		// since we use a private contextKey type, but we can test that
		// GetExecutor returns nil when value is missing
		exec := GetExecutor(r)
		if exec != nil {
			t.Error("expected nil when executor not in context")
		}
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
}

func TestMiddlewareChainWithMultipleHandlers(t *testing.T) {
	exec, cleanup := setupTestExecutor(t)
	defer cleanup()

	// Simulate a realistic middleware chain
	var handlerExec *executor.Executor

	// Custom middleware that checks executor is available
	checkMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if GetExecutor(r) == nil {
				t.Error("executor should be available in middleware chain")
			}
			next.ServeHTTP(w, r)
		})
	}

	handler := WithExecutor(exec)(
		checkMiddleware(
			http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handlerExec = GetExecutor(r)
				w.WriteHeader(http.StatusOK)
			}),
		),
	)

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if handlerExec != exec {
		t.Error("executor should propagate through middleware chain")
	}
}
