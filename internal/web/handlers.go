package web

import (
	"net/http"
)

// handleIndex serves the main page of the web UI.
// This will be expanded with actual HTML templates in a future task.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<!DOCTYPE html>
<html>
<head>
    <title>ClaudeDB</title>
</head>
<body>
    <h1>ClaudeDB Web Interface</h1>
    <p>Welcome to ClaudeDB. This interface will be expanded with query capabilities.</p>
    <p><a href="/health">Health Check</a></p>
</body>
</html>`))
}

// handleHealth returns a simple health check response.
// This endpoint is used by load balancers and monitoring systems.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// handleQueryPage serves the SQL query input form.
// Supports pre-populating the query via ?q= query parameter for bookmarking.
func (s *Server) handleQueryPage(w http.ResponseWriter, r *http.Request) {
	data := map[string]interface{}{
		"Query": "", // Empty initially
	}

	// Pre-populate from query param if provided
	if q := r.URL.Query().Get("q"); q != "" {
		data["Query"] = q
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := RenderTemplate(w, "query.html", data); err != nil {
		http.Error(w, "Template error: "+err.Error(), http.StatusInternalServerError)
	}
}
