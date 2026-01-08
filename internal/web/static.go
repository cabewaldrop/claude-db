// Package web provides the HTTP server for the database web UI.
//
// This file handles static file serving using Go's embed package.
// Static files like JavaScript and CSS are embedded into the binary
// at compile time, making deployment easier (single binary).

package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var staticFS embed.FS

// staticRoutes sets up routes for serving static files.
// Files are served from /static/* path, stripping the prefix before lookup.
//
// EDUCATIONAL NOTE:
// -----------------
// Using embed.FS allows static files to be compiled into the binary.
// This has several advantages:
// 1. Single binary deployment - no need to manage separate asset files
// 2. Files can't be accidentally modified in production
// 3. Faster startup - no disk I/O to load assets
//
// The fs.Sub call creates a sub-filesystem rooted at "static", so
// a request for /static/htmx.min.js looks up "htmx.min.js" in the FS.
func (s *Server) staticRoutes() {
	// Create a sub-filesystem rooted at "static" directory
	staticContent, err := fs.Sub(staticFS, "static")
	if err != nil {
		// This should never happen since "static" is embedded at compile time
		panic("failed to create static file sub-filesystem: " + err.Error())
	}

	// Create a file server for the embedded files
	fileServer := http.FileServer(http.FS(staticContent))

	// Strip /static/ prefix before serving
	// Request: /static/htmx.min.js -> Lookup: htmx.min.js
	s.router.Handle("/static/*", http.StripPrefix("/static/", fileServer))
}
