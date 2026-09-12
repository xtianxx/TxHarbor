// Package migrations holds the embedded, versioned goose SQL migrations.
//
// The embed directive must live next to the .sql files, which is why this
// tiny package exists at the repository root (Go embed cannot reach parent
// directories from internal/db).
package migrations

import "embed"

// FS contains all goose migration files (NNNNNN_name.sql).
//
//go:embed *.sql
var FS embed.FS
