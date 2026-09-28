// Package schema holds the embedded, versioned goose SQL migrations of the 015
// recovery control store.
//
// This filesystem is deliberately separate from the data DB's root migrations
// package (migrations/embed.go): the control store owns its own recovery_*
// schema and its own version sequence, and `recovery-admin migrate` will only
// ever apply it to the independent control DSN
// (TXHARBOR_RECOVERY_CONTROL_DSN). The data DB migration files and its
// goose_db_version table stay untouched — 015 makes zero data-DB schema
// changes this phase.
//
// The embed directive must live next to the .sql files, which is why this tiny
// package exists.
package schema

import "embed"

// FS contains the control-store goose migration files (NNNNNN_name.sql), in
// their own version sequence, independent of the data DB migrations.
//
//go:embed *.sql
var FS embed.FS
