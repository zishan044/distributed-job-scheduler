package migrations

import "embed"

// FS contains the database migrations used by the migrate service.
//
//go:embed *.sql
var FS embed.FS
