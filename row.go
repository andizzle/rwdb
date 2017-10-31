package rwdb

import (
	"database/sql"
)

// Row allow us to write our custom row struct
type Row interface {
	Scan(dest ...interface{}) error
}

type row struct {
	sql.Row

	err error
}
