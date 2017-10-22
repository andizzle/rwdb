package rwdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"
)

// DB holds connection pool(s)
// This should be created once, and for each acquaring of
// new db pool, use New()
type DB struct {
	cpool           *CPool
	sticky          bool // sticky redirects subsequent queries after a write to Writer DB, this is default to true
	modified        bool
	maxIdle         int
	maxOpen         int
	maxLifetime     time.Duration
	maxIdleModified bool
}

func walk(cpool *CPool, fn func(conn *sql.DB) error) error {
	n := len(cpool.pool)
	errors := make(chan error, n)

	for _, conn := range cpool.pool {
		if conn == nil {
			continue
		}

		go func(conn *sql.DB) {
			errors <- fn(conn)
		}(conn)
	}

	for err := range errors {
		if err != nil {
			close(errors)
			return err
		}
	}

	return nil
}

// Open creates the DB instance
// The opening of each underline connection is non-blocking
func Open(driver string, dataSourceNames ...string) (*DB, error) {
	var db = &DB{cpool: &CPool{}}

	db.SetSticky(true)

	if len(dataSourceNames) == 0 {
		return nil, errors.New("no data source name available")
	}

	d, err := sql.Open(driver, dataSourceNames[0])

	if err != nil {
		// writer failed to open
		// this is fatal
		return nil, err
	}

	db.cpool.AddWriter(d)

	for _, conn := range dataSourceNames[1:] {
		go func(conn string) {
			d, _ := sql.Open(driver, conn)

			if db.maxIdleModified {
				// we don't want set this to 0 blindly
				// once 0 is used, it can't be 0 again
				d.SetMaxIdleConns(db.maxIdle)
			}

			d.SetConnMaxLifetime(db.maxLifetime)
			d.SetMaxOpenConns(db.maxOpen)

			db.cpool.AddReader(d)
		}(conn)
	}

	return db, nil
}

func (db *DB) next() (*sql.DB, error) {
	if db.sticky && db.modified {
		return db.cpool.Writer()
	}

	return db.cpool.Reader()
}

// SetSticky allows sticky be turned on and off
func (db *DB) SetSticky(stick bool) {
	db.sticky = stick
}

// New creates a new DB with the same
// sticky and Connection pool, but reset modified
func (db *DB) New() *DB {
	return &DB{
		cpool:  db.cpool,
		sticky: db.sticky,
	}
}

// Driver returns the driver of the DB
// The Writer's driver represents all readers
func (db *DB) Driver() driver.Driver {
	writer, _ := db.cpool.Writer()

	return writer.Driver()
}

// Begin starts a transaction on Writer
// It's likely the subsequent queries will perform a write
func (db *DB) Begin() (*sql.Tx, error) {
	writer, err := db.cpool.Writer()
	if err != nil {
		return nil, err
	}

	return writer.Begin()
}

// Exec writes to Writer and mark db as modified
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args)
}

// ExecContext execute a query with context
// and mark the db as modified
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	writer, err := db.cpool.Writer()

	if err != nil {
		return nil, err
	}

	result, err := writer.ExecContext(ctx, query, args...)

	if rowAffected, _ := result.RowsAffected(); rowAffected > 0 {
		db.modified = true
	}

	return result, err
}

// Ping execute a ping context with a background context
func (db *DB) Ping() error {
	return db.PingContext(context.Background())
}

// PingContext pings all physical dbs with context
func (db *DB) PingContext(ctx context.Context) error {
	return walk(db.cpool, func(conn *sql.DB) error {
		return conn.PingContext(ctx)
	})
}

// Prepare prepare stateuments with a background context
func (db *DB) Prepare(query string) (Stmt, error) {
	return db.PrepareContext(context.Background(), query)
}

// PrepareContext two statements, one in Writer one in Reader
// The statement will be executed in the writer
// and queries in reader
func (db *DB) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmt := stmt{}

	writer, err := db.cpool.Writer()

	if err != nil {
		return nil, err
	}

	write, err := writer.Prepare(query)

	if err != nil {
		return nil, err
	}

	stmt.stmts = append([]*sql.Stmt{write}, stmt.stmts...)

	if len(db.cpool.pool) > 1 {
		go func() {
			reader, err := db.cpool.Reader()

			if err != nil {
				// we have writer statement prepared
				// this error can be ignored
				return
			}

			read, _ := reader.PrepareContext(ctx, query)
			stmt.stmts = append(stmt.stmts, read)
		}()
	}

	return &stmt, nil
}

// Close closes all physical db connections
func (db *DB) Close() error {
	return walk(db.cpool, func(conn *sql.DB) error {
		return conn.Close()
	})
}

// Query perform a query context with background context
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// The query will be performed in the next connection
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	reader, err := db.next()
	if err != nil {
		return nil, err
	}

	return reader.QueryContext(ctx, query, args...)
}

// QueryRow runs the QueryRowContext with a background context
func (db *DB) QueryRow(query string, args ...interface{}) Row {
	return db.QueryRowContext(context.Background(), query, args)
}

// QueryRowContext perform the underline QueryRowContext of sql.DB
// on the next connection
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) Row {
	reader, err := db.next()
	if err != nil {
		return &row{err: err}
	}

	return reader.QueryRowContext(ctx, query, args...)
}

// SetMaxIdleConns sets the max idel conns
// for all connections. This is concurrency safe.
func (db *DB) SetMaxIdleConns(n int) {
	db.maxIdle = n
	db.maxIdleModified = true

	walk(db.cpool, func(conn *sql.DB) error {
		conn.SetMaxIdleConns(n)
		return nil
	})
}

// SetMaxOpenConns sets the max open connections limit
// for all connections. This is concurrency safe.
func (db *DB) SetMaxOpenConns(n int) {
	db.maxOpen = n

	walk(db.cpool, func(conn *sql.DB) error {
		conn.SetMaxOpenConns(n)
		return nil
	})
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused
// for all connections. This is concurrency safe.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.maxLifetime = d

	walk(db.cpool, func(conn *sql.DB) error {
		conn.SetConnMaxLifetime(d)
		return nil
	})
}
