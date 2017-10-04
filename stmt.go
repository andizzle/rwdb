package rwdb

import (
	"context"
	"database/sql"
	"errors"
)

// Stmt allows DB
type Stmt interface {
	Close() error
	Exec(args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error)
	Query(args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error)
	QueryRow(args ...interface{}) Row
	QueryRowContext(ctx context.Context, args ...interface{}) Row
}

// stmt holds at most 2 sql.Stmt
type stmt struct {
	stmts []*sql.Stmt
}

// Close will close the statments connections
func (s *stmt) Close() error {
	for _, s := range s.stmts {
		s.Close()
	}

	return nil
}

// Exec execute statement with background context
func (s *stmt) Exec(args ...interface{}) (sql.Result, error) {
	return s.ExecContext(context.Background(), args...)
}

// Exec execute statement with context
// The statement is executed on the writer database
func (s *stmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	if len(s.stmts) == 0 {
		return nil, errors.New("zero statement executable")
	}

	return s.stmts[0].Exec(args...)
}

// Query execute statement with background context
func (s *stmt) Query(args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), args...)
}

// Query execute statement with context
// The statement is executed on reader database
func (s *stmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	if len(s.stmts) == 0 {
		return nil, errors.New("zero statement executable")
	}

	stmt := s.stmts[0]

	if len(s.stmts) > 1 {
		return s.stmts[1].QueryContext(ctx, args...)
	}

	return stmt.QueryContext(ctx, args...)
}

// QueryRow query the statement with background context
func (s *stmt) QueryRow(args ...interface{}) Row {
	return s.QueryRowContext(context.Background(), args...)
}

// QueryRowContext is executed on reader database
func (s *stmt) QueryRowContext(ctx context.Context, args ...interface{}) Row {
	if len(s.stmts) == 0 {
		return &row{err: errors.New("zero statement executable")}
	}

	stmt := s.stmts[0]
	if len(s.stmts) > 1 {
		return s.stmts[1].QueryRowContext(ctx, args...)
	}

	return stmt.QueryRowContext(ctx, args...)
}
