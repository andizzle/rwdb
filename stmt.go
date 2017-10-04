package rwdb

import (
	"context"
	"database/sql"
)

type Stmt struct {
	stmts []*sql.Stmt
}

func (s *Stmt) Close() error {
	for _, s := range s.stmts {
		s.Close()
	}

	return nil
}

func (s *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	return s.ExecContext(context.Background(), args...)
}

func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	return s.stmts[0].Exec(args...)
}

func (s *Stmt) Query(args ...interface{}) (*sql.Rows, error) {
	return s.QueryContext(context.Background(), args...)
}

func (s *Stmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	stmt := s.stmts[0]
	if len(s.stmts) > 1 {
		stmt = s.stmts[1]
	}

	return stmt.QueryContext(ctx, args...)
}

func (s *Stmt) QueryRow(args ...interface{}) *sql.Row {
	return s.QueryRowContext(context.Background(), args...)
}

func (s *Stmt) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	stmt := s.stmts[0]
	if len(s.stmts) > 1 {
		stmt = s.stmts[1]
	}

	return stmt.QueryRowContext(ctx, args...)
}
