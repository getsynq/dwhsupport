package querylogs

import (
	"context"
	"io"

	"github.com/jmoiron/sqlx"
)

// SqlxRowsIterator is a generic iterator for sqlx.Rows that converts rows to QueryLog.
// It automatically handles row iteration, scanning, conversion, and skipping nil results.
type SqlxRowsIterator[T any] struct {
	rows       *sqlx.Rows
	convertFn  func(*T) (*QueryLog, error)
	obfuscator QueryObfuscator
	closed     bool
}

// NewSqlxRowsIterator creates a new iterator for sqlx.Rows with a conversion function.
// The conversion function should return nil to skip a row, or an error to stop iteration.
func NewSqlxRowsIterator[T any](rows *sqlx.Rows, obfuscator QueryObfuscator, convertFn func(*T, QueryObfuscator) (*QueryLog, error)) QueryLogIterator {
	return &SqlxRowsIterator[T]{
		rows:       rows,
		convertFn:  func(row *T) (*QueryLog, error) { return convertFn(row, obfuscator) },
		obfuscator: obfuscator,
	}
}

func (it *SqlxRowsIterator[T]) Next(ctx context.Context) (*QueryLog, error) {
	for {
		if it.closed {
			return nil, io.EOF
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			it.Close()
			return nil, ctx.Err()
		default:
		}

		// Use native rows.Next() iteration
		if !it.rows.Next() {
			// No more rows - check for error
			if err := it.rows.Err(); err != nil {
				// Don't auto-close on error - caller might want to inspect
				return nil, err
			}
			// Normal completion - auto-close
			it.Close()
			return nil, io.EOF
		}

		// Scan into struct
		var row T
		if err := it.rows.StructScan(&row); err != nil {
			// Defensive: scan error, but don't crash entire ingestion
			// Caller can decide whether to continue
			return nil, err
		}

		// Convert to QueryLog
		log, err := it.convertFn(&row)
		if err != nil {
			// Defensive: conversion error
			return nil, err
		}

		// If nil is returned, it means we should skip this row (e.g., running queries)
		if log == nil {
			continue
		}

		return log, nil
	}
}

func (it *SqlxRowsIterator[T]) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	return it.rows.Close()
}
