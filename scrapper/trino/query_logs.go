package trino

import (
	"context"
	_ "embed"
	"io"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/jmoiron/sqlx"
)

//go:embed query_logs.sql
var queryLogsSql string

type TrinoQueryLogSchema struct {
	QueryId           string     `db:"query_id"`
	State             *string    `db:"state"`
	User              *string    `db:"user"`
	Source            *string    `db:"source"`
	Query             *string    `db:"query"`
	ResourceGroupId   *string    `db:"resource_group_id"`
	QueuedTimeMs      *int64     `db:"queued_time_ms"`
	AnalysisTimeMs    *int64     `db:"analysis_time_ms"`
	PlanningTimeMs    *int64     `db:"planning_time_ms"`
	Created           *time.Time `db:"created"`
	Started           *time.Time `db:"started"`
	LastHeartbeat     *time.Time `db:"last_heartbeat"`
	End               *time.Time `db:"end"`
	ErrorType         *string    `db:"error_type"`
	ErrorCode         *string    `db:"error_code"`
}

type trinoQueryLogIterator struct {
	rows   *sqlx.Rows
	closed bool
}

func (s *TrinoScrapper) FetchQueryLogs(ctx context.Context, from, to time.Time) (querylogs.QueryLogIterator, error) {
	// Use native QueryRows - returns sqlx.Rows iterator
	rows, err := s.Executor().QueryRows(ctx, queryLogsSql, from, to)
	if err != nil {
		return nil, err
	}

	return &trinoQueryLogIterator{
		rows: rows,
	}, nil
}

func (it *trinoQueryLogIterator) Next(ctx context.Context) (*querylogs.QueryLog, error) {
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
	var row TrinoQueryLogSchema
	if err := it.rows.StructScan(&row); err != nil {
		// Defensive: scan error, but don't crash entire ingestion
		// Caller can decide whether to continue
		return nil, err
	}

	// Convert to QueryLog
	log, err := convertTrinoRowToQueryLog(&row)
	if err != nil {
		// Defensive: conversion error
		return nil, err
	}

	return log, nil
}

func (it *trinoQueryLogIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	return it.rows.Close()
}

func convertTrinoRowToQueryLog(row *TrinoQueryLogSchema) (*querylogs.QueryLog, error) {
	// Determine status from state and error fields
	status := "UNKNOWN"
	if row.State != nil {
		state := strings.ToUpper(*row.State)
		switch state {
		case "FINISHED":
			status = "SUCCESS"
		case "FAILED":
			status = "FAILED"
		case "RUNNING", "QUEUED", "PLANNING", "STARTING":
			status = "RUNNING"
		default:
			status = state
		}
	}
	// If there's an error, mark as failed
	if row.ErrorType != nil || row.ErrorCode != nil {
		status = "FAILED"
	}

	// Use created as CreatedAt, fall back to started if not available
	var createdAt time.Time
	if row.Created != nil {
		createdAt = *row.Created
	} else if row.Started != nil {
		createdAt = *row.Started
	} else if row.End != nil {
		createdAt = *row.End
	} else {
		createdAt = time.Now() // Defensive fallback
	}

	// Build metadata with all Trino-specific fields
	metadata := make(map[string]any)
	if row.State != nil {
		metadata["state"] = *row.State
	}
	if row.Source != nil {
		metadata["source"] = *row.Source
	}
	if row.ResourceGroupId != nil {
		metadata["resource_group_id"] = *row.ResourceGroupId
	}
	if row.QueuedTimeMs != nil {
		metadata["queued_time_ms"] = *row.QueuedTimeMs
	}
	if row.AnalysisTimeMs != nil {
		metadata["analysis_time_ms"] = *row.AnalysisTimeMs
	}
	if row.PlanningTimeMs != nil {
		metadata["planning_time_ms"] = *row.PlanningTimeMs
	}
	if row.Started != nil {
		metadata["started"] = *row.Started
	}
	if row.LastHeartbeat != nil {
		metadata["last_heartbeat"] = *row.LastHeartbeat
	}
	if row.End != nil {
		metadata["end"] = *row.End
	}
	if row.ErrorType != nil {
		metadata["error_type"] = *row.ErrorType
	}
	if row.ErrorCode != nil {
		metadata["error_code"] = *row.ErrorCode
	}

	// Get query text, default to empty string
	queryText := ""
	if row.Query != nil {
		queryText = *row.Query
	}

	// Build DwhContext
	dwhContext := &querylogs.DwhContext{}
	if row.User != nil {
		dwhContext.User = *row.User
	}
	// Trino doesn't provide database/schema in system.runtime.queries

	return &querylogs.QueryLog{
		CreatedAt:                createdAt,
		QueryID:                  row.QueryId,
		SQL:                      queryText,
		DwhContext:               dwhContext,
		QueryType:                "", // Trino doesn't provide query type in this table
		Status:                   status,
		Metadata:                 metadata,
		SqlObfuscationMode:       querylogs.ObfuscationNone, // Will be set by wrapper if needed
		IsParsable:               len(queryText) > 0 && len(queryText) < 100000,
		HasCompleteNativeLineage: false, // Trino doesn't provide lineage in system.runtime.queries
		NativeLineage:            nil,
	}, nil
}
