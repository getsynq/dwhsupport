package trino

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
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

func (s *TrinoScrapper) FetchQueryLogs(ctx context.Context, from, to time.Time, obfuscator querylogs.QueryObfuscator) (querylogs.QueryLogIterator, error) {
	// Validate obfuscator is provided
	if obfuscator == nil {
		return nil, fmt.Errorf("obfuscator is required")
	}

	// Use native QueryRows - returns sqlx.Rows iterator
	rows, err := s.Executor().QueryRows(ctx, queryLogsSql, from, to)
	if err != nil {
		return nil, err
	}

	return querylogs.NewSqlxRowsIterator[TrinoQueryLogSchema](rows, obfuscator, s.DialectType(), convertTrinoRowToQueryLog), nil
}

func convertTrinoRowToQueryLog(row *TrinoQueryLogSchema, obfuscator querylogs.QueryObfuscator, sqlDialect string) (*querylogs.QueryLog, error) {
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

	// Get query text and apply obfuscation
	queryText := ""
	if row.Query != nil {
		queryText = obfuscator.Obfuscate(*row.Query)
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
		SqlDialect:               sqlDialect,
		DwhContext:               dwhContext,
		QueryType:                "", // Trino doesn't provide query type in this table
		Status:                   status,
		Metadata:                 metadata,
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: false, // Trino doesn't provide lineage in system.runtime.queries
		NativeLineage:            nil,
	}, nil
}
