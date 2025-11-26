package trino

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/trinodb/trino-go-client/trino"
)

//go:embed query_logs.sql
var queryLogsSql string

type TrinoQueryLogSchema struct {
	QueryId         string     `db:"query_id"`
	State           *string    `db:"state"`
	User            *string    `db:"user"`
	Source          *string    `db:"source"`
	Query           *string    `db:"query"`
	ResourceGroupId trino.NullSliceString `db:"resource_group_id"`
	QueuedTimeMs    *int64     `db:"queued_time_ms"`
	AnalysisTimeMs  *int64     `db:"analysis_time_ms"`
	PlanningTimeMs  *int64     `db:"planning_time_ms"`
	Created         *time.Time `db:"created"`
	Started         *time.Time `db:"started"`
	LastHeartbeat   *time.Time `db:"last_heartbeat"`
	End             *time.Time `db:"end"`
	ErrorType       *string    `db:"error_type"`
	ErrorCode       *string    `db:"error_code"`
}

func (s *TrinoScrapper) FetchQueryLogs(
	ctx context.Context,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
) (querylogs.QueryLogIterator, error) {
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

	// Timing information - use end as CreatedAt (when query finished/logged)
	var createdAt time.Time
	if row.End != nil {
		createdAt = *row.End
	} else if row.Started != nil {
		createdAt = *row.Started // Fallback to started if end not available
	} else if row.Created != nil {
		createdAt = *row.Created // Fallback to created if neither available
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
	if len(row.ResourceGroupId.SliceString) > 0 {
		var resourceGroupIds []string
		for _, nullString := range row.ResourceGroupId.SliceString {
			if nullString.Valid {
				resourceGroupIds = append(resourceGroupIds, nullString.String)
			}
		}
		if len(resourceGroupIds) > 0 {
			metadata["resource_group_id"] = resourceGroupIds
		}
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

	// Get query text, sanitize and apply obfuscation
	queryText := ""
	if row.Query != nil {
		queryText = strings.TrimSpace(strings.ToValidUTF8(*row.Query, ""))
		queryText = obfuscator.Obfuscate(queryText)
	}

	// Build DwhContext
	dwhContext := &querylogs.DwhContext{}
	if row.User != nil {
		dwhContext.User = *row.User
	}
	// Trino doesn't provide database/schema in system.runtime.queries

	return &querylogs.QueryLog{
		CreatedAt:                createdAt,
		StartedAt:                row.Started, // When query execution started
		FinishedAt:               row.End,     // When query execution finished
		QueryID:                  row.QueryId,
		SQL:                      queryText,
		NormalizedQueryHash:      nil, // Trino doesn't provide normalized query hash
		SqlDialect:               sqlDialect,
		DwhContext:               dwhContext,
		QueryType:                "", // Trino doesn't provide query type in this table
		Status:                   status,
		Metadata:                 querylogs.SanitizeMetadata(metadata),
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: false, // Trino doesn't provide lineage in system.runtime.queries
		NativeLineage:            nil,
	}, nil
}
