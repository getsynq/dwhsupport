package databricks

import (
	"context"
	"io"
	"strings"
	"time"

	servicesql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/pkg/errors"
)

type databricksQueryLogIterator struct {
	ctx        context.Context
	from       time.Time
	to         time.Time
	closed     bool
	results    []servicesql.QueryInfo
	currentIdx int
}

func (s *DatabricksScrapper) FetchQueryLogs(ctx context.Context, from, to time.Time) (querylogs.QueryLogIterator, error) {
	// Databricks API requires a slightly wider time range to account for clock skew
	fromAdjusted := from.Add(-2 * time.Hour)

	// Use the List method which returns a response with Res field
	resp, err := s.client.QueryHistory.List(ctx, servicesql.ListQueryHistoryRequest{
		FilterBy: &servicesql.QueryFilter{
			QueryStartTimeRange: &servicesql.TimeRange{
				EndTimeMs:   to.UnixMilli(),
				StartTimeMs: fromAdjusted.UnixMilli(),
			},
			Statuses: []servicesql.QueryStatus{
				servicesql.QueryStatusFinished,
				servicesql.QueryStatusCanceled,
				servicesql.QueryStatusFailed,
			},
		},
		IncludeMetrics: true,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get query history")
	}

	return &databricksQueryLogIterator{
		ctx:     ctx,
		from:    from,
		to:      to,
		results: resp.Res,
	}, nil
}

func (it *databricksQueryLogIterator) Next(ctx context.Context) (*querylogs.QueryLog, error) {
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

		// Check if we've reached the end of results
		if it.currentIdx >= len(it.results) {
			// No more results
			it.Close()
			return nil, io.EOF
		}

		// Get next query info
		queryInfo := it.results[it.currentIdx]
		it.currentIdx++

		// Filter by exact time range (API returns wider range)
		endTime := time.UnixMilli(queryInfo.QueryEndTimeMs)
		if endTime.Before(it.from) {
			// Skip this item and continue loop
			continue
		}

		// Convert to QueryLog
		log, err := convertDatabricksQueryInfoToQueryLog(&queryInfo)
		if err != nil {
			return nil, err
		}

		// If nil is returned, it means we should skip this item (SHOW/USE statements)
		if log == nil {
			continue
		}

		return log, nil
	}
}

func (it *databricksQueryLogIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	// Databricks SDK iterator doesn't need explicit close
	return nil
}

func convertDatabricksQueryInfoToQueryLog(queryInfo *servicesql.QueryInfo) (*querylogs.QueryLog, error) {
	// Skip SHOW and USE statements
	switch queryInfo.StatementType {
	case servicesql.QueryStatementTypeShow, servicesql.QueryStatementTypeUse:
		// Return nil to indicate this should be skipped
		// Caller will call Next() again
		return nil, nil
	}

	// Determine status
	status := "UNKNOWN"
	switch queryInfo.Status {
	case servicesql.QueryStatusFinished:
		status = "SUCCESS"
	case servicesql.QueryStatusCanceled:
		status = "CANCELED"
	case servicesql.QueryStatusFailed:
		status = "FAILED"
	default:
		// For queued, running, or other states, just use the string representation
		status = strings.ToUpper(string(queryInfo.Status))
	}

	// Get query text, but skip for INSERT statements (can be huge) and limit size
	queryText := queryInfo.QueryText
	if queryInfo.StatementType == servicesql.QueryStatementTypeInsert {
		queryText = ""
	}
	if len(queryText) > 1*1024*1024 {
		queryText = ""
	}

	startedAt := time.UnixMilli(int64(queryInfo.QueryStartTimeMs))

	// Build metadata with all Databricks-specific fields
	metadata := map[string]any{
		"endpoint_id":           queryInfo.EndpointId,
		"warehouse_id":          queryInfo.WarehouseId,
		"executed_as_user_id":   queryInfo.ExecutedAsUserId,
		"executed_as_user_name": queryInfo.ExecutedAsUserName,
		"lookup_key":            queryInfo.LookupKey,
		"plans_state":           queryInfo.PlansState.String(),
		"rows_produced":         queryInfo.RowsProduced,
		"spark_ui_url":          queryInfo.SparkUiUrl,
		"statement_type":        queryInfo.StatementType.String(),
		"is_final":              queryInfo.IsFinal,
	}

	if queryInfo.Duration != 0 {
		metadata["duration_ms"] = queryInfo.Duration
	}
	if queryInfo.ExecutionEndTimeMs != 0 {
		metadata["execution_end_time"] = time.UnixMilli(int64(queryInfo.ExecutionEndTimeMs))
	}
	if queryInfo.ErrorMessage != "" {
		metadata["error_message"] = queryInfo.ErrorMessage
	}

	// Add metrics if available
	if queryInfo.Metrics != nil {
		metrics := map[string]any{}
		if queryInfo.Metrics.CompilationTimeMs != 0 {
			metrics["compilation_time_ms"] = queryInfo.Metrics.CompilationTimeMs
		}
		if queryInfo.Metrics.ExecutionTimeMs != 0 {
			metrics["execution_time_ms"] = queryInfo.Metrics.ExecutionTimeMs
		}
		if queryInfo.Metrics.NetworkSentBytes != 0 {
			metrics["network_sent_bytes"] = queryInfo.Metrics.NetworkSentBytes
		}
		if queryInfo.Metrics.PhotonTotalTimeMs != 0 {
			metrics["photon_total_time_ms"] = queryInfo.Metrics.PhotonTotalTimeMs
		}
		if queryInfo.Metrics.ReadBytes != 0 {
			metrics["read_bytes"] = queryInfo.Metrics.ReadBytes
		}
		if queryInfo.Metrics.ReadCacheBytes != 0 {
			metrics["read_cache_bytes"] = queryInfo.Metrics.ReadCacheBytes
		}
		if queryInfo.Metrics.ReadFilesCount != 0 {
			metrics["read_files_count"] = queryInfo.Metrics.ReadFilesCount
		}
		if queryInfo.Metrics.ReadPartitionsCount != 0 {
			metrics["read_partitions_count"] = queryInfo.Metrics.ReadPartitionsCount
		}
		if queryInfo.Metrics.ReadRemoteBytes != 0 {
			metrics["read_remote_bytes"] = queryInfo.Metrics.ReadRemoteBytes
		}
		if queryInfo.Metrics.ResultFetchTimeMs != 0 {
			metrics["result_fetch_time_ms"] = queryInfo.Metrics.ResultFetchTimeMs
		}
		metrics["result_from_cache"] = queryInfo.Metrics.ResultFromCache
		if queryInfo.Metrics.RowsProducedCount != 0 {
			metrics["rows_produced_count"] = queryInfo.Metrics.RowsProducedCount
		}
		if queryInfo.Metrics.RowsReadCount != 0 {
			metrics["rows_read_count"] = queryInfo.Metrics.RowsReadCount
		}
		if queryInfo.Metrics.SpillToDiskBytes != 0 {
			metrics["spill_to_disk_bytes"] = queryInfo.Metrics.SpillToDiskBytes
		}
		if queryInfo.Metrics.TaskTotalTimeMs != 0 {
			metrics["task_total_time_ms"] = queryInfo.Metrics.TaskTotalTimeMs
		}
		if queryInfo.Metrics.TotalTimeMs != 0 {
			metrics["total_time_ms"] = queryInfo.Metrics.TotalTimeMs
		}
		if queryInfo.Metrics.WriteRemoteBytes != 0 {
			metrics["write_remote_bytes"] = queryInfo.Metrics.WriteRemoteBytes
		}
		if queryInfo.Metrics.PrunedBytes != 0 {
			metrics["pruned_bytes"] = queryInfo.Metrics.PrunedBytes
		}
		if queryInfo.Metrics.PrunedFilesCount != 0 {
			metrics["pruned_files_count"] = queryInfo.Metrics.PrunedFilesCount
		}

		metadata["metrics"] = metrics
	}

	// Add channel info if available
	if queryInfo.ChannelUsed != nil {
		metadata["channel_used"] = map[string]any{
			"name":          string(queryInfo.ChannelUsed.Name),
			"dbsql_version": queryInfo.ChannelUsed.DbsqlVersion,
		}
	}

	// Build DwhContext
	dwhContext := &querylogs.DwhContext{
		User: queryInfo.UserName,
	}
	// Databricks doesn't provide database/schema in QueryHistory API

	return &querylogs.QueryLog{
		CreatedAt:                startedAt,
		QueryID:                  queryInfo.QueryId,
		SQL:                      queryText,
		DwhContext:               dwhContext,
		QueryType:                queryInfo.StatementType.String(),
		Status:                   status,
		Metadata:                 metadata,
		SqlObfuscationMode:       querylogs.ObfuscationNone,
		IsParsable:               len(queryText) > 0 && len(queryText) < 100000,
		HasCompleteNativeLineage: false, // Databricks doesn't provide lineage in QueryHistory
		NativeLineage:            nil,
	}, nil
}
