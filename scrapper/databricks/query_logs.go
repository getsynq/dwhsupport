package databricks

import (
	"context"
	"io"
	"strings"
	"time"

	servicesql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/structpb"
)

type databricksQueryLogIterator struct {
	scrapper      *DatabricksScrapper
	ctx           context.Context
	from          time.Time
	to            time.Time
	fromAdjusted  time.Time
	obfuscator    querylogs.QueryObfuscator
	sqlDialect    string
	closed        bool
	results       []servicesql.QueryInfo
	currentIdx    int
	nextPageToken string
	hasNextPage   bool
}

const (
	defaultQueryLogsStartTimeBuffer = 2 * time.Hour
	maxResultsPerPage               = 1000 // Databricks max is 1000
)

func (s *DatabricksScrapper) FetchQueryLogs(
	ctx context.Context,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
) (querylogs.QueryLogIterator, error) {
	// Validate obfuscator is provided
	if obfuscator == nil {
		return nil, errors.New("obfuscator is required")
	}

	// Get time buffer from config or use default
	startTimeBuffer := defaultQueryLogsStartTimeBuffer
	if s.conf.QueryLogsStartTimeBuffer != nil {
		startTimeBuffer = *s.conf.QueryLogsStartTimeBuffer
	}

	// Adjust 'from' time to catch queries that started earlier but finished in the target range
	// This is needed because Databricks API filters by query start time, not end time
	fromAdjusted := from.Add(-startTimeBuffer)

	// Create iterator that will fetch first page lazily
	iter := &databricksQueryLogIterator{
		scrapper:     s,
		ctx:          ctx,
		from:         from,
		to:           to,
		fromAdjusted: fromAdjusted,
		obfuscator:   obfuscator,
		sqlDialect:   s.DialectType(),
		hasNextPage:  true, // Assume there's at least one page to fetch
	}

	return iter, nil
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

		// Check if we need to fetch more results
		if it.currentIdx >= len(it.results) {
			// Try to fetch next page if available
			if it.hasNextPage {
				if err := it.fetchNextPage(ctx); err != nil {
					// Don't auto-close on error - let caller handle it
					return nil, err
				}
				// After fetching, check if we got any results
				if len(it.results) == 0 {
					// No more results
					it.Close()
					return nil, io.EOF
				}
			} else {
				// No more pages to fetch
				it.Close()
				return nil, io.EOF
			}
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
		log, err := convertDatabricksQueryInfoToQueryLog(&queryInfo, it.obfuscator, it.sqlDialect, it.scrapper.conf.WorkspaceUrl)
		if err != nil {
			// Don't auto-close on conversion error
			return nil, err
		}

		// If nil is returned, it means we should skip this item (SHOW/USE statements)
		if log == nil {
			continue
		}

		return log, nil
	}
}

// fetchNextPage fetches the next page of results from Databricks API
func (it *databricksQueryLogIterator) fetchNextPage(ctx context.Context) error {
	req := servicesql.ListQueryHistoryRequest{
		FilterBy: &servicesql.QueryFilter{
			QueryStartTimeRange: &servicesql.TimeRange{
				EndTimeMs:   it.to.UnixMilli(),
				StartTimeMs: it.fromAdjusted.UnixMilli(),
			},
			Statuses: []servicesql.QueryStatus{
				servicesql.QueryStatusFinished,
				servicesql.QueryStatusCanceled,
				servicesql.QueryStatusFailed,
			},
		},
		IncludeMetrics: true,
		MaxResults:     maxResultsPerPage,
	}

	// Add page token if this is not the first page
	if it.nextPageToken != "" {
		req.PageToken = it.nextPageToken
	}

	resp, err := it.scrapper.client.QueryHistory.List(ctx, req)
	if err != nil {
		return errors.Wrap(err, "failed to get query history page")
	}

	// Update iterator state with new page
	it.results = resp.Res
	it.currentIdx = 0
	it.nextPageToken = resp.NextPageToken
	it.hasNextPage = resp.HasNextPage

	return nil
}

func (it *databricksQueryLogIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	// Databricks SDK iterator doesn't need explicit close
	return nil
}

func convertDatabricksQueryInfoToQueryLog(
	queryInfo *servicesql.QueryInfo,
	obfuscator querylogs.QueryObfuscator,
	sqlDialect string,
	workspaceUrl string,
) (*querylogs.QueryLog, error) {
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

	// Get query text, sanitize and apply obfuscation
	// Skip for INSERT statements (can be huge) and limit size
	queryText := ""
	if queryInfo.StatementType != servicesql.QueryStatementTypeInsert && len(queryInfo.QueryText) <= 1*1024*1024 {
		queryText = strings.TrimSpace(strings.ToValidUTF8(queryInfo.QueryText, ""))
		queryText = obfuscator.Obfuscate(queryText)
	}

	// Timing information
	startedAt := time.UnixMilli(int64(queryInfo.QueryStartTimeMs))
	finishedAt := time.UnixMilli(int64(queryInfo.QueryEndTimeMs))

	// Build metadata with all Databricks-specific fields
	// Include ALL available fields, even those mapped to higher-level QueryLog fields
	metadata := map[string]*structpb.Value{
		// Fields also mapped to higher-level QueryLog fields
		"query_id":         querylogs.StringValue(queryInfo.QueryId),
		"user_name":        querylogs.StringValue(queryInfo.UserName),
		"status":           querylogs.StringValue(queryInfo.Status.String()),
		"query_start_time": querylogs.TimeValue(time.UnixMilli(int64(queryInfo.QueryStartTimeMs))),
		"query_end_time":   querylogs.TimeValue(time.UnixMilli(int64(queryInfo.QueryEndTimeMs))),
		"statement_type":   querylogs.StringValue(queryInfo.StatementType.String()),

		// Databricks-specific fields
		"endpoint_id":           querylogs.StringValue(queryInfo.EndpointId),
		"warehouse_id":          querylogs.StringValue(queryInfo.WarehouseId),
		"executed_as_user_id":   querylogs.IntValue(queryInfo.ExecutedAsUserId),
		"executed_as_user_name": querylogs.StringValue(queryInfo.ExecutedAsUserName),
		"lookup_key":            querylogs.StringValue(queryInfo.LookupKey),
		"plans_state":           querylogs.StringValue(queryInfo.PlansState.String()),
		"rows_produced":         querylogs.IntValue(queryInfo.RowsProduced),
		"spark_ui_url":          querylogs.StringValue(queryInfo.SparkUiUrl),
		"is_final":              querylogs.BoolValue(queryInfo.IsFinal),
	}

	if queryInfo.Duration != 0 {
		metadata["duration_ms"] = querylogs.IntValue(queryInfo.Duration)
	}
	if queryInfo.ExecutionEndTimeMs != 0 {
		metadata["execution_end_time"] = querylogs.TimeValue(time.UnixMilli(int64(queryInfo.ExecutionEndTimeMs)))
	}
	if queryInfo.ErrorMessage != "" {
		metadata["error_message"] = querylogs.StringValue(queryInfo.ErrorMessage)
	}

	// Add metrics if available
	if queryInfo.Metrics != nil {
		metrics := map[string]*structpb.Value{}
		if queryInfo.Metrics.CompilationTimeMs != 0 {
			metrics["compilation_time_ms"] = querylogs.IntValue(queryInfo.Metrics.CompilationTimeMs)
		}
		if queryInfo.Metrics.ExecutionTimeMs != 0 {
			metrics["execution_time_ms"] = querylogs.IntValue(queryInfo.Metrics.ExecutionTimeMs)
		}
		if queryInfo.Metrics.NetworkSentBytes != 0 {
			metrics["network_sent_bytes"] = querylogs.IntValue(queryInfo.Metrics.NetworkSentBytes)
		}
		if queryInfo.Metrics.PhotonTotalTimeMs != 0 {
			metrics["photon_total_time_ms"] = querylogs.IntValue(queryInfo.Metrics.PhotonTotalTimeMs)
		}
		if queryInfo.Metrics.ReadBytes != 0 {
			metrics["read_bytes"] = querylogs.IntValue(queryInfo.Metrics.ReadBytes)
		}
		if queryInfo.Metrics.ReadCacheBytes != 0 {
			metrics["read_cache_bytes"] = querylogs.IntValue(queryInfo.Metrics.ReadCacheBytes)
		}
		if queryInfo.Metrics.ReadFilesCount != 0 {
			metrics["read_files_count"] = querylogs.IntValue(queryInfo.Metrics.ReadFilesCount)
		}
		if queryInfo.Metrics.ReadPartitionsCount != 0 {
			metrics["read_partitions_count"] = querylogs.IntValue(queryInfo.Metrics.ReadPartitionsCount)
		}
		if queryInfo.Metrics.ReadRemoteBytes != 0 {
			metrics["read_remote_bytes"] = querylogs.IntValue(queryInfo.Metrics.ReadRemoteBytes)
		}
		if queryInfo.Metrics.ResultFetchTimeMs != 0 {
			metrics["result_fetch_time_ms"] = querylogs.IntValue(queryInfo.Metrics.ResultFetchTimeMs)
		}
		metrics["result_from_cache"] = querylogs.BoolValue(queryInfo.Metrics.ResultFromCache)
		if queryInfo.Metrics.RowsProducedCount != 0 {
			metrics["rows_produced_count"] = querylogs.IntValue(queryInfo.Metrics.RowsProducedCount)
		}
		if queryInfo.Metrics.RowsReadCount != 0 {
			metrics["rows_read_count"] = querylogs.IntValue(queryInfo.Metrics.RowsReadCount)
		}
		if queryInfo.Metrics.SpillToDiskBytes != 0 {
			metrics["spill_to_disk_bytes"] = querylogs.IntValue(queryInfo.Metrics.SpillToDiskBytes)
		}
		if queryInfo.Metrics.TaskTotalTimeMs != 0 {
			metrics["task_total_time_ms"] = querylogs.IntValue(queryInfo.Metrics.TaskTotalTimeMs)
		}
		if queryInfo.Metrics.TotalTimeMs != 0 {
			metrics["total_time_ms"] = querylogs.IntValue(queryInfo.Metrics.TotalTimeMs)
		}
		if queryInfo.Metrics.WriteRemoteBytes != 0 {
			metrics["write_remote_bytes"] = querylogs.IntValue(queryInfo.Metrics.WriteRemoteBytes)
		}
		if queryInfo.Metrics.PrunedBytes != 0 {
			metrics["pruned_bytes"] = querylogs.IntValue(queryInfo.Metrics.PrunedBytes)
		}
		if queryInfo.Metrics.PrunedFilesCount != 0 {
			metrics["pruned_files_count"] = querylogs.IntValue(queryInfo.Metrics.PrunedFilesCount)
		}

		metadata["metrics"] = querylogs.StructValue(metrics)
	}

	// Add channel info if available
	if queryInfo.ChannelUsed != nil {
		metadata["channel_used"] = querylogs.StructValue(map[string]*structpb.Value{
			"name":          querylogs.StringValue(string(queryInfo.ChannelUsed.Name)),
			"dbsql_version": querylogs.StringValue(queryInfo.ChannelUsed.DbsqlVersion),
		})
	}

	// Build DwhContext
	dwhContext := &querylogs.DwhContext{
		Instance: workspaceUrl,
		User:     queryInfo.UserName,
	}
	// Databricks doesn't provide database/schema in QueryHistory API

	return &querylogs.QueryLog{
		CreatedAt:                finishedAt,  // Use QueryEndTimeMs as CreatedAt (when query finished/logged)
		StartedAt:                &startedAt,  // When query execution started
		FinishedAt:               &finishedAt, // When query execution finished
		QueryID:                  queryInfo.QueryId,
		SQL:                      queryText,
		NormalizedQueryHash:      nil, // Databricks doesn't provide normalized query hash
		SqlDialect:               sqlDialect,
		DwhContext:               dwhContext,
		QueryType:                queryInfo.StatementType.String(),
		Status:                   status,
		Metadata:                 querylogs.NewMetadataStruct(metadata),
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: false, // Databricks doesn't provide lineage in QueryHistory
		NativeLineage:            nil,
	}, nil
}
