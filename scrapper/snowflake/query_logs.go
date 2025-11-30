package snowflake

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"github.com/getsynq/dwhsupport/sqldialect"
	"google.golang.org/protobuf/types/known/structpb"
)

// SnowflakeQueryLogSchema represents Snowflake's ACCOUNT_USAGE.QUERY_HISTORY schema
// Source: https://docs.snowflake.com/en/sql-reference/account-usage/query_history
type SnowflakeQueryLogSchema struct {
	QueryID                                string    `db:"QUERY_ID"`
	QueryText                              string    `db:"QUERY_TEXT"`
	DatabaseID                             *int64    `db:"DATABASE_ID"`
	DatabaseName                           *string   `db:"DATABASE_NAME"`
	SchemaID                               *int64    `db:"SCHEMA_ID"`
	SchemaName                             *string   `db:"SCHEMA_NAME"`
	QueryType                              string    `db:"QUERY_TYPE"`
	SessionID                              int64     `db:"SESSION_ID"`
	UserName                               string    `db:"USER_NAME"`
	RoleName                               *string   `db:"ROLE_NAME"`
	WarehouseID                            *int64    `db:"WAREHOUSE_ID"`
	WarehouseName                          *string   `db:"WAREHOUSE_NAME"`
	WarehouseSize                          *string   `db:"WAREHOUSE_SIZE"`
	WarehouseType                          *string   `db:"WAREHOUSE_TYPE"`
	ClusterNumber                          *int32    `db:"CLUSTER_NUMBER"`
	QueryTag                               string    `db:"QUERY_TAG"`
	ExecutionStatus                        string    `db:"EXECUTION_STATUS"`
	ErrorCode                              *int32    `db:"ERROR_CODE"`
	ErrorMessage                           *string   `db:"ERROR_MESSAGE"`
	StartTime                              time.Time `db:"START_TIME"`
	EndTime                                time.Time `db:"END_TIME"`
	TotalElapsedTime                       int64     `db:"TOTAL_ELAPSED_TIME"`
	BytesScanned                           int64     `db:"BYTES_SCANNED"`
	PercentageScannedFromCache             float64   `db:"PERCENTAGE_SCANNED_FROM_CACHE"`
	BytesWritten                           int64     `db:"BYTES_WRITTEN"`
	BytesWrittenToResult                   int64     `db:"BYTES_WRITTEN_TO_RESULT"`
	BytesReadFromResult                    int64     `db:"BYTES_READ_FROM_RESULT"`
	RowsProduced                           *int64    `db:"ROWS_PRODUCED"`
	RowsInserted                           int64     `db:"ROWS_INSERTED"`
	RowsUpdated                            int64     `db:"ROWS_UPDATED"`
	RowsDeleted                            int64     `db:"ROWS_DELETED"`
	RowsUnloaded                           int64     `db:"ROWS_UNLOADED"`
	BytesDeleted                           int64     `db:"BYTES_DELETED"`
	PartitionsScanned                      int64     `db:"PARTITIONS_SCANNED"`
	PartitionsTotal                        int64     `db:"PARTITIONS_TOTAL"`
	BytesSpilledToLocalStorage             int64     `db:"BYTES_SPILLED_TO_LOCAL_STORAGE"`
	BytesSpilledToRemoteStorage            int64     `db:"BYTES_SPILLED_TO_REMOTE_STORAGE"`
	BytesSentOverTheNetwork                int64     `db:"BYTES_SENT_OVER_THE_NETWORK"`
	CompilationTime                        int64     `db:"COMPILATION_TIME"`
	ExecutionTime                          int64     `db:"EXECUTION_TIME"`
	QueuedProvisioningTime                 int64     `db:"QUEUED_PROVISIONING_TIME"`
	QueuedRepairTime                       int64     `db:"QUEUED_REPAIR_TIME"`
	QueuedOverloadTime                     int64     `db:"QUEUED_OVERLOAD_TIME"`
	TransactionBlockedTime                 int64     `db:"TRANSACTION_BLOCKED_TIME"`
	OutboundDataTransferCloud              *string   `db:"OUTBOUND_DATA_TRANSFER_CLOUD"`
	OutboundDataTransferRegion             *string   `db:"OUTBOUND_DATA_TRANSFER_REGION"`
	OutboundDataTransferBytes              *int64    `db:"OUTBOUND_DATA_TRANSFER_BYTES"`
	InboundDataTransferCloud               *string   `db:"INBOUND_DATA_TRANSFER_CLOUD"`
	InboundDataTransferRegion              *string   `db:"INBOUND_DATA_TRANSFER_REGION"`
	InboundDataTransferBytes               *int64    `db:"INBOUND_DATA_TRANSFER_BYTES"`
	ListExternalFilesTime                  int64     `db:"LIST_EXTERNAL_FILES_TIME"`
	CreditsUsedCloudServices               float64   `db:"CREDITS_USED_CLOUD_SERVICES"`
	ReleaseVersion                         string    `db:"RELEASE_VERSION"`
	ExternalFunctionTotalInvocations       int64     `db:"EXTERNAL_FUNCTION_TOTAL_INVOCATIONS"`
	ExternalFunctionTotalSentRows          int64     `db:"EXTERNAL_FUNCTION_TOTAL_SENT_ROWS"`
	ExternalFunctionTotalReceivedRows      int64     `db:"EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS"`
	ExternalFunctionTotalSentBytes         int64     `db:"EXTERNAL_FUNCTION_TOTAL_SENT_BYTES"`
	ExternalFunctionTotalReceivedBytes     int64     `db:"EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES"`
	QueryLoadPercent                       *int64    `db:"QUERY_LOAD_PERCENT"`
	IsClientGeneratedStatement             bool      `db:"IS_CLIENT_GENERATED_STATEMENT"`
	QueryAccelerationBytesScanned          int64     `db:"QUERY_ACCELERATION_BYTES_SCANNED"`
	QueryAccelerationPartitionsScanned     int64     `db:"QUERY_ACCELERATION_PARTITIONS_SCANNED"`
	QueryAccelerationUpperLimitScaleFactor int64     `db:"QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR"`
	ChildQueriesWaitTime                   int64     `db:"CHILD_QUERIES_WAIT_TIME"`
	TransactionID                          int64     `db:"TRANSACTION_ID"`
	RoleType                               *string   `db:"ROLE_TYPE"`
	QueryHash                              *string   `db:"QUERY_HASH"`
	QueryHashVersion                       *int64    `db:"QUERY_HASH_VERSION"`
	QueryParameterizedHash                 *string   `db:"QUERY_PARAMETERIZED_HASH"`
	QueryParameterizedHashVersion          *int64    `db:"QUERY_PARAMETERIZED_HASH_VERSION"`
}

var queryHistoryMandatoryColumns = []string{
	"QUERY_ID",
	"QUERY_TEXT",
	"END_TIME",      // Required for time-based filtering
	"DATABASE_NAME", // Required for database filtering
}

func (s *SnowflakeScrapper) FetchQueryLogs(
	ctx context.Context,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
) (querylogs.QueryLogIterator, error) {
	// Validate obfuscator is provided
	if obfuscator == nil {
		return nil, fmt.Errorf("obfuscator is required")
	}

	// Detect available columns (some customers may have different columns)
	columnsToSelect, err := s.queryLogsColumns(ctx)
	if err != nil {
		return nil, err
	}

	// Build SQL query
	sqlQuery, err := s.buildQueryLogsSql(ctx, from, to, columnsToSelect)
	if err != nil {
		return nil, err
	}

	// Use native QueryRows - returns sqlx.Rows iterator
	rows, err := s.Executor().QueryRows(ctx, sqlQuery)
	if err != nil {
		return nil, err
	}

	account := s.conf.Account
	return querylogs.NewSqlxRowsIterator[SnowflakeQueryLogSchema](
		rows,
		obfuscator,
		s.DialectType(),
		func(row *SnowflakeQueryLogSchema, obfuscator querylogs.QueryObfuscator, sqlDialect string) (*querylogs.QueryLog, error) {
			return convertSnowflakeRowToQueryLog(row, obfuscator, sqlDialect, account)
		},
	), nil
}

// findTableColumns queries the table to find available columns
func (s *SnowflakeScrapper) findTableColumns(ctx context.Context, tableName string) ([]string, error) {
	rows, err := s.Executor().QueryRows(ctx, fmt.Sprintf("DESCRIBE TABLE %s", tableName))
	if err != nil {
		return nil, fmt.Errorf("could not get columns for table %s: %w", tableName, err)
	}
	defer rows.Close()

	var columnNames []string
	for rows.Next() {
		m := map[string]interface{}{}
		if err := rows.MapScan(m); err != nil {
			return nil, err
		}
		if name, found := m["name"]; found {
			if nameStr, ok := name.(string); ok && len(nameStr) > 0 {
				columnNames = append(columnNames, nameStr)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columnNames, nil
}

// queryLogsColumns determines which columns are available in QUERY_HISTORY
// Some customers may have different columns available depending on their Snowflake version
func (s *SnowflakeScrapper) queryLogsColumns(ctx context.Context) ([]string, error) {
	// Get all expected columns from struct tags
	expectedColumns := getDBFields(&SnowflakeQueryLogSchema{})

	// Determine account usage database
	accountUsageDb := "SNOWFLAKE"
	if s.conf.AccountUsageDb != nil && len(*s.conf.AccountUsageDb) > 0 {
		accountUsageDb = *s.conf.AccountUsageDb
	}

	// Query the actual table to see what columns exist
	tableName := fmt.Sprintf("%s.ACCOUNT_USAGE.QUERY_HISTORY", accountUsageDb)
	columnsFound, err := s.findTableColumns(ctx, tableName)
	if err != nil {
		return nil, err
	}

	// Build a set of available columns for quick lookup
	availableColumns := make(map[string]bool)
	for _, col := range columnsFound {
		availableColumns[col] = true
	}

	// Filter expected columns to only those available
	var columnsToUse []string
	for _, expectedColumn := range expectedColumns {
		if availableColumns[expectedColumn] {
			columnsToUse = append(columnsToUse, expectedColumn)
		}
	}

	// Validate that mandatory columns are present in the final list
	for _, mandatoryColumn := range queryHistoryMandatoryColumns {
		found := false
		for _, col := range columnsToUse {
			if col == mandatoryColumn {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf(
				"table %s is not compatible with ACCOUNT_USAGE.QUERY_HISTORY - missing mandatory column %s",
				tableName,
				mandatoryColumn,
			)
		}
	}

	return columnsToUse, nil
}

// getDBFields extracts db tag values from struct fields
func getDBFields(v interface{}) []string {
	var fields []string
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("db")
		if tag != "" && tag != "-" {
			fields = append(fields, tag)
		}
	}

	return fields
}

func (s *SnowflakeScrapper) buildQueryLogsSql(ctx context.Context, from time.Time, to time.Time, columnsToSelect []string) (string, error) {
	cols := make([]sqldialect.Expr, len(columnsToSelect))
	for i, col := range columnsToSelect {
		cols[i] = sqldialect.Identifier(col)
	}

	conditions := []sqldialect.CondExpr{
		sqldialect.Between(
			sqldialect.TimeCol("END_TIME"),
			sqldialect.Fn("to_timestamp_ltz", sqldialect.String(from.Format(time.RFC3339))),
			sqldialect.Fn("to_timestamp_ltz", sqldialect.String(to.Format(time.RFC3339))),
		),
	}

	// Determine which databases to filter by
	databasesForQueryLogs := s.conf.Databases
	if len(s.conf.QueryLogsDatabases) > 0 {
		databasesForQueryLogs = s.conf.QueryLogsDatabases
	}

	// Add database filtering if configured
	if len(databasesForQueryLogs) > 0 {
		// Create database match conditions
		var dbMatchConditions []sqldialect.CondExpr

		// Condition 1: Direct DATABASE_NAME match
		dbExprs := make([]sqldialect.Expr, len(databasesForQueryLogs))
		for i, db := range databasesForQueryLogs {
			dbExprs[i] = sqldialect.String(db)
		}
		dbMatchConditions = append(dbMatchConditions,
			sqldialect.In(sqldialect.TextCol("DATABASE_NAME"), dbExprs...),
		)

		// Condition 2: REGEXP_LIKE on QUERY_TEXT to catch queries that reference the database
		// This handles both: NYC_TAXI.PUBLIC and "NYC_TAXI"."PUBLIC"
		// Match database name with optional quotes/whitespace before the dot
		// Note: Snowflake doesn't support \s, so we use explicit character class
		// Single quotes in SQL strings must be escaped by doubling them
		regexpString := fmt.Sprintf(`.*[\"' \\t\\n\\r]?(%s)[\"' \\t\\n\\r]*[.].*`, strings.Join(databasesForQueryLogs, "|"))
		dbMatchConditions = append(dbMatchConditions,
			sqldialect.FnCond("REGEXP_LIKE",
				sqldialect.TextCol("QUERY_TEXT"),
				sqldialect.String(regexpString),
				sqldialect.String("is"),
			),
		)

		// Combine database conditions with OR
		conditions = append(conditions, sqldialect.Or(dbMatchConditions...))
	}

	accountUsageDb := "SNOWFLAKE"
	if s.conf.AccountUsageDb != nil && len(*s.conf.AccountUsageDb) > 0 {
		accountUsageDb = *s.conf.AccountUsageDb
	}

	return sqldialect.
		NewSelect().
		Cols(cols...).
		From(
			sqldialect.TableFqn(accountUsageDb, "ACCOUNT_USAGE", "QUERY_HISTORY"),
		).
		Where(
			conditions...,
		).
		ToSql(sqldialect.NewSnowflakeDialect())
}

func convertSnowflakeRowToQueryLog(
	row *SnowflakeQueryLogSchema,
	obfuscator querylogs.QueryObfuscator,
	sqlDialect string,
	account string,
) (*querylogs.QueryLog, error) {
	// Skip running queries (following original implementation)
	switch strings.ToLower(row.ExecutionStatus) {
	case "resuming_warehouse", "running", "queued", "blocked":
		// Return nil to indicate this query should be skipped
		return nil, nil
	}

	// Determine status from execution_status
	status := "UNKNOWN"
	switch strings.ToLower(row.ExecutionStatus) {
	case "success":
		status = "SUCCESS"
	case "failed_with_error", "fail":
		status = "FAILED"
	case "failed_with_incident", "incident":
		status = "CRITICAL"
	default:
		status = strings.ToUpper(row.ExecutionStatus)
	}

	// Build metadata with all Snowflake-specific fields
	// Include ALL available fields, even those mapped to higher-level QueryLog fields
	metadata := map[string]*structpb.Value{
		// Fields also mapped to higher-level QueryLog fields
		"query_id":       querylogs.StringValue(row.QueryID),
		"query_type":     querylogs.StringValue(row.QueryType),
		"user_name":      querylogs.StringValue(row.UserName),
		"database_name":  querylogs.StringPtrValue(row.DatabaseName),
		"schema_name":    querylogs.StringPtrValue(row.SchemaName),
		"warehouse_name": querylogs.StringPtrValue(row.WarehouseName),
		"role_name":      querylogs.StringPtrValue(row.RoleName),
		"start_time":     querylogs.TimeValue(row.StartTime),
		"end_time":       querylogs.TimeValue(row.EndTime),

		// Snowflake-specific fields
		"database_id":                                 querylogs.IntPtrValue(row.DatabaseID),
		"schema_id":                                   querylogs.IntPtrValue(row.SchemaID),
		"session_id":                                  querylogs.IntValue(row.SessionID),
		"warehouse_id":                                querylogs.IntPtrValue(row.WarehouseID),
		"warehouse_size":                              querylogs.StringPtrValue(row.WarehouseSize),
		"warehouse_type":                              querylogs.StringPtrValue(row.WarehouseType),
		"cluster_number":                              querylogs.Int32PtrValue(row.ClusterNumber),
		"query_tag":                                   querylogs.StringValue(row.QueryTag),
		"execution_status":                            querylogs.StringValue(row.ExecutionStatus),
		"error_code":                                  querylogs.Int32PtrValue(row.ErrorCode),
		"error_message":                               querylogs.StringPtrValue(row.ErrorMessage),
		"total_elapsed_time":                          querylogs.IntValue(row.TotalElapsedTime),
		"bytes_scanned":                               querylogs.IntValue(row.BytesScanned),
		"percentage_scanned_from_cache":               querylogs.FloatValue(row.PercentageScannedFromCache),
		"bytes_written":                               querylogs.IntValue(row.BytesWritten),
		"bytes_written_to_result":                     querylogs.IntValue(row.BytesWrittenToResult),
		"bytes_read_from_result":                      querylogs.IntValue(row.BytesReadFromResult),
		"rows_produced":                               querylogs.IntPtrValue(row.RowsProduced),
		"rows_inserted":                               querylogs.IntValue(row.RowsInserted),
		"rows_updated":                                querylogs.IntValue(row.RowsUpdated),
		"rows_deleted":                                querylogs.IntValue(row.RowsDeleted),
		"rows_unloaded":                               querylogs.IntValue(row.RowsUnloaded),
		"bytes_deleted":                               querylogs.IntValue(row.BytesDeleted),
		"partitions_scanned":                          querylogs.IntValue(row.PartitionsScanned),
		"partitions_total":                            querylogs.IntValue(row.PartitionsTotal),
		"bytes_spilled_to_local_storage":              querylogs.IntValue(row.BytesSpilledToLocalStorage),
		"bytes_spilled_to_remote_storage":             querylogs.IntValue(row.BytesSpilledToRemoteStorage),
		"bytes_sent_over_the_network":                 querylogs.IntValue(row.BytesSentOverTheNetwork),
		"compilation_time":                            querylogs.IntValue(row.CompilationTime),
		"execution_time":                              querylogs.IntValue(row.ExecutionTime),
		"queued_provisioning_time":                    querylogs.IntValue(row.QueuedProvisioningTime),
		"queued_repair_time":                          querylogs.IntValue(row.QueuedRepairTime),
		"queued_overload_time":                        querylogs.IntValue(row.QueuedOverloadTime),
		"transaction_blocked_time":                    querylogs.IntValue(row.TransactionBlockedTime),
		"outbound_data_transfer_cloud":                querylogs.StringPtrValue(row.OutboundDataTransferCloud),
		"outbound_data_transfer_region":               querylogs.StringPtrValue(row.OutboundDataTransferRegion),
		"outbound_data_transfer_bytes":                querylogs.IntPtrValue(row.OutboundDataTransferBytes),
		"inbound_data_transfer_cloud":                 querylogs.StringPtrValue(row.InboundDataTransferCloud),
		"inbound_data_transfer_region":                querylogs.StringPtrValue(row.InboundDataTransferRegion),
		"inbound_data_transfer_bytes":                 querylogs.IntPtrValue(row.InboundDataTransferBytes),
		"list_external_files_time":                    querylogs.IntValue(row.ListExternalFilesTime),
		"credits_used_cloud_services":                 querylogs.FloatValue(row.CreditsUsedCloudServices),
		"release_version":                             querylogs.StringValue(row.ReleaseVersion),
		"external_function_total_invocations":         querylogs.IntValue(row.ExternalFunctionTotalInvocations),
		"external_function_total_sent_rows":           querylogs.IntValue(row.ExternalFunctionTotalSentRows),
		"external_function_total_received_rows":       querylogs.IntValue(row.ExternalFunctionTotalReceivedRows),
		"external_function_total_sent_bytes":          querylogs.IntValue(row.ExternalFunctionTotalSentBytes),
		"external_function_total_received_bytes":      querylogs.IntValue(row.ExternalFunctionTotalReceivedBytes),
		"query_load_percent":                          querylogs.IntPtrValue(row.QueryLoadPercent),
		"is_client_generated_statement":               querylogs.BoolValue(row.IsClientGeneratedStatement),
		"query_acceleration_bytes_scanned":            querylogs.IntValue(row.QueryAccelerationBytesScanned),
		"query_acceleration_partitions_scanned":       querylogs.IntValue(row.QueryAccelerationPartitionsScanned),
		"query_acceleration_upper_limit_scale_factor": querylogs.IntValue(row.QueryAccelerationUpperLimitScaleFactor),
		"child_queries_wait_time":                     querylogs.IntValue(row.ChildQueriesWaitTime),
		"transaction_id":                              querylogs.IntValue(row.TransactionID),
		"role_type":                                   querylogs.StringPtrValue(row.RoleType),
		"query_hash":                                  querylogs.StringPtrValue(row.QueryHash),
		"query_hash_version":                          querylogs.IntPtrValue(row.QueryHashVersion),
		"query_parameterized_hash":                    querylogs.StringPtrValue(row.QueryParameterizedHash),
		"query_parameterized_hash_version":            querylogs.IntPtrValue(row.QueryParameterizedHashVersion),
	}

	// Build DwhContext
	dwhContext := &querylogs.DwhContext{
		Instance: account,
		User:     row.UserName,
	}
	if row.DatabaseName != nil && *row.DatabaseName != "" {
		dwhContext.Database = *row.DatabaseName
	}
	if row.SchemaName != nil && *row.SchemaName != "" {
		dwhContext.Schema = *row.SchemaName
	}
	if row.WarehouseName != nil && *row.WarehouseName != "" {
		dwhContext.Warehouse = *row.WarehouseName
	}
	if row.RoleName != nil && *row.RoleName != "" {
		dwhContext.Role = *row.RoleName
	}

	// Sanitize and apply obfuscation to query text
	// Replace $$$$ with '' (Snowflake-specific escaping) and clean up whitespace/invalid UTF-8
	queryText := strings.ReplaceAll(strings.TrimSpace(strings.ToValidUTF8(row.QueryText, "")), "$$$$", "''")
	queryText = obfuscator.Obfuscate(queryText)

	// Timing information
	startedAt := row.StartTime
	finishedAt := row.EndTime

	return &querylogs.QueryLog{
		CreatedAt:                row.EndTime, // Use EndTime as CreatedAt (when query finished)
		StartedAt:                &startedAt,  // When query execution started
		FinishedAt:               &finishedAt, // When query execution finished
		QueryID:                  row.QueryID,
		SQL:                      queryText,
		NormalizedQueryHash:      row.QueryParameterizedHash, // Native Snowflake parameterized query hash
		SqlDialect:               sqlDialect,
		DwhContext:               dwhContext,
		QueryType:                row.QueryType,
		Status:                   status,
		Metadata:                 querylogs.NewMetadataStruct(metadata),
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: false, // Snowflake doesn't provide lineage in QUERY_HISTORY
		NativeLineage:            nil,
	}, nil
}
