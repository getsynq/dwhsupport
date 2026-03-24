package mssql

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/querylogs"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed query_logs.sql
var queryLogsSql string

// MSSQLQueryLogSchema maps columns from the Query Store runtime stats query.
// Query Store aggregates execution statistics per plan per time interval,
// so each row represents a plan's stats during one interval (not a single execution).
type MSSQLQueryLogSchema struct {
	Database             string     `db:"database"`
	QueryId              int64      `db:"query_id"`
	QueryHash            *int64     `db:"query_hash"`
	QuerySqlText         *string    `db:"query_sql_text"`
	ExecutionType        int        `db:"execution_type"`
	CountExecutions      int64      `db:"count_executions"`
	FirstExecutionTime   *time.Time `db:"first_execution_time"`
	LastExecutionTime    *time.Time `db:"last_execution_time"`
	AvgDurationUs        *float64   `db:"avg_duration_us"`
	LastDurationUs       *float64   `db:"last_duration_us"`
	AvgCpuTimeUs         *float64   `db:"avg_cpu_time_us"`
	AvgLogicalIoReads    *float64   `db:"avg_logical_io_reads"`
	AvgLogicalIoWrites   *float64   `db:"avg_logical_io_writes"`
	AvgPhysicalIoReads   *float64   `db:"avg_physical_io_reads"`
	AvgRowcount          *float64   `db:"avg_rowcount"`
	AvgQueryMaxUsedMemKb *float64   `db:"avg_query_max_used_memory_kb"`
	PlanId               int64      `db:"plan_id"`
	IntervalStart        *time.Time `db:"interval_start"`
	IntervalEnd          *time.Time `db:"interval_end"`
}

var _ querylogs.QueryLogsProvider = &MSSQLScrapper{}

func (s *MSSQLScrapper) FetchQueryLogs(
	ctx context.Context,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
) (querylogs.QueryLogIterator, error) {
	if obfuscator == nil {
		return nil, fmt.Errorf("obfuscator is required")
	}

	rows, err := s.executor.QueryRows(ctx, queryLogsSql, from, to)
	if err != nil {
		return nil, err
	}

	host := s.conf.Host
	return querylogs.NewSqlxRowsIterator[MSSQLQueryLogSchema](
		rows,
		obfuscator,
		s.DialectType(),
		func(row *MSSQLQueryLogSchema, obfuscator querylogs.QueryObfuscator, sqlDialect string) (*querylogs.QueryLog, error) {
			return convertMSSQLRowToQueryLog(row, obfuscator, sqlDialect, host)
		},
	), nil
}

// mssqlExecutionTypeToStatus maps Query Store execution_type to a status string.
// 0 = Regular (successful), 3 = Aborted, 4 = Exception.
func mssqlExecutionTypeToStatus(execType int) string {
	switch execType {
	case 0:
		return "SUCCESS"
	case 3:
		return "ABORTED"
	case 4:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

func convertMSSQLRowToQueryLog(
	row *MSSQLQueryLogSchema,
	obfuscator querylogs.QueryObfuscator,
	sqlDialect string,
	host string,
) (*querylogs.QueryLog, error) {
	status := mssqlExecutionTypeToStatus(row.ExecutionType)

	var createdAt time.Time
	if row.LastExecutionTime != nil {
		createdAt = *row.LastExecutionTime
	} else if row.IntervalEnd != nil {
		createdAt = *row.IntervalEnd
	} else {
		createdAt = time.Now()
	}

	queryText := ""
	if row.QuerySqlText != nil {
		queryText = strings.TrimSpace(strings.ToValidUTF8(*row.QuerySqlText, ""))
		queryText = obfuscator.Obfuscate(queryText)
	}

	var normalizedHash *string
	if row.QueryHash != nil {
		h := fmt.Sprintf("%d", *row.QueryHash)
		normalizedHash = &h
	}

	metadata := map[string]*structpb.Value{
		"query_id":                     querylogs.IntValue(row.QueryId),
		"query_hash":                   querylogs.IntPtrValue(row.QueryHash),
		"execution_type":               querylogs.IntValue(int64(row.ExecutionType)),
		"count_executions":             querylogs.IntValue(row.CountExecutions),
		"first_execution_time":         querylogs.TimePtrValue(row.FirstExecutionTime),
		"last_execution_time":          querylogs.TimePtrValue(row.LastExecutionTime),
		"avg_duration_us":              querylogs.FloatPtrValue(row.AvgDurationUs),
		"last_duration_us":             querylogs.FloatPtrValue(row.LastDurationUs),
		"avg_cpu_time_us":              querylogs.FloatPtrValue(row.AvgCpuTimeUs),
		"avg_logical_io_reads":         querylogs.FloatPtrValue(row.AvgLogicalIoReads),
		"avg_logical_io_writes":        querylogs.FloatPtrValue(row.AvgLogicalIoWrites),
		"avg_physical_io_reads":        querylogs.FloatPtrValue(row.AvgPhysicalIoReads),
		"avg_rowcount":                 querylogs.FloatPtrValue(row.AvgRowcount),
		"avg_query_max_used_memory_kb": querylogs.FloatPtrValue(row.AvgQueryMaxUsedMemKb),
		"plan_id":                      querylogs.IntValue(row.PlanId),
		"interval_start":               querylogs.TimePtrValue(row.IntervalStart),
		"interval_end":                 querylogs.TimePtrValue(row.IntervalEnd),
	}

	return &querylogs.QueryLog{
		CreatedAt:                createdAt,
		StartedAt:                row.FirstExecutionTime,
		FinishedAt:               row.LastExecutionTime,
		QueryID:                  fmt.Sprintf("%d", row.QueryId),
		SQL:                      queryText,
		NormalizedQueryHash:      normalizedHash,
		SqlDialect:               sqlDialect,
		DwhContext:               &querylogs.DwhContext{Instance: host, Database: row.Database},
		QueryType:                "",
		Status:                   status,
		Metadata:                 querylogs.NewMetadataStruct(metadata),
		SqlObfuscationMode:       obfuscator.Mode(),
		HasCompleteNativeLineage: false,
		NativeLineage:            nil,
	}, nil
}
