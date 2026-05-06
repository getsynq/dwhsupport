package athena

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsathena "github.com/aws/aws-sdk-go-v2/service/athena"
	awstypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/getsynq/dwhsupport/querylogs"
)

// FetchQueryLogs streams the workgroup's query history from the Athena
// management APIs (athena:ListQueryExecutions + BatchGetQueryExecution).
//
// These calls do NOT bill per data scanned — they're metadata operations.
// Page size is 50 (the AWS hard cap for both ListQueryExecutions and
// BatchGetQueryExecution); the iterator stops paging once it sees an
// execution older than `from`. AWS returns executions newest-first.
//
// Athena retains query history for 45 days by default per workgroup; older
// `from` values silently truncate to that window.
func (e *AthenaScrapper) FetchQueryLogs(
	ctx context.Context,
	from, to time.Time,
	obfuscator querylogs.QueryObfuscator,
) (querylogs.QueryLogIterator, error) {
	if obfuscator == nil {
		return nil, fmt.Errorf("athena: obfuscator is required")
	}
	return &athenaQueryLogIterator{
		client:     e.executor.AthenaClient(),
		workgroup:  e.executor.Workgroup(),
		catalog:    e.executor.Catalog(),
		instance:   e.executor.Instance(),
		dialect:    e.DialectType(),
		from:       from,
		to:         to,
		obfuscator: obfuscator,
	}, nil
}

const athenaListPageSize = 50 // AWS hard cap for both List and BatchGet

type athenaQueryLogIterator struct {
	client     *awsathena.Client
	workgroup  string
	catalog    string
	instance   string
	dialect    string
	from       time.Time
	to         time.Time
	obfuscator querylogs.QueryObfuscator

	nextToken *string // ListQueryExecutions pagination
	buf       []awstypes.QueryExecution
	exhausted bool
	closed    bool
}

func (it *athenaQueryLogIterator) Next(ctx context.Context) (*querylogs.QueryLog, error) {
	for {
		if it.closed {
			return nil, io.EOF
		}
		if len(it.buf) > 0 {
			qe := it.buf[0]
			it.buf = it.buf[1:]
			ql, keep := it.toQueryLog(qe)
			if !keep {
				continue
			}
			return ql, nil
		}
		if it.exhausted {
			_ = it.Close()
			return nil, io.EOF
		}
		if err := it.refill(ctx); err != nil {
			_ = it.Close()
			return nil, err
		}
	}
}

func (it *athenaQueryLogIterator) Close() error {
	it.closed = true
	it.buf = nil
	return nil
}

// refill fetches the next page of QueryExecutionIds from ListQueryExecutions
// and resolves them via BatchGetQueryExecution. Stops paging once every ID
// returned in this batch lands before `from`.
func (it *athenaQueryLogIterator) refill(ctx context.Context) error {
	listOut, err := it.client.ListQueryExecutions(ctx, &awsathena.ListQueryExecutionsInput{
		WorkGroup:  aws.String(it.workgroup),
		MaxResults: aws.Int32(athenaListPageSize),
		NextToken:  it.nextToken,
	})
	if err != nil {
		return fmt.Errorf("athena: ListQueryExecutions: %w", err)
	}
	it.nextToken = listOut.NextToken
	if listOut.NextToken == nil || *listOut.NextToken == "" {
		it.exhausted = true
	}
	if len(listOut.QueryExecutionIds) == 0 {
		it.exhausted = true
		return nil
	}
	batchOut, err := it.client.BatchGetQueryExecution(ctx, &awsathena.BatchGetQueryExecutionInput{
		QueryExecutionIds: listOut.QueryExecutionIds,
	})
	if err != nil {
		return fmt.Errorf("athena: BatchGetQueryExecution: %w", err)
	}
	// AWS does not guarantee BatchGet preserves the List order; but the executions
	// it returns are still globally newest-first within the workgroup. Track the
	// oldest timestamp we observed so we know when we've paged before `from`.
	var oldest time.Time
	for _, qe := range batchOut.QueryExecutions {
		if qe.Status != nil && qe.Status.SubmissionDateTime != nil {
			ts := *qe.Status.SubmissionDateTime
			if oldest.IsZero() || ts.Before(oldest) {
				oldest = ts
			}
		}
		it.buf = append(it.buf, qe)
	}
	if !oldest.IsZero() && oldest.Before(it.from) {
		// Whole next page would be < from — no point paging further.
		it.exhausted = true
	}
	return nil
}

func (it *athenaQueryLogIterator) toQueryLog(qe awstypes.QueryExecution) (*querylogs.QueryLog, bool) {
	if qe.Status == nil || qe.Status.SubmissionDateTime == nil {
		return nil, false
	}
	submitted := *qe.Status.SubmissionDateTime
	if submitted.Before(it.from) || !submitted.Before(it.to) {
		return nil, false
	}

	createdAt := submitted
	if qe.Status.CompletionDateTime != nil {
		createdAt = *qe.Status.CompletionDateTime
	}

	var startedAt, finishedAt *time.Time
	startedAt = &submitted
	if qe.Status.CompletionDateTime != nil {
		finishedAt = qe.Status.CompletionDateTime
	}

	sqlText := aws.ToString(qe.Query)
	sqlText = it.obfuscator.Obfuscate(sqlText)

	dwhCtx := &querylogs.DwhContext{
		Instance: it.instance,
	}
	if qe.QueryExecutionContext != nil {
		// Catalog → Database (Glue catalog), Database → Schema (Glue database).
		dwhCtx.Database = aws.ToString(qe.QueryExecutionContext.Catalog)
		dwhCtx.Schema = aws.ToString(qe.QueryExecutionContext.Database)
	}
	if dwhCtx.Database == "" {
		dwhCtx.Database = it.catalog
	}

	state := "UNKNOWN"
	if qe.Status.State != "" {
		state = string(qe.Status.State)
	}

	return &querylogs.QueryLog{
		CreatedAt:          createdAt,
		StartedAt:          startedAt,
		FinishedAt:         finishedAt,
		QueryID:            aws.ToString(qe.QueryExecutionId),
		SQL:                sqlText,
		SqlDialect:         it.dialect,
		DwhContext:         dwhCtx,
		QueryType:          strings.ToUpper(string(qe.StatementType)),
		Status:             state,
		SqlObfuscationMode: it.obfuscator.Mode(),
	}, true
}
