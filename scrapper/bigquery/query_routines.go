package bigquery

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

// routinesSql reads the routines with the DDL that creates each. PARAMETERS
// lists a function's return value at ordinal 0, and ROUTINE_OPTIONS holds the
// description as a string literal. {SCHEMA} is the region or the dataset whose
// INFORMATION_SCHEMA is read.
const routinesSql = `
select
	r.routine_catalog as database,
	r.routine_schema as schema,
	r.routine_name as name,
	r.routine_type as routine_type,
	coalesce(r.external_language, r.routine_body) as language,
	p.arguments as arguments,
	r.data_type as return_type,
	r.ddl as definition,
	o.option_value as description
from
	{SCHEMA}.INFORMATION_SCHEMA.ROUTINES r
	left join (
		select
			specific_schema,
			specific_name,
			string_agg(concat(coalesce(parameter_name, ''), ' ', data_type), ', ' order by ordinal_position) as arguments
		from {SCHEMA}.INFORMATION_SCHEMA.PARAMETERS
		where ordinal_position > 0
		group by specific_schema, specific_name
	) p
		on p.specific_schema = r.specific_schema and p.specific_name = r.specific_name
	left join {SCHEMA}.INFORMATION_SCHEMA.ROUTINE_OPTIONS o
		on o.specific_schema = r.specific_schema and o.specific_name = r.specific_name and o.option_name = 'description'
where r.routine_catalog is not null and r.routine_schema is not null and r.routine_name is not null
order by r.routine_schema, r.routine_name
`

// QueryRoutines reads the procedures, functions and table functions from the
// region's INFORMATION_SCHEMA.ROUTINES. A role granted only on the datasets may
// not read that, but may read each dataset's own, so a refused region read
// falls back to reading each dataset, and the scrapper remembers the refusal
// and reads each dataset from then on. A dataset that refuses that too is left
// out.
func (e *BigQueryScrapper) QueryRoutines(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	if e.regionRoutinesRefused.known() {
		return e.queryRoutinesPerDataset(ctx)
	}
	rows, err := e.readRoutines(ctx, fmt.Sprintf("`region-%s`", e.conf.Region))
	if e.regionRoutinesRefused.remember(ctx, "ROUTINES", err) {
		return e.queryRoutinesPerDataset(ctx)
	}
	return rows, err
}

func (e *BigQueryScrapper) queryRoutinesPerDataset(ctx context.Context) ([]*scrapper.RoutineRow, error) {
	allDatasets, err := e.listDatasets(ctx)
	if err != nil {
		return nil, err
	}

	var rows []*scrapper.RoutineRow
	var mutex sync.Mutex

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(datasetQueryConcurrency)
	for _, dataset := range allDatasets {
		datasetId := dataset.DatasetID
		if isPrivateDataset(datasetId) || !e.scope.IsSchemaAccepted(e.conf.ProjectId, datasetId) {
			continue
		}

		g.Go(func() error {
			datasetRows, err := e.readRoutines(groupCtx, fmt.Sprintf("`%s`.`%s`", e.conf.ProjectId, datasetId))
			if isRefused(err) {
				logging.GetLogger(groupCtx).WithField("dataset", datasetId).WithError(err).
					Warn("dataset INFORMATION_SCHEMA.ROUTINES refused, leaving its routines out")
				return nil
			}
			if err != nil {
				return errors.Wrapf(err, "reading the routines of dataset %s", datasetId)
			}

			mutex.Lock()
			defer mutex.Unlock()
			rows = append(rows, datasetRows...)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return rows, nil
}

// readRoutines runs routinesSql over schema and returns the routines of the
// datasets in scope.
func (e *BigQueryScrapper) readRoutines(ctx context.Context, schema string) ([]*scrapper.RoutineRow, error) {
	it, err := e.queryRows(ctx, strings.ReplaceAll(routinesSql, "{SCHEMA}", schema))
	if err != nil {
		return nil, err
	}

	var results []*scrapper.RoutineRow
	for {
		var res map[string]bigquery.Value
		err := it.Next(&res)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}

		row := routineFromValues(res)
		if !e.scope.IsSchemaAccepted(e.conf.ProjectId, row.Schema) {
			continue
		}
		results = append(results, row)
	}
	return results, nil
}

func routineFromValues(res map[string]bigquery.Value) *scrapper.RoutineRow {
	routineType := stringValue(res["routine_type"])
	kind := scrapper.RoutineKindFunction
	if routineType == "PROCEDURE" {
		kind = scrapper.RoutineKindProcedure
	}
	row := &scrapper.RoutineRow{
		Database:    stringValue(res["database"]),
		Schema:      stringValue(res["schema"]),
		Name:        stringValue(res["name"]),
		Kind:        kind,
		RoutineType: routineType,
		Language:    stringPtrValue(res["language"]),
		Arguments:   stringPtrValue(res["arguments"]),
		ReturnType:  stringPtrValue(res["return_type"]),
		Definition:  stringPtrValue(res["definition"]),
	}
	if description := stringPtrValue(res["description"]); description != nil {
		row.Description = unquoteOptionValue(*description)
	}
	return row
}

// unquoteOptionValue reads a string option as ROUTINE_OPTIONS shows it, a
// double-quoted literal. A value it cannot unquote is kept as it is.
func unquoteOptionValue(value string) *string {
	if unquoted, err := strconv.Unquote(value); err == nil {
		value = unquoted
	}
	if value == "" {
		return nil
	}
	return &value
}

func stringValue(v bigquery.Value) string {
	s, _ := v.(string)
	return s
}

func stringPtrValue(v bigquery.Value) *string {
	s, ok := v.(string)
	if !ok || s == "" {
		return nil
	}
	return &s
}
