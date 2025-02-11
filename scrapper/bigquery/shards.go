package bigquery

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
)

type ShardedTable struct {
	TableName     string
	Shards        []time.Time
	LatestShardId string
}

func getShardedTables(tableIds []string, lowerDateBound, upperDateBound time.Time) map[string]*ShardedTable {
	shardedTables := make(map[string]*ShardedTable)
	for _, tableId := range tableIds {
		if tableName, ok := shardedTableName(tableId, lowerDateBound, upperDateBound); ok {
			shard := shardedTableIdRegex.FindStringSubmatch(tableId)[2]
			if shardTime, err := time.Parse("20060102", shard); err != nil {
				continue
			} else {
				tableNameLower := strings.ToLower(tableName)
				shardedTable, ok := shardedTables[tableNameLower]
				if !ok {
					shardedTable = &ShardedTable{
						TableName: tableName,
						Shards:    make([]time.Time, 0),
					}
					shardedTables[tableNameLower] = shardedTable
				}

				shardedTable.Shards = append(shardedTable.Shards, shardTime)
			}
		}
	}

	for _, shardedTable := range shardedTables {
		shardedTable.LatestShardId = fmt.Sprintf(
			"%s%s",
			shardedTable.TableName,
			getLatestShard(shardedTable.Shards).Format("20060102"),
		)
		shardedTable.TableName = strings.TrimRight(shardedTable.TableName, "_")
	}

	return shardedTables
}

var shardedTableIdRegex = regexp.MustCompile(`^(.*\D)(\d{8})$`)

func shardedTableName(tableId string, lowerDateBound, upperDateBound time.Time) (string, bool) {
	if shardedTableIdRegex.MatchString(tableId) {
		regexRes := shardedTableIdRegex.FindStringSubmatch(tableId)
		// if suffix is valid date
		if at, err := time.Parse("20060102", regexRes[2]); err == nil {
			if at.After(lowerDateBound) && at.Before(upperDateBound) {
				// return name without suffix
				return regexRes[1], true
			}
		}
	}

	return "", false
}

func getLatestShard(shards []time.Time) time.Time {
	latest := time.Time{}
	for _, shard := range shards {
		if shard.After(latest) {
			latest = shard
		}
	}
	return latest
}

func collapseShardedMetrics(results []*scrapper.TableMetricsRow) ([]*scrapper.TableMetricsRow, error) {
	resultsWithShards := map[string]*scrapper.TableMetricsRow{}
	shardLowerBound, shardUpperBound := getValidShardDateRange()

	// if there are shards collapse them into key/table with only `_` as suffix
	for _, result := range results {
		tableId := result.Table
		if shardedName, isSharded := shardedTableName(result.Table, shardLowerBound, shardUpperBound); isSharded {
			// if it is sharded table w/o `_YYYYMMDD` suffix, add `_` suffix to accumulate metrics and
			// in case there is same non-sharded table keep it separated for later picking the latest
			// updated
			if strings.HasSuffix(shardedName, "_") {
				tableId = shardedName
			} else {
				tableId = shardedName + "_"
			}
		}
		result.Table = tableId
		tableFqn := strings.ToLower(strings.Join([]string{result.Database, result.Schema, tableId}, "::"))

		if existingRes, ok := resultsWithShards[tableFqn]; !ok {
			resultsWithShards[tableFqn] = result
		} else {
			if result.RowCount != nil {
				if existingRes.RowCount == nil {
					existingRes.RowCount = lo.ToPtr(*result.RowCount)
				} else {
					*existingRes.RowCount += *result.RowCount
				}
			}
			if result.SizeBytes != nil {
				if existingRes.SizeBytes == nil {
					existingRes.SizeBytes = lo.ToPtr(*result.SizeBytes)
				} else {
					*existingRes.SizeBytes += *result.SizeBytes
				}
			}

			if result.UpdatedAt.After(*existingRes.UpdatedAt) {
				existingRes.UpdatedAt = result.UpdatedAt
			}
		}
	}

	// in case we have same tables sharded and non-sharded, use the latest one
	// and handle `_` suffix in the table name if latest was sharded
	collapsedResults := map[string]*scrapper.TableMetricsRow{}
	for fqn, result := range resultsWithShards {
		fqnWithoutShardPart := strings.TrimRight(fqn, "_")
		tableId := strings.TrimRight(result.Table, "_")

		if existingRes, ok := collapsedResults[fqnWithoutShardPart]; !ok {
			result.Table = tableId
			collapsedResults[fqnWithoutShardPart] = result
		} else {
			if existingRes.UpdatedAt.Before(*result.UpdatedAt) {
				result.Table = tableId
				collapsedResults[fqnWithoutShardPart] = result
			}
		}
	}

	return lo.Values(collapsedResults), nil
}

func collapseShardedSqlDefinitions(results []*scrapper.SqlDefinitionRow) ([]*scrapper.SqlDefinitionRow, error) {
	shardLowerBound, shardUpperBound := getValidShardDateRange()

	shardedTables := getShardedTables(lo.Map(results, func(r *scrapper.SqlDefinitionRow, _ int) string {
		return strings.Join([]string{r.Database, r.Schema, r.Table}, "::")
	}), shardLowerBound, shardUpperBound)

	collapsedResults := map[string]*scrapper.SqlDefinitionRow{}

	for _, result := range results {
		tableFqn := strings.ToLower(strings.Join([]string{result.Database, result.Schema, result.Table}, "::"))
		// Test if the table has shard suffix
		if shardedName, ok := shardedTableName(tableFqn, shardLowerBound, shardUpperBound); ok {

			// Test that it was actually parsed as sharded table before
			if shardedTable, ok := shardedTables[shardedName]; ok {
				tableId := strings.Split(shardedTable.TableName, "::")
				// Test if we already seen any shard
				if strings.HasSuffix(shardedTable.LatestShardId, result.Table) {
					// Create new result
					collapsedResults[shardedTable.TableName] = &scrapper.SqlDefinitionRow{
						Instance: result.Instance,
						Database: result.Database,
						Schema:   result.Schema,
						Table:    tableId[len(tableId)-1],
						IsView:   result.IsView,
						Sql:      strings.ReplaceAll(result.Sql, result.Table, tableId[len(tableId)-1]),
					}
				}
			} else {
				collapsedResults[tableFqn] = result
			}
		} else {
			collapsedResults[tableFqn] = result
		}
	}

	return lo.Values(collapsedResults), nil
}

func getValidShardDateRange() (time.Time, time.Time) {
	now := time.Now()
	shardLowerBound := now.AddDate(-30, 0, 0)
	shardUpperBound := now.AddDate(30, 0, 0)
	return shardLowerBound, shardUpperBound
}
