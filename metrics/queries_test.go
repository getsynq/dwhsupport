package metrics

//
//import (
//	"testing"
//	"time"
//
//	dwhsql "github.com/getsynq/dwhsupport/sqldialect"
//	"github.com/gkampitakis/go-snaps/snaps"
//	"github.com/stretchr/testify/require"
//	"github.com/stretchr/testify/suite"
//	"google.golang.org/protobuf/types/known/durationpb"
//)
//
//type MetricsSuite struct {
//	suite.Suite
//}
//
//func TestMetricsSuite(t *testing.T) {
//	suite.Run(t, new(MetricsSuite))
//}
//
//var snowflakeTablePath = "sf-account::database::schema::table"
//
//func (s *MetricsSuite) TestDwh() {
//	dailyScheduleWithDelay := &anomaliesv1.MonitorSchedule{
//		Schedule: &anomaliesv1.MonitorSchedule_Daily{
//			Daily: &anomaliesv1.MonitorScheduleDaily{
//				SinceMidnight: durationpb.New(30 * time.Minute),
//			},
//		},
//	}
//	intervalScheduleWithDelay := &anomaliesv1.MonitorSchedule{
//		Schedule: &anomaliesv1.MonitorSchedule_OnInterval{
//			OnInterval: &anomaliesv1.MonitorScheduleOnInterval{
//				Interval: durationpb.New(time.Hour),
//				Delay:    durationpb.New(5 * time.Minute),
//			},
//		},
//	}
//
//	intervalSchedule := &anomaliesv1.MonitorSchedule{
//		Schedule: &anomaliesv1.MonitorSchedule_OnInterval{
//			OnInterval: &anomaliesv1.MonitorScheduleOnInterval{
//				Interval: durationpb.New(time.Hour),
//			},
//		},
//	}
//
//	for _, dialect := range []struct {
//		name    string
//		dialect dwhsql.Dialect
//	}{
//		{"clickhouse", dwhsql.NewClickHouseDialect()},
//		{"snowflake", dwhsql.NewSnowflakeDialect()},
//		{"redshift", dwhsql.NewRedshiftDialect()},
//		{"bigquery", dwhsql.NewBigQueryDialect()},
//		{"postgres", dwhsql.NewPostgresDialect()},
//		{"mysql", dwhsql.NewMySQLDialect()},
//		{"databricks", dwhsql.NewDatabricksDialect()},
//		{"duckdb", dwhsql.NewDuckDBDialect()},
//	} {
//		s.Run(dialect.name, func() {
//			for _, def := range []struct {
//				name                  string
//				def                   *anomaliesv1.MonitorDef
//				expectedResponseTypes []MetricResponseI
//			}{
//				{
//					name:                  "custom_numeric_sql",
//					expectedResponseTypes: []MetricResponseI{&MetricCustomNumeric{}},
//					def: &anomaliesv1.MonitorDef{
//						MonitoredAssetPath: snowflakeTablePath,
//						Args: &anomaliesv1.MonitorArgs{
//							Segmentation: &anomaliesv1.Segmentation{
//								Rule: &anomaliesv1.Segmentation_All_{},
//							},
//						},
//						Partitioning: partitioning("updated_at", time.Hour, false),
//						Monitor: &anomaliesv1.MonitorDef_CustomNumeric{
//							CustomNumeric: &anomaliesv1.MonitorCustomNumeric{
//								Sql: "avg(expected)",
//							},
//						},
//					},
//				},
//
//				{
//					name:                  "volume_without_segments",
//					expectedResponseTypes: []MetricResponseI{&MetricVolume{}},
//					def: &anomaliesv1.MonitorDef{
//						MonitoredAssetPath: snowflakeTablePath,
//						Partitioning:       partitioning("updated_at", time.Hour, false),
//						Monitor: &anomaliesv1.MonitorDef_Volume{
//							Volume: &anomaliesv1.MonitorVolume{},
//						},
//					},
//				},
//
//				{
//					name:                  "volume_with_segments-deprecated",
//					expectedResponseTypes: []MetricResponseI{&MetricVolume{}},
//					def: &anomaliesv1.MonitorDef{
//						MonitoredAssetPath: snowflakeTablePath,
//						AssetFqn: &anomaliesv1.AssetFqn{
//							Database: "database",
//							Schema:   "schema",
//							Table:    "caseSensitiveTable",
//						},
//						Schedule:     dailyScheduleWithDelay,
//						Partitioning: partitioning("updated_at", time.Hour, false),
//						Args:         args("foo", "qux"),
//						Monitor: &anomaliesv1.MonitorDef_Volume{
//							Volume: &anomaliesv1.MonitorVolume{},
//						},
//					},
//				},
//
//				{
//					name:                  "volume_with_segments-rule-all",
//					expectedResponseTypes: []MetricResponseI{&MetricVolume{}},
//					def: &anomaliesv1.MonitorDef{
//						MonitoredAssetPath: snowflakeTablePath,
//						Schedule:           dailyScheduleWithDelay,
//						Partitioning:       partitioning("usage_day", 24*time.Hour, true),
//						Args: &anomaliesv1.MonitorArgs{
//							Filter: "age > 18",
//							Segmentation: &anomaliesv1.Segmentation{
//								Field: "country",
//								Rule:  &anomaliesv1.Segmentation_All_{},
//							},
//						},
//						Monitor: &anomaliesv1.MonitorDef_Volume{
//							Volume: &anomaliesv1.MonitorVolume{},
//						},
//					},
//				},
//
//				{
//					name:                  "volume_with_segments-rule-list",
//					expectedResponseTypes: []MetricResponseI{&MetricVolume{}},
//					def: &anomaliesv1.MonitorDef{
//						MonitoredAssetPath: snowflakeTablePath,
//						Schedule:           intervalScheduleWithDelay,
//						Partitioning:       partitioning("updated_at", time.Hour, false),
//						Args: &anomaliesv1.MonitorArgs{
//							Filter: "age > 18",
//							Segmentation: &anomaliesv1.Segmentation{
//								Field: "age",
//								Rule: &anomaliesv1.Segmentation_List_{
//									List: &anomaliesv1.Segmentation_List{Values: []string{"19", "20", "50"}},
//								},
//							},
//						},
//						Monitor: &anomaliesv1.MonitorDef_Volume{
//							Volume: &anomaliesv1.MonitorVolume{},
//						},
//					},
//				},
//
//				{
//					name:                  "volume_with_segments-rule-list-exclude",
//					expectedResponseTypes: []MetricResponseI{&MetricVolume{}},
//					def: &anomaliesv1.MonitorDef{
//						MonitoredAssetPath: snowflakeTablePath,
//						Schedule:           intervalSchedule,
//						Partitioning:       partitioning("updated_at", time.Hour, false),
//						Args: &anomaliesv1.MonitorArgs{
//							Filter: "age > 18",
//							Segmentation: &anomaliesv1.Segmentation{
//								Field: "age",
//								Rule: &anomaliesv1.Segmentation_ExcludeList{
//									ExcludeList: &anomaliesv1.Segmentation_List{Values: []string{"19", "20", "50"}},
//								},
//							},
//						},
//						Monitor: &anomaliesv1.MonitorDef_Volume{
//							Volume: &anomaliesv1.MonitorVolume{},
//						},
//					},
//				},
//
//				{
//					name:                  "freshness",
//					expectedResponseTypes: []MetricResponseI{&MetricLastLoadedAt{}},
//					def: &anomaliesv1.MonitorDef{
//						MonitoredAssetPath: snowflakeTablePath,
//						Args:               args("foo", "qux"),
//						Partitioning:       partitioning("updated_at", time.Hour, false),
//						Monitor: &anomaliesv1.MonitorDef_Freshness{
//							Freshness: &anomaliesv1.MonitorFreshness{
//								FreshnessSource: &anomaliesv1.MonitorFreshness_Field{
//									Field: "updated_at",
//								},
//							},
//						},
//					},
//				},
//
//				{
//					name: "field_stats",
//					expectedResponseTypes: []MetricResponseI{
//						&MetricNumericFieldStats{Field: "score"},
//						&MetricTextFieldStats{Field: "workspace"},
//						&MetricTimeFieldStats{Field: "updated_at"},
//					},
//					def: &anomaliesv1.MonitorDef{
//						MonitoredAssetPath: snowflakeTablePath,
//						Args:               args("foo", ""),
//						Partitioning:       partitioning("updated_at", time.Hour, false),
//						Monitor: &anomaliesv1.MonitorDef_FieldStats{
//							FieldStats: &anomaliesv1.MonitorFieldStats{
//								Fields: []*anomaliesv1.MonitorFieldStats_FieldDef{
//									{
//										Name:     "score",
//										DataType: anomaliesv1.DataType_DATA_TYPE_NUMERIC,
//									},
//									{
//										Name:     "workspace",
//										DataType: anomaliesv1.DataType_DATA_TYPE_STRING,
//									},
//									{
//										Name:     "updated_at",
//										DataType: anomaliesv1.DataType_DATA_TYPE_TIME,
//									},
//								},
//							},
//						},
//					},
//				},
//			} {
//				s.Run(def.name, func() {
//					queriesDefs, err := MonitorDefToMetricsQueries(def.def)
//					s.Require().NoError(err)
//					s.NotNil(queriesDefs)
//
//					for idx, queryDef := range queriesDefs {
//						sql, err := queryDef.QueryBuilder.ToSql(dialect.dialect)
//						s.NoError(err)
//						snaps.WithConfig(snaps.Filename(dialect.name)).MatchSnapshot(s.T(), sql)
//						s.Equal(def.expectedResponseTypes[idx], queryDef.MetricResponse)
//					}
//				})
//			}
//		})
//	}
//}
//
//func (s *MetricsSuite) TestDwhListSegments() {
//	for _, dialect := range []struct {
//		name    string
//		dialect dwhsql.Dialect
//	}{
//		{"clickhouse", dwhsql.NewClickHouseDialect()},
//		{"snowflake", dwhsql.NewSnowflakeDialect()},
//		{"redshift", dwhsql.NewRedshiftDialect()},
//		{"bigquery", dwhsql.NewBigQueryDialect()},
//		{"postgres", dwhsql.NewPostgresDialect()},
//		{"mysql", dwhsql.NewMySQLDialect()},
//		{"databricks", dwhsql.NewDatabricksDialect()},
//	} {
//		s.Run(dialect.name, func() {
//			def := &anomaliesv1.MonitorDef{
//				MonitoredAssetPath: snowflakeTablePath,
//				Partitioning:       partitioning("updated_at", time.Hour, false),
//				Args: &anomaliesv1.MonitorArgs{
//					Filter: "",
//					Segmentation: &anomaliesv1.Segmentation{
//						Field: "workspace",
//					},
//				},
//				Monitor: &anomaliesv1.MonitorDef_Volume{
//					Volume: &anomaliesv1.MonitorVolume{},
//				},
//			}
//
//			to := time.Date(2023, 2, 31, 1, 0, 0, 0, time.UTC)
//			from := time.Date(2023, 2, 1, 1, 0, 0, 0, time.UTC)
//			q, err := MonitorDefToSegmentListQuery(def, from, to, 10)
//			require.NoError(s.T(), err)
//			require.NotNil(s.T(), q)
//
//			sql, err := q.ToSql(dialect.dialect)
//			require.NoError(s.T(), err)
//			snaps.WithConfig(snaps.Filename(dialect.name)).MatchSnapshot(s.T(), sql)
//		})
//	}
//}
//
//func args(filter string, segmentationField string) *MonitorArgs {
//	if segmentationField == "" {
//		return &MonitorArgs{
//			Filter: filter,
//		}
//	}
//
//	return &MonitorArgs{
//		Filter:       filter,
//		Segmentation: &Segmentation{Field: segmentationField, Rule: &SegmentationRuleAll{}},
//	}
//}
//
//func partitioning(field string, interval time.Duration, isDateField bool) *MonitorPartitioning {
//	return &MonitorPartitioning{
//		Field:       field,
//		Interval:    interval,
//		IsDateField: isDateField,
//	}
//}
