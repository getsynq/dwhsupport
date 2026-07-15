package scrapper

import (
	"fmt"
	"math/big"
	"time"

	"github.com/samber/lo"
)

type DwhFqn struct {
	InstanceName string
	DatabaseName string
	SchemaName   string
	ObjectName   string
}

type HasTableFqn interface {
	TableFqn() DwhFqn
}

type TableMetricsRow struct {
	Instance  string     `db:"instance"   json:"instance"   ch:"instance"   bigquery:"instance"`
	Database  string     `db:"database"   json:"database"   ch:"_database"  bigquery:"database"`
	Schema    string     `db:"schema"     json:"schema"     ch:"schema"     bigquery:"schema"`
	Table     string     `db:"table"      json:"table"      ch:"table"      bigquery:"table"`
	RowCount  *int64     `db:"row_count"  json:"row_count"  ch:"row_count"  bigquery:"row_count"`
	UpdatedAt *time.Time `db:"updated_at" json:"updated_at" ch:"updated_at" bigquery:"updated_at"`
	SizeBytes *int64     `db:"size_bytes" json:"size_bytes" ch:"size_bytes" bigquery:"size_bytes"`
}

func (r TableMetricsRow) TableFqn() DwhFqn {
	return DwhFqn{
		InstanceName: r.Instance,
		DatabaseName: r.Database,
		SchemaName:   r.Schema,
		ObjectName:   r.Table,
	}
}

type Tag struct {
	// Optionally prefix (`tag.` / `policytag.` / etc)
	TagName  string `json:"tag_name"`
	TagValue string `json:"tag_value"`
}

func (t Tag) String() string {
	return fmt.Sprintf("%s:%s", t.TagName, t.TagValue)
}

type CatalogColumnRow struct {
	Instance           string  `db:"instance"             json:"instance"             ch:"instance"             bigquery:"instance"`
	Database           string  `db:"database"             json:"database"             ch:"_database"            bigquery:"database"`
	Schema             string  `db:"schema"               json:"schema"               ch:"schema"               bigquery:"schema"`
	Table              string  `db:"table"                json:"table"                ch:"table"                bigquery:"table"`
	IsView             bool    `db:"is_view"              json:"is_view"              ch:"is_view"              bigquery:"is_view"`
	IsTable            bool    `db:"is_table"             json:"is_table"             ch:"is_table"             bigquery:"is_table"`
	IsMaterializedView bool    `db:"is_materialized_view" json:"is_materialized_view" ch:"is_materialized_view" bigquery:"is_materialized_view"`
	TableType          string  `db:"table_type"           json:"table_type"           ch:"table_type"           bigquery:"table_type"`
	Column             string  `db:"column"               json:"column"               ch:"column"               bigquery:"column"`
	Type               string  `db:"type"                 json:"type"                 ch:"type"                 bigquery:"type"`
	Position           int32   `db:"position"             json:"position"             ch:"position"             bigquery:"position"`
	Comment            *string `db:"comment"              json:"comment"              ch:"comment"              bigquery:"comment"`
	TableComment       *string `db:"table_comment"        json:"table_comment"        ch:"table_comment"        bigquery:"table_comment"`
	ColumnTags         []*Tag  `                          json:"column_tags"`
	TableTags          []*Tag  `                          json:"table_tags"`
	IsStructColumn     bool    `db:"is_struct_column"     json:"is_struct_column"     ch:"is_struct_column"     bigquery:"is_struct_column"`
	IsArrayColumn      bool    `db:"is_array_column"      json:"is_array_column"      ch:"is_array_column"      bigquery:"is_array_column"`
	FieldSchemas       []*SchemaColumnField
}

type SchemaColumnField struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Human readable name of the column as present in dbt or data warehouse.
	HumanName string `protobuf:"bytes,2,opt,name=human_name,json=humanName,proto3" json:"human_name,omitempty"`
	// Native data type of the column as present in data warehouse.
	NativeType string `protobuf:"bytes,4,opt,name=native_type,json=nativeType,proto3" json:"native_type,omitempty"`
	// Description of the column
	Description *string `protobuf:"bytes,5,opt,name=description,proto3" json:"description,omitempty"`
	// Ordinal position of the column in the table, starting from 1
	OrdinalPosition int32 `protobuf:"varint,6,opt,name=ordinal_position,json=ordinalPosition,proto3" json:"ordinal_position,omitempty"`
	// Indicates that the column type could be used as a struct/json in a data warehouse
	IsStruct bool `protobuf:"varint,7,opt,name=is_struct,json=isStruct,proto3" json:"is_struct,omitempty"`
	// Indicates that the column is a repeated field in a data warehouse (e.g. array)
	IsRepeated bool `protobuf:"varint,8,opt,name=is_repeated,json=isRepeated,proto3" json:"is_repeated,omitempty"`
	// Fields inside of the struct/record like column
	Fields []*SchemaColumnField `protobuf:"bytes,9,rep,name=fields,proto3" json:"fields,omitempty"`
}

func (r CatalogColumnRow) TableFqn() DwhFqn {
	return DwhFqn{
		InstanceName: r.Instance,
		DatabaseName: r.Database,
		SchemaName:   r.Schema,
		ObjectName:   r.Table,
	}
}

func (r CatalogColumnRow) GetComment() string {
	if r.Comment == nil {
		return ""
	}
	return *r.Comment
}

func (r CatalogColumnRow) GetTableComment() string {
	if r.TableComment == nil {
		return ""
	}
	return *r.TableComment
}

type Annotation struct {
	AnnotationName  string `json:"annotation_name"`
	AnnotationValue string `json:"annotation_value"`
}

type TableRow struct {
	Instance           string                 `db:"instance"             json:"instance"             ch:"instance"             bigquery:"instance"`
	Database           string                 `db:"database"             json:"database"             ch:"_database"            bigquery:"database"`
	Schema             string                 `db:"schema"               json:"schema"               ch:"schema"               bigquery:"schema"`
	Table              string                 `db:"table"                json:"table"                ch:"table"                bigquery:"table"`
	TableType          string                 `db:"table_type"           json:"table_type"           ch:"table_type"           bigquery:"table_type"`
	Description        *string                `db:"description"          json:"description"          ch:"description"          bigquery:"description"`
	Tags               []*Tag                 `db:"tags"                 json:"tags"`
	IsView             bool                   `db:"is_view"              json:"is_view"              ch:"is_view"`
	IsTable            bool                   `db:"is_table"             json:"is_table"             ch:"is_table"`
	IsMaterializedView bool                   `db:"is_materialized_view" json:"is_materialized_view" ch:"is_materialized_view"`
	Options            map[string]interface{} `db:"options"              json:"options"`
	Annotations        []*Annotation          `db:"annotations"          json:"annotations"`

	// Constraints is optionally populated by QueryTables when WithConstraints() is passed.
	// When non-nil, callers may skip a separate QueryTableConstraints call.
	Constraints []*TableConstraintRow `json:"constraints,omitempty"`
}

func (r TableRow) TableFqn() DwhFqn {
	return DwhFqn{
		InstanceName: r.Instance,
		DatabaseName: r.Database,
		SchemaName:   r.Schema,
		ObjectName:   r.Table,
	}
}

func GetTableRowOption[T any](tableRow *TableRow, optionName string) T {
	def := lo.Empty[T]()
	if tableRow != nil {
		if tableRow.Options != nil && len(tableRow.Options) > 0 {
			if v, found := tableRow.Options[optionName]; found {
				if v, ok := v.(T); ok {
					return v
				}
			}
		}
	}

	return def
}

type SqlDefinitionRow struct {
	Instance           string  `db:"instance"             json:"instance"              ch:"instance"             bigquery:"instance"`
	Database           string  `db:"database"             json:"database"              ch:"_database"            bigquery:"database"`
	Schema             string  `db:"schema"               json:"schema"                ch:"schema"               bigquery:"schema"`
	Table              string  `db:"table"                json:"table"                 ch:"table"                bigquery:"table"`
	IsView             bool    `db:"is_view"              json:"is_view"               ch:"is_view"              bigquery:"is_view"`
	IsTable            bool    `db:"is_table"             json:"is_table"              ch:"is_table"             bigquery:"is_table"`
	IsMaterializedView bool    `db:"is_materialized_view" json:"is_materialized_view"  ch:"is_materialized_view" bigquery:"is_materialized_view"`
	TableType          string  `db:"table_type"           json:"table_type"            ch:"table_type"           bigquery:"table_type"`
	Sql                string  `db:"sql"                  json:"sql"                   ch:"sql"                  bigquery:"sql"`
	Description        *string `                          json:"description,omitempty"`
	Tags               []*Tag  `                          json:"tags,omitempty"`
}

type DatabaseRow struct {
	Instance      string  `db:"instance"       json:"instance"       ch:"instance"       bigquery:"instance"`
	Database      string  `db:"database"       json:"database"       ch:"_database"      bigquery:"database"`
	Description   *string `db:"description"    json:"description"    ch:"description"    bigquery:"description"`
	DatabaseType  *string `db:"database_type"  json:"database_type"  ch:"database_type"  bigquery:"database_type"`
	DatabaseOwner *string `db:"database_owner" json:"database_owner" ch:"database_owner" bigquery:"database_owner"`
}

func (r *DatabaseRow) SetInstance(instance string) {
	r.Instance = instance
}

// SchemaRow describes a single schema (or the warehouse concept we map to a
// schema — e.g. a ClickHouse database, a BigQuery dataset, an Oracle user).
// It mirrors DatabaseRow one level deeper in the (database, schema) hierarchy.
type SchemaRow struct {
	Instance    string  `db:"instance"     json:"instance"     ch:"instance"     bigquery:"instance"`
	Database    string  `db:"database"     json:"database"     ch:"_database"    bigquery:"database"`
	Schema      string  `db:"schema"       json:"schema"       ch:"schema"       bigquery:"schema"`
	Description *string `db:"description"  json:"description"  ch:"description"  bigquery:"description"`
	SchemaType  *string `db:"schema_type"  json:"schema_type"  ch:"schema_type"  bigquery:"schema_type"`
	SchemaOwner *string `db:"schema_owner" json:"schema_owner" ch:"schema_owner" bigquery:"schema_owner"`
}

func (r *SchemaRow) SetInstance(instance string) {
	r.Instance = instance
}

// TableFqn returns the schema's fully-qualified name with an empty object name.
// This lets SchemaRow satisfy HasTableFqn for generic helpers, though scope
// filtering of schemas should use FilterSchemaRows (schema-level partial eval).
func (r SchemaRow) TableFqn() DwhFqn {
	return DwhFqn{
		InstanceName: r.Instance,
		DatabaseName: r.Database,
		SchemaName:   r.Schema,
	}
}

type WithSetInstance interface {
	SetInstance(instance string)
}

func (r SqlDefinitionRow) TableFqn() DwhFqn {
	return DwhFqn{
		InstanceName: r.Instance,
		DatabaseName: r.Database,
		SchemaName:   r.Schema,
		ObjectName:   r.Table,
	}
}

type Value interface {
	isValue()
}

type DoubleValue float64

func (DoubleValue) isValue() {}

type IntValue int64

func (IntValue) isValue() {}

type TimeValue time.Time

func (TimeValue) isValue() {}

type IgnoredValue struct{}

func (IgnoredValue) isValue() {}

// StringValue preserves text values verbatim. Used by RunRawQuery so generic
// "run this query" surfaces see string columns as strings rather than having
// them collapsed to IgnoredValue (as QueryCustomMetrics does, which is correct
// for the metrics/profile agent path but wrong for raw data preview).
type StringValue string

func (StringValue) isValue() {}

// JsonValue carries a complex / nested cell — an array/list/vector, a
// struct/record/named-tuple, a map, or a semi-structured object/variant — as
// canonical JSON text. Warehouses expose these either as native nested values
// (ClickHouse Array/Map/Tuple/Nested, BigQuery repeated fields & STRUCT/RECORD,
// DuckDB LIST/STRUCT/MAP, Postgres arrays, Trino array/map/row) or as
// JSON/semi-structured strings (Snowflake ARRAY/OBJECT/VARIANT, Redshift SUPER,
// Fabric/MSSQL nvarchar JSON). Rather than flatten them to fmt.Sprint garbage or
// lossy scalars, RunRawQuery normalises them to JSON so a single frontend
// renderer can show a structured tree uniformly across every warehouse.
//
// The text is always valid JSON. Precision-sensitive scalars are encoded so the
// JSON round-trip is lossless at the text level: arbitrary-precision integers as
// JSON number literals, decimals as JSON numbers, and timestamps as RFC3339
// strings. JsonValue is only emitted on the RunRawQuery path — the metrics /
// profile path (QueryCustomMetrics) still collapses non-scalar cells to
// IgnoredValue, since a nested cell cannot be a metric or a segment.
type JsonValue string

func (JsonValue) isValue() {}

// BigIntValue represents arbitrary precision integers (e.g., DuckDB hugeint, uint128)
type BigIntValue big.Int

func (*BigIntValue) isValue() {}

// String returns the string representation of the big integer
func (v *BigIntValue) String() string {
	return (*big.Int)(v).String()
}

// BigInt returns the underlying *big.Int
func (v *BigIntValue) BigInt() *big.Int {
	return (*big.Int)(v)
}

// NewBigIntValue creates a BigIntValue from a *big.Int
func NewBigIntValue(v *big.Int) *BigIntValue {
	return (*BigIntValue)(v)
}

type ColumnValue struct {
	Name   string
	Value  Value
	IsNull bool
}

type SegmentValue struct {
	Name  string
	Value string
}

type CustomMetricsRow struct {
	Segments     []*SegmentValue
	ColumnValues []*ColumnValue
}

// TableConstraintRow represents a constraint or key on a table column.
// This covers traditional indexes, primary keys, unique constraints,
// as well as warehouse-specific concepts like BigQuery partitioning/clustering,
// ClickHouse sorting keys, and Snowflake clustering keys.
type TableConstraintRow struct {
	Instance             string `db:"instance"              json:"instance"              ch:"instance"              bigquery:"instance"`
	Database             string `db:"database"              json:"database"              ch:"_database"             bigquery:"database"`
	Schema               string `db:"schema"                json:"schema"                ch:"schema"                bigquery:"schema"`
	Table                string `db:"table"                 json:"table"                 ch:"table"                 bigquery:"table"`
	ConstraintName       string `db:"constraint_name"       json:"constraint_name"       ch:"constraint_name"       bigquery:"constraint_name"`
	ColumnName           string `db:"column_name"           json:"column_name"           ch:"column_name"           bigquery:"column_name"`
	ConstraintType       string `db:"constraint_type"       json:"constraint_type"       ch:"constraint_type"       bigquery:"constraint_type"`
	ColumnPosition       int32  `db:"column_position"       json:"column_position"       ch:"column_position"       bigquery:"column_position"`
	ConstraintExpression string `db:"constraint_expression" json:"constraint_expression" ch:"constraint_expression" bigquery:"constraint_expression"`
	IsEnforced           *bool  `db:"is_enforced"           json:"is_enforced"           ch:"is_enforced"           bigquery:"is_enforced"`
}

func (r TableConstraintRow) TableFqn() DwhFqn {
	return DwhFqn{
		InstanceName: r.Instance,
		DatabaseName: r.Database,
		SchemaName:   r.Schema,
		ObjectName:   r.Table,
	}
}

// Constraint type constants
const (
	ConstraintTypePrimaryKey      = "PRIMARY KEY"
	ConstraintTypeUniqueIndex     = "UNIQUE INDEX"
	ConstraintTypeForeignKey      = "FOREIGN KEY"
	ConstraintTypeIndex           = "INDEX"
	ConstraintTypeSortingKey      = "SORTING KEY"
	ConstraintTypePartitionBy     = "PARTITION BY"
	ConstraintTypeClusterBy       = "CLUSTER BY"
	ConstraintTypeDistributionKey = "DISTRIBUTION KEY"
	ConstraintTypeProjection      = "PROJECTION"
	ConstraintTypeCheck           = "CHECK"
)
