package scrapper

import (
	"fmt"
	"math/big"
	"strings"
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

// TagKind says what a warehouse means by a tag, as the platforms use one word
// for things that work differently.
type TagKind string

const (
	// TagKindLabel is free key-value metadata on an object: BigQuery labels and
	// Databricks tags. The name is the key, the value may be empty.
	TagKindLabel TagKind = "label"
	// TagKindGovernedTag is a tag that is an object of its own, with the values
	// it allows and the policies it carries: Snowflake tags. The name is the
	// tag's `<TAG_DATABASE>.<TAG_SCHEMA>.<TAG_NAME>`, unquoted, as Snowflake
	// stores it.
	TagKindGovernedTag TagKind = "governed_tag"
	// TagKindPolicyTag is a classification in a taxonomy that controls who
	// reads a column: BigQuery policy tags, on columns only. The name is the
	// display name of the taxonomy and the value the display path of the policy
	// tag inside it, `<parent>:...:<tag>`. One whose taxonomy could not be read
	// is named by resource instead, see IsUnresolvedPolicyTag.
	TagKindPolicyTag TagKind = "policy_tag"
)

// Tag is a tag or label the warehouse assigns to a table or a column. Kind
// says which, and the name is the platform's own, with nothing added to it.
type Tag struct {
	TagName  string  `json:"tag_name"`
	TagValue string  `json:"tag_value"`
	Kind     TagKind `json:"kind,omitempty"`
}

// IsUnresolvedPolicyTag is whether this is a policy tag whose taxonomy could
// not be read, so it carries the taxonomy's resource name as its name and the
// policy tag's resource name as its value, rather than their display names.
func (t Tag) IsUnresolvedPolicyTag() bool {
	return t.Kind == TagKindPolicyTag && strings.HasPrefix(t.TagName, policyTagResourcePrefix)
}

// policyTagResourcePrefix starts the resource name of a Data Catalog taxonomy.
// A display name cannot start with it, as it holds a '/'.
const policyTagResourcePrefix = "projects/"

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
	// Precision and Scale are an exact numeric column's declared size, and
	// NumericCategory what kind of number the column holds, all filled by
	// ResolveNumeric. Both sizes are nil for every other column, and for a
	// numeric whose size the warehouse does not report: a nil scale is not a
	// scale of 0.
	Precision       *int64          `db:"numeric_precision" json:"precision,omitempty"        ch:"numeric_precision"`
	Scale           *int64          `db:"numeric_scale"     json:"scale,omitempty"            ch:"numeric_scale"`
	NumericCategory NumericCategory `                       json:"numeric_category,omitempty"`
	// IsNullable is true when the column accepts NULL and false when it is
	// declared NOT NULL. Nil when the warehouse does not report it, which is
	// never read as either.
	IsNullable *bool `db:"is_nullable" json:"is_nullable,omitempty" ch:"is_nullable" bigquery:"is_nullable"`
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

	// Owner is the role or user the warehouse names as the object's owner, nil
	// when it does not report one.
	Owner *string `db:"owner" json:"owner,omitempty" ch:"owner"`
	// CreatedAt is when the warehouse created the object, always UTC. Nil when
	// it does not report it.
	CreatedAt *time.Time `db:"created_at" json:"created_at,omitempty" ch:"created_at"`

	// Constraints is optionally populated by QueryTables when WithConstraints() is passed.
	// When non-nil, callers may skip a separate QueryTableConstraints call.
	Constraints []*TableConstraintRow `json:"constraints,omitempty"`

	// CopyOf is the table this one is a copy of, as the warehouse states it:
	// the base table of a BigQuery table snapshot or table clone. Nil when the
	// object is not a copy, or the warehouse does not say what it copies.
	CopyOf *TableCopyOf `json:"copy_of,omitempty"`
}

// TableCopyKind is how a table copy was made, in the platform's own terms.
type TableCopyKind string

const (
	// TableCopySnapshot is a read-only copy of the base table at one point in
	// time (BigQuery table snapshot).
	TableCopySnapshot TableCopyKind = "snapshot"
	// TableCopyClone is a writable copy that starts from the base table at one
	// point in time and is changed on its own after that (BigQuery table clone).
	TableCopyClone TableCopyKind = "clone"
)

// TableCopyOf names the base table a copy was made from. The copy holds the
// base table's data as of At, so it is neither a view of the base nor fed by
// it: a caller decides whether that is lineage.
type TableCopyOf struct {
	Kind     TableCopyKind `json:"kind"`
	Instance string        `json:"instance,omitempty"`
	Database string        `json:"database"`
	Schema   string        `json:"schema"`
	Table    string        `json:"table"`
	// At is the time of the base table the copy holds, in UTC. Nil when the
	// warehouse does not report it.
	At *time.Time `json:"at,omitempty"`
}

func (c TableCopyOf) TableFqn() DwhFqn {
	return DwhFqn{
		InstanceName: c.Instance,
		DatabaseName: c.Database,
		SchemaName:   c.Schema,
		ObjectName:   c.Table,
	}
}

func (r TableRow) TableFqn() DwhFqn {
	return DwhFqn{
		InstanceName: r.Instance,
		DatabaseName: r.Database,
		SchemaName:   r.Schema,
		ObjectName:   r.Table,
	}
}

// UTCTimePtr returns t in UTC, or nil when t is nil or the zero time, which
// several APIs return for a time they do not know.
func UTCTimePtr(t *time.Time) *time.Time {
	if t == nil || t.IsZero() {
		return nil
	}
	return lo.ToPtr(t.UTC())
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
	// ColumnTags holds the tags of each column the definition assigns them in,
	// by the column name as the definition writes it, unquoted.
	ColumnTags map[string][]*Tag `json:"column_tags,omitempty"`
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
	// The table a FOREIGN KEY references, on the same instance, and on each
	// column's row the referenced column at the same position. Empty for every
	// other constraint type.
	ReferencedDatabase   string `db:"referenced_database"    json:"referenced_database,omitempty"    ch:"referenced_database"    bigquery:"referenced_database"`
	ReferencedSchema     string `db:"referenced_schema"      json:"referenced_schema,omitempty"      ch:"referenced_schema"      bigquery:"referenced_schema"`
	ReferencedTable      string `db:"referenced_table"       json:"referenced_table,omitempty"       ch:"referenced_table"       bigquery:"referenced_table"`
	ReferencedColumnName string `db:"referenced_column_name" json:"referenced_column_name,omitempty" ch:"referenced_column_name" bigquery:"referenced_column_name"`
}

func (r TableConstraintRow) TableFqn() DwhFqn {
	return DwhFqn{
		InstanceName: r.Instance,
		DatabaseName: r.Database,
		SchemaName:   r.Schema,
		ObjectName:   r.Table,
	}
}

// ReferencedTableFqn is the table a FOREIGN KEY row references, on the row's
// own instance, and false for a row that references none.
func (r TableConstraintRow) ReferencedTableFqn() (DwhFqn, bool) {
	if r.ReferencedTable == "" {
		return DwhFqn{}, false
	}
	return DwhFqn{
		InstanceName: r.Instance,
		DatabaseName: r.ReferencedDatabase,
		SchemaName:   r.ReferencedSchema,
		ObjectName:   r.ReferencedTable,
	}, true
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
