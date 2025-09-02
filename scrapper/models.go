package scrapper

import (
	"fmt"
	"time"
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
	Instance       string  `db:"instance"         json:"instance"         ch:"instance"         bigquery:"instance"`
	Database       string  `db:"database"         json:"database"         ch:"_database"        bigquery:"database"`
	Schema         string  `db:"schema"           json:"schema"           ch:"schema"           bigquery:"schema"`
	Table          string  `db:"table"            json:"table"            ch:"table"            bigquery:"table"`
	IsView         bool    `db:"is_view"          json:"is_view"          ch:"is_view"          bigquery:"is_view"`
	Column         string  `db:"column"           json:"column"           ch:"column"           bigquery:"column"`
	Type           string  `db:"type"             json:"type"             ch:"type"             bigquery:"type"`
	Position       int32   `db:"position"         json:"position"         ch:"position"         bigquery:"position"`
	Comment        *string `db:"comment"          json:"comment"          ch:"comment"          bigquery:"comment"`
	TableComment   *string `db:"table_comment"    json:"table_comment"    ch:"table_comment"    bigquery:"table_comment"`
	ColumnTags     []*Tag  `                      json:"column_tags"`
	TableTags      []*Tag  `                      json:"table_tags"`
	IsStructColumn bool    `db:"is_struct_column" json:"is_struct_column" ch:"is_struct_column" bigquery:"is_struct_column"`
	IsArrayColumn  bool    `db:"is_array_column"  json:"is_array_column"  ch:"is_array_column"  bigquery:"is_array_column"`
	FieldSchemas   []*SchemaColumnField
}

type SchemaColumnField struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3"                                   json:"name,omitempty"`
	// Human readable name of the column as present in dbt or data warehouse.
	HumanName string `protobuf:"bytes,2,opt,name=human_name,json=humanName,proto3"              json:"human_name,omitempty"`
	// Native data type of the column as present in data warehouse.
	NativeType string `protobuf:"bytes,4,opt,name=native_type,json=nativeType,proto3"            json:"native_type,omitempty"`
	// Description of the column
	Description *string `protobuf:"bytes,5,opt,name=description,proto3"                            json:"description,omitempty"`
	// Ordinal position of the column in the table, starting from 1
	OrdinalPosition int32 `protobuf:"varint,6,opt,name=ordinal_position,json=ordinalPosition,proto3" json:"ordinal_position,omitempty"`
	// Indicates that the column type could be used as a struct/json in a data warehouse
	IsStruct bool `protobuf:"varint,7,opt,name=is_struct,json=isStruct,proto3"               json:"is_struct,omitempty"`
	// Indicates that the column is a repeated field in a data warehouse (e.g. array)
	IsRepeated bool `protobuf:"varint,8,opt,name=is_repeated,json=isRepeated,proto3"           json:"is_repeated,omitempty"`
	// Fields inside of the struct/record like column
	Fields []*SchemaColumnField `protobuf:"bytes,9,rep,name=fields,proto3"                                 json:"fields,omitempty"`
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
	Instance    string                 `db:"instance"    json:"instance"    ch:"instance"    bigquery:"instance"`
	Database    string                 `db:"database"    json:"database"    ch:"_database"   bigquery:"database"`
	Schema      string                 `db:"schema"      json:"schema"      ch:"schema"      bigquery:"schema"`
	Table       string                 `db:"table"       json:"table"       ch:"table"       bigquery:"table"`
	TableType   string                 `db:"table_type"  json:"table_type"  ch:"table_type"  bigquery:"table_type"`
	Description *string                `db:"description" json:"description" ch:"description" bigquery:"description"`
	Tags        []*Tag                 `db:"tags"        json:"tags"`
	IsView      bool                   `db:"is_view"     json:"is_view"     ch:"is_view"`
	IsTable     bool                   `db:"is_table"    json:"is_table"    ch:"is_table"`
	Options     map[string]interface{} `db:"options"     json:"options"`
	Annotations []*Annotation          `db:"annotations"  json:"annotations"`
}

func (r TableRow) TableFqn() DwhFqn {
	return DwhFqn{
		InstanceName: r.Instance,
		DatabaseName: r.Database,
		SchemaName:   r.Schema,
		ObjectName:   r.Table,
	}
}

type SqlDefinitionRow struct {
	Instance           string `db:"instance"             json:"instance"             ch:"instance"             bigquery:"instance"`
	Database           string `db:"database"             json:"database"             ch:"_database"            bigquery:"database"`
	Schema             string `db:"schema"               json:"schema"               ch:"schema"               bigquery:"schema"`
	Table              string `db:"table"                json:"table"                ch:"table"                bigquery:"table"`
	IsView             bool   `db:"is_view"              json:"is_view"              ch:"is_view"              bigquery:"is_view"`
	IsMaterializedView bool   `db:"is_materialized_view" json:"is_materialized_view" ch:"is_materialized_view" bigquery:"is_materialized_view"`
	Sql                string `db:"sql"                  json:"sql"                  ch:"sql"                  bigquery:"sql"`
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
