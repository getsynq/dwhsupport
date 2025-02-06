// Generated by dev-tools; DO NOT EDIT.
// Modify or add entity types in proto/core/types/v1/asset_type.proto
// Modify template at dev-tools/contracts/templates/entity_types.proto.tpl

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        (unknown)
// source: synq/entities/v1/entity_types.proto

package v1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type EntityType int32

const (
	EntityType_ENTITY_TYPE_UNSPECIFIED                   EntityType = 0
	EntityType_ENTITY_TYPE_BQ_TABLE                      EntityType = 103
	EntityType_ENTITY_TYPE_BQ_VIEW                       EntityType = 105
	EntityType_ENTITY_TYPE_LOOKER_LOOK                   EntityType = 201
	EntityType_ENTITY_TYPE_LOOKER_EXPLORE                EntityType = 203
	EntityType_ENTITY_TYPE_LOOKER_VIEW                   EntityType = 207
	EntityType_ENTITY_TYPE_LOOKER_DASHBOARD              EntityType = 208
	EntityType_ENTITY_TYPE_DBT_MODEL                     EntityType = 301
	EntityType_ENTITY_TYPE_DBT_TEST                      EntityType = 302
	EntityType_ENTITY_TYPE_DBT_SOURCE                    EntityType = 303
	EntityType_ENTITY_TYPE_DBT_PROJECT                   EntityType = 306
	EntityType_ENTITY_TYPE_DBT_METRIC                    EntityType = 307
	EntityType_ENTITY_TYPE_DBT_SNAPSHOT                  EntityType = 310
	EntityType_ENTITY_TYPE_DBT_SEED                      EntityType = 311
	EntityType_ENTITY_TYPE_DBT_ANALYSIS                  EntityType = 312
	EntityType_ENTITY_TYPE_DBT_EXPOSURE                  EntityType = 313
	EntityType_ENTITY_TYPE_DBT_GROUP                     EntityType = 314
	EntityType_ENTITY_TYPE_DBT_CLOUD_PROJECT             EntityType = 352
	EntityType_ENTITY_TYPE_DBT_CLOUD_JOB                 EntityType = 353
	EntityType_ENTITY_TYPE_SNOWFLAKE_TABLE               EntityType = 503
	EntityType_ENTITY_TYPE_SNOWFLAKE_VIEW                EntityType = 508
	EntityType_ENTITY_TYPE_REDSHIFT_TABLE                EntityType = 803
	EntityType_ENTITY_TYPE_REDSHIFT_VIEW                 EntityType = 805
	EntityType_ENTITY_TYPE_TABLEAU_EMBEDDED              EntityType = 1101
	EntityType_ENTITY_TYPE_TABLEAU_PUBLISHED             EntityType = 1102
	EntityType_ENTITY_TYPE_TABLEAU_CUSTOM_SQL            EntityType = 1103
	EntityType_ENTITY_TYPE_TABLEAU_TABLE                 EntityType = 1104
	EntityType_ENTITY_TYPE_TABLEAU_SHEET                 EntityType = 1105
	EntityType_ENTITY_TYPE_TABLEAU_DASHBOARD             EntityType = 1106
	EntityType_ENTITY_TYPE_AIRFLOW_DAG                   EntityType = 1201
	EntityType_ENTITY_TYPE_AIRFLOW_TASK                  EntityType = 1202
	EntityType_ENTITY_TYPE_CLICKHOUSE_TABLE              EntityType = 1303
	EntityType_ENTITY_TYPE_CLICKHOUSE_VIEW               EntityType = 1305
	EntityType_ENTITY_TYPE_ANOMALY_MONITOR               EntityType = 1403
	EntityType_ENTITY_TYPE_ANOMALY_MONITOR_SEGMENT       EntityType = 1404
	EntityType_ENTITY_TYPE_SQLTEST_TEST                  EntityType = 1421
	EntityType_ENTITY_TYPE_POSTGRES_TABLE                EntityType = 1603
	EntityType_ENTITY_TYPE_POSTGRES_VIEW                 EntityType = 1605
	EntityType_ENTITY_TYPE_MYSQL_TABLE                   EntityType = 1703
	EntityType_ENTITY_TYPE_MYSQL_VIEW                    EntityType = 1705
	EntityType_ENTITY_TYPE_DATABRICKS_WAREHOUSE          EntityType = 1801
	EntityType_ENTITY_TYPE_DATABRICKS_TABLE              EntityType = 1804
	EntityType_ENTITY_TYPE_DATABRICKS_VIEW               EntityType = 1805
	EntityType_ENTITY_TYPE_DATABRICKS_JOB                EntityType = 1807
	EntityType_ENTITY_TYPE_DATABRICKS_NOTEBOOK           EntityType = 1809
	EntityType_ENTITY_TYPE_DATABRICKS_QUERY              EntityType = 1810
	EntityType_ENTITY_TYPE_DATABRICKS_DASHBOARD          EntityType = 1811
	EntityType_ENTITY_TYPE_SQLMESH_PROJECT               EntityType = 1901
	EntityType_ENTITY_TYPE_SQLMESH_SQL_MODEL             EntityType = 1902
	EntityType_ENTITY_TYPE_SQLMESH_PYTHON_MODEL          EntityType = 1903
	EntityType_ENTITY_TYPE_SQLMESH_EXTERNAL              EntityType = 1904
	EntityType_ENTITY_TYPE_SQLMESH_SEED                  EntityType = 1905
	EntityType_ENTITY_TYPE_SQLMESH_AUDIT                 EntityType = 1906
	EntityType_ENTITY_TYPE_SQLMESH_UNIT_TEST             EntityType = 1907
	EntityType_ENTITY_TYPE_SQLMESH_ENVIRONMENT           EntityType = 1908
	EntityType_ENTITY_TYPE_SQLMESH_SNAPSHOT              EntityType = 1909
	EntityType_ENTITY_TYPE_DUCKDB_TABLE                  EntityType = 2003
	EntityType_ENTITY_TYPE_DUCKDB_VIEW                   EntityType = 2005
	EntityType_ENTITY_TYPE_CUSTOM_ENTITY_GENERIC         EntityType = 50000
	EntityType_ENTITY_TYPE_CUSTOM_ENTITY_CUSTOM_TYPE_MIN EntityType = 50001
	EntityType_ENTITY_TYPE_CUSTOM_ENTITY_CUSTOM_TYPE_MAX EntityType = 59999
)

// Enum value maps for EntityType.
var (
	EntityType_name = map[int32]string{
		0:     "ENTITY_TYPE_UNSPECIFIED",
		103:   "ENTITY_TYPE_BQ_TABLE",
		105:   "ENTITY_TYPE_BQ_VIEW",
		201:   "ENTITY_TYPE_LOOKER_LOOK",
		203:   "ENTITY_TYPE_LOOKER_EXPLORE",
		207:   "ENTITY_TYPE_LOOKER_VIEW",
		208:   "ENTITY_TYPE_LOOKER_DASHBOARD",
		301:   "ENTITY_TYPE_DBT_MODEL",
		302:   "ENTITY_TYPE_DBT_TEST",
		303:   "ENTITY_TYPE_DBT_SOURCE",
		306:   "ENTITY_TYPE_DBT_PROJECT",
		307:   "ENTITY_TYPE_DBT_METRIC",
		310:   "ENTITY_TYPE_DBT_SNAPSHOT",
		311:   "ENTITY_TYPE_DBT_SEED",
		312:   "ENTITY_TYPE_DBT_ANALYSIS",
		313:   "ENTITY_TYPE_DBT_EXPOSURE",
		314:   "ENTITY_TYPE_DBT_GROUP",
		352:   "ENTITY_TYPE_DBT_CLOUD_PROJECT",
		353:   "ENTITY_TYPE_DBT_CLOUD_JOB",
		503:   "ENTITY_TYPE_SNOWFLAKE_TABLE",
		508:   "ENTITY_TYPE_SNOWFLAKE_VIEW",
		803:   "ENTITY_TYPE_REDSHIFT_TABLE",
		805:   "ENTITY_TYPE_REDSHIFT_VIEW",
		1101:  "ENTITY_TYPE_TABLEAU_EMBEDDED",
		1102:  "ENTITY_TYPE_TABLEAU_PUBLISHED",
		1103:  "ENTITY_TYPE_TABLEAU_CUSTOM_SQL",
		1104:  "ENTITY_TYPE_TABLEAU_TABLE",
		1105:  "ENTITY_TYPE_TABLEAU_SHEET",
		1106:  "ENTITY_TYPE_TABLEAU_DASHBOARD",
		1201:  "ENTITY_TYPE_AIRFLOW_DAG",
		1202:  "ENTITY_TYPE_AIRFLOW_TASK",
		1303:  "ENTITY_TYPE_CLICKHOUSE_TABLE",
		1305:  "ENTITY_TYPE_CLICKHOUSE_VIEW",
		1403:  "ENTITY_TYPE_ANOMALY_MONITOR",
		1404:  "ENTITY_TYPE_ANOMALY_MONITOR_SEGMENT",
		1421:  "ENTITY_TYPE_SQLTEST_TEST",
		1603:  "ENTITY_TYPE_POSTGRES_TABLE",
		1605:  "ENTITY_TYPE_POSTGRES_VIEW",
		1703:  "ENTITY_TYPE_MYSQL_TABLE",
		1705:  "ENTITY_TYPE_MYSQL_VIEW",
		1801:  "ENTITY_TYPE_DATABRICKS_WAREHOUSE",
		1804:  "ENTITY_TYPE_DATABRICKS_TABLE",
		1805:  "ENTITY_TYPE_DATABRICKS_VIEW",
		1807:  "ENTITY_TYPE_DATABRICKS_JOB",
		1809:  "ENTITY_TYPE_DATABRICKS_NOTEBOOK",
		1810:  "ENTITY_TYPE_DATABRICKS_QUERY",
		1811:  "ENTITY_TYPE_DATABRICKS_DASHBOARD",
		1901:  "ENTITY_TYPE_SQLMESH_PROJECT",
		1902:  "ENTITY_TYPE_SQLMESH_SQL_MODEL",
		1903:  "ENTITY_TYPE_SQLMESH_PYTHON_MODEL",
		1904:  "ENTITY_TYPE_SQLMESH_EXTERNAL",
		1905:  "ENTITY_TYPE_SQLMESH_SEED",
		1906:  "ENTITY_TYPE_SQLMESH_AUDIT",
		1907:  "ENTITY_TYPE_SQLMESH_UNIT_TEST",
		1908:  "ENTITY_TYPE_SQLMESH_ENVIRONMENT",
		1909:  "ENTITY_TYPE_SQLMESH_SNAPSHOT",
		2003:  "ENTITY_TYPE_DUCKDB_TABLE",
		2005:  "ENTITY_TYPE_DUCKDB_VIEW",
		50000: "ENTITY_TYPE_CUSTOM_ENTITY_GENERIC",
		50001: "ENTITY_TYPE_CUSTOM_ENTITY_CUSTOM_TYPE_MIN",
		59999: "ENTITY_TYPE_CUSTOM_ENTITY_CUSTOM_TYPE_MAX",
	}
	EntityType_value = map[string]int32{
		"ENTITY_TYPE_UNSPECIFIED":                   0,
		"ENTITY_TYPE_BQ_TABLE":                      103,
		"ENTITY_TYPE_BQ_VIEW":                       105,
		"ENTITY_TYPE_LOOKER_LOOK":                   201,
		"ENTITY_TYPE_LOOKER_EXPLORE":                203,
		"ENTITY_TYPE_LOOKER_VIEW":                   207,
		"ENTITY_TYPE_LOOKER_DASHBOARD":              208,
		"ENTITY_TYPE_DBT_MODEL":                     301,
		"ENTITY_TYPE_DBT_TEST":                      302,
		"ENTITY_TYPE_DBT_SOURCE":                    303,
		"ENTITY_TYPE_DBT_PROJECT":                   306,
		"ENTITY_TYPE_DBT_METRIC":                    307,
		"ENTITY_TYPE_DBT_SNAPSHOT":                  310,
		"ENTITY_TYPE_DBT_SEED":                      311,
		"ENTITY_TYPE_DBT_ANALYSIS":                  312,
		"ENTITY_TYPE_DBT_EXPOSURE":                  313,
		"ENTITY_TYPE_DBT_GROUP":                     314,
		"ENTITY_TYPE_DBT_CLOUD_PROJECT":             352,
		"ENTITY_TYPE_DBT_CLOUD_JOB":                 353,
		"ENTITY_TYPE_SNOWFLAKE_TABLE":               503,
		"ENTITY_TYPE_SNOWFLAKE_VIEW":                508,
		"ENTITY_TYPE_REDSHIFT_TABLE":                803,
		"ENTITY_TYPE_REDSHIFT_VIEW":                 805,
		"ENTITY_TYPE_TABLEAU_EMBEDDED":              1101,
		"ENTITY_TYPE_TABLEAU_PUBLISHED":             1102,
		"ENTITY_TYPE_TABLEAU_CUSTOM_SQL":            1103,
		"ENTITY_TYPE_TABLEAU_TABLE":                 1104,
		"ENTITY_TYPE_TABLEAU_SHEET":                 1105,
		"ENTITY_TYPE_TABLEAU_DASHBOARD":             1106,
		"ENTITY_TYPE_AIRFLOW_DAG":                   1201,
		"ENTITY_TYPE_AIRFLOW_TASK":                  1202,
		"ENTITY_TYPE_CLICKHOUSE_TABLE":              1303,
		"ENTITY_TYPE_CLICKHOUSE_VIEW":               1305,
		"ENTITY_TYPE_ANOMALY_MONITOR":               1403,
		"ENTITY_TYPE_ANOMALY_MONITOR_SEGMENT":       1404,
		"ENTITY_TYPE_SQLTEST_TEST":                  1421,
		"ENTITY_TYPE_POSTGRES_TABLE":                1603,
		"ENTITY_TYPE_POSTGRES_VIEW":                 1605,
		"ENTITY_TYPE_MYSQL_TABLE":                   1703,
		"ENTITY_TYPE_MYSQL_VIEW":                    1705,
		"ENTITY_TYPE_DATABRICKS_WAREHOUSE":          1801,
		"ENTITY_TYPE_DATABRICKS_TABLE":              1804,
		"ENTITY_TYPE_DATABRICKS_VIEW":               1805,
		"ENTITY_TYPE_DATABRICKS_JOB":                1807,
		"ENTITY_TYPE_DATABRICKS_NOTEBOOK":           1809,
		"ENTITY_TYPE_DATABRICKS_QUERY":              1810,
		"ENTITY_TYPE_DATABRICKS_DASHBOARD":          1811,
		"ENTITY_TYPE_SQLMESH_PROJECT":               1901,
		"ENTITY_TYPE_SQLMESH_SQL_MODEL":             1902,
		"ENTITY_TYPE_SQLMESH_PYTHON_MODEL":          1903,
		"ENTITY_TYPE_SQLMESH_EXTERNAL":              1904,
		"ENTITY_TYPE_SQLMESH_SEED":                  1905,
		"ENTITY_TYPE_SQLMESH_AUDIT":                 1906,
		"ENTITY_TYPE_SQLMESH_UNIT_TEST":             1907,
		"ENTITY_TYPE_SQLMESH_ENVIRONMENT":           1908,
		"ENTITY_TYPE_SQLMESH_SNAPSHOT":              1909,
		"ENTITY_TYPE_DUCKDB_TABLE":                  2003,
		"ENTITY_TYPE_DUCKDB_VIEW":                   2005,
		"ENTITY_TYPE_CUSTOM_ENTITY_GENERIC":         50000,
		"ENTITY_TYPE_CUSTOM_ENTITY_CUSTOM_TYPE_MIN": 50001,
		"ENTITY_TYPE_CUSTOM_ENTITY_CUSTOM_TYPE_MAX": 59999,
	}
)

func (x EntityType) Enum() *EntityType {
	p := new(EntityType)
	*p = x
	return p
}

func (x EntityType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (EntityType) Descriptor() protoreflect.EnumDescriptor {
	return file_synq_entities_v1_entity_types_proto_enumTypes[0].Descriptor()
}

func (EntityType) Type() protoreflect.EnumType {
	return &file_synq_entities_v1_entity_types_proto_enumTypes[0]
}

func (x EntityType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use EntityType.Descriptor instead.
func (EntityType) EnumDescriptor() ([]byte, []int) {
	return file_synq_entities_v1_entity_types_proto_rawDescGZIP(), []int{0}
}

var File_synq_entities_v1_entity_types_proto protoreflect.FileDescriptor

var file_synq_entities_v1_entity_types_proto_rawDesc = string([]byte{
	0x0a, 0x23, 0x73, 0x79, 0x6e, 0x71, 0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f,
	0x76, 0x31, 0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x73, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x10, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69,
	0x74, 0x69, 0x65, 0x73, 0x2e, 0x76, 0x31, 0x2a, 0x81, 0x10, 0x0a, 0x0a, 0x45, 0x6e, 0x74, 0x69,
	0x74, 0x79, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1b, 0x0a, 0x17, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59,
	0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45,
	0x44, 0x10, 0x00, 0x12, 0x18, 0x0a, 0x14, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59,
	0x50, 0x45, 0x5f, 0x42, 0x51, 0x5f, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x10, 0x67, 0x12, 0x17, 0x0a,
	0x13, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x42, 0x51, 0x5f,
	0x56, 0x49, 0x45, 0x57, 0x10, 0x69, 0x12, 0x1c, 0x0a, 0x17, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59,
	0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4c, 0x4f, 0x4f, 0x4b, 0x45, 0x52, 0x5f, 0x4c, 0x4f, 0x4f,
	0x4b, 0x10, 0xc9, 0x01, 0x12, 0x1f, 0x0a, 0x1a, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54,
	0x59, 0x50, 0x45, 0x5f, 0x4c, 0x4f, 0x4f, 0x4b, 0x45, 0x52, 0x5f, 0x45, 0x58, 0x50, 0x4c, 0x4f,
	0x52, 0x45, 0x10, 0xcb, 0x01, 0x12, 0x1c, 0x0a, 0x17, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f,
	0x54, 0x59, 0x50, 0x45, 0x5f, 0x4c, 0x4f, 0x4f, 0x4b, 0x45, 0x52, 0x5f, 0x56, 0x49, 0x45, 0x57,
	0x10, 0xcf, 0x01, 0x12, 0x21, 0x0a, 0x1c, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59,
	0x50, 0x45, 0x5f, 0x4c, 0x4f, 0x4f, 0x4b, 0x45, 0x52, 0x5f, 0x44, 0x41, 0x53, 0x48, 0x42, 0x4f,
	0x41, 0x52, 0x44, 0x10, 0xd0, 0x01, 0x12, 0x1a, 0x0a, 0x15, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59,
	0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x42, 0x54, 0x5f, 0x4d, 0x4f, 0x44, 0x45, 0x4c, 0x10,
	0xad, 0x02, 0x12, 0x19, 0x0a, 0x14, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50,
	0x45, 0x5f, 0x44, 0x42, 0x54, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x10, 0xae, 0x02, 0x12, 0x1b, 0x0a,
	0x16, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x42, 0x54,
	0x5f, 0x53, 0x4f, 0x55, 0x52, 0x43, 0x45, 0x10, 0xaf, 0x02, 0x12, 0x1c, 0x0a, 0x17, 0x45, 0x4e,
	0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x42, 0x54, 0x5f, 0x50, 0x52,
	0x4f, 0x4a, 0x45, 0x43, 0x54, 0x10, 0xb2, 0x02, 0x12, 0x1b, 0x0a, 0x16, 0x45, 0x4e, 0x54, 0x49,
	0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x42, 0x54, 0x5f, 0x4d, 0x45, 0x54, 0x52,
	0x49, 0x43, 0x10, 0xb3, 0x02, 0x12, 0x1d, 0x0a, 0x18, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f,
	0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x42, 0x54, 0x5f, 0x53, 0x4e, 0x41, 0x50, 0x53, 0x48, 0x4f,
	0x54, 0x10, 0xb6, 0x02, 0x12, 0x19, 0x0a, 0x14, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54,
	0x59, 0x50, 0x45, 0x5f, 0x44, 0x42, 0x54, 0x5f, 0x53, 0x45, 0x45, 0x44, 0x10, 0xb7, 0x02, 0x12,
	0x1d, 0x0a, 0x18, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44,
	0x42, 0x54, 0x5f, 0x41, 0x4e, 0x41, 0x4c, 0x59, 0x53, 0x49, 0x53, 0x10, 0xb8, 0x02, 0x12, 0x1d,
	0x0a, 0x18, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x42,
	0x54, 0x5f, 0x45, 0x58, 0x50, 0x4f, 0x53, 0x55, 0x52, 0x45, 0x10, 0xb9, 0x02, 0x12, 0x1a, 0x0a,
	0x15, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x42, 0x54,
	0x5f, 0x47, 0x52, 0x4f, 0x55, 0x50, 0x10, 0xba, 0x02, 0x12, 0x22, 0x0a, 0x1d, 0x45, 0x4e, 0x54,
	0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x42, 0x54, 0x5f, 0x43, 0x4c, 0x4f,
	0x55, 0x44, 0x5f, 0x50, 0x52, 0x4f, 0x4a, 0x45, 0x43, 0x54, 0x10, 0xe0, 0x02, 0x12, 0x1e, 0x0a,
	0x19, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x42, 0x54,
	0x5f, 0x43, 0x4c, 0x4f, 0x55, 0x44, 0x5f, 0x4a, 0x4f, 0x42, 0x10, 0xe1, 0x02, 0x12, 0x20, 0x0a,
	0x1b, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x53, 0x4e, 0x4f,
	0x57, 0x46, 0x4c, 0x41, 0x4b, 0x45, 0x5f, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x10, 0xf7, 0x03, 0x12,
	0x1f, 0x0a, 0x1a, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x53,
	0x4e, 0x4f, 0x57, 0x46, 0x4c, 0x41, 0x4b, 0x45, 0x5f, 0x56, 0x49, 0x45, 0x57, 0x10, 0xfc, 0x03,
	0x12, 0x1f, 0x0a, 0x1a, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f,
	0x52, 0x45, 0x44, 0x53, 0x48, 0x49, 0x46, 0x54, 0x5f, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x10, 0xa3,
	0x06, 0x12, 0x1e, 0x0a, 0x19, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45,
	0x5f, 0x52, 0x45, 0x44, 0x53, 0x48, 0x49, 0x46, 0x54, 0x5f, 0x56, 0x49, 0x45, 0x57, 0x10, 0xa5,
	0x06, 0x12, 0x21, 0x0a, 0x1c, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45,
	0x5f, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x41, 0x55, 0x5f, 0x45, 0x4d, 0x42, 0x45, 0x44, 0x44, 0x45,
	0x44, 0x10, 0xcd, 0x08, 0x12, 0x22, 0x0a, 0x1d, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54,
	0x59, 0x50, 0x45, 0x5f, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x41, 0x55, 0x5f, 0x50, 0x55, 0x42, 0x4c,
	0x49, 0x53, 0x48, 0x45, 0x44, 0x10, 0xce, 0x08, 0x12, 0x23, 0x0a, 0x1e, 0x45, 0x4e, 0x54, 0x49,
	0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x41, 0x55, 0x5f,
	0x43, 0x55, 0x53, 0x54, 0x4f, 0x4d, 0x5f, 0x53, 0x51, 0x4c, 0x10, 0xcf, 0x08, 0x12, 0x1e, 0x0a,
	0x19, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x54, 0x41, 0x42,
	0x4c, 0x45, 0x41, 0x55, 0x5f, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x10, 0xd0, 0x08, 0x12, 0x1e, 0x0a,
	0x19, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x54, 0x41, 0x42,
	0x4c, 0x45, 0x41, 0x55, 0x5f, 0x53, 0x48, 0x45, 0x45, 0x54, 0x10, 0xd1, 0x08, 0x12, 0x22, 0x0a,
	0x1d, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x54, 0x41, 0x42,
	0x4c, 0x45, 0x41, 0x55, 0x5f, 0x44, 0x41, 0x53, 0x48, 0x42, 0x4f, 0x41, 0x52, 0x44, 0x10, 0xd2,
	0x08, 0x12, 0x1c, 0x0a, 0x17, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45,
	0x5f, 0x41, 0x49, 0x52, 0x46, 0x4c, 0x4f, 0x57, 0x5f, 0x44, 0x41, 0x47, 0x10, 0xb1, 0x09, 0x12,
	0x1d, 0x0a, 0x18, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x41,
	0x49, 0x52, 0x46, 0x4c, 0x4f, 0x57, 0x5f, 0x54, 0x41, 0x53, 0x4b, 0x10, 0xb2, 0x09, 0x12, 0x21,
	0x0a, 0x1c, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x43, 0x4c,
	0x49, 0x43, 0x4b, 0x48, 0x4f, 0x55, 0x53, 0x45, 0x5f, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x10, 0x97,
	0x0a, 0x12, 0x20, 0x0a, 0x1b, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45,
	0x5f, 0x43, 0x4c, 0x49, 0x43, 0x4b, 0x48, 0x4f, 0x55, 0x53, 0x45, 0x5f, 0x56, 0x49, 0x45, 0x57,
	0x10, 0x99, 0x0a, 0x12, 0x20, 0x0a, 0x1b, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59,
	0x50, 0x45, 0x5f, 0x41, 0x4e, 0x4f, 0x4d, 0x41, 0x4c, 0x59, 0x5f, 0x4d, 0x4f, 0x4e, 0x49, 0x54,
	0x4f, 0x52, 0x10, 0xfb, 0x0a, 0x12, 0x28, 0x0a, 0x23, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f,
	0x54, 0x59, 0x50, 0x45, 0x5f, 0x41, 0x4e, 0x4f, 0x4d, 0x41, 0x4c, 0x59, 0x5f, 0x4d, 0x4f, 0x4e,
	0x49, 0x54, 0x4f, 0x52, 0x5f, 0x53, 0x45, 0x47, 0x4d, 0x45, 0x4e, 0x54, 0x10, 0xfc, 0x0a, 0x12,
	0x1d, 0x0a, 0x18, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x53,
	0x51, 0x4c, 0x54, 0x45, 0x53, 0x54, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x10, 0x8d, 0x0b, 0x12, 0x1f,
	0x0a, 0x1a, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x50, 0x4f,
	0x53, 0x54, 0x47, 0x52, 0x45, 0x53, 0x5f, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x10, 0xc3, 0x0c, 0x12,
	0x1e, 0x0a, 0x19, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x50,
	0x4f, 0x53, 0x54, 0x47, 0x52, 0x45, 0x53, 0x5f, 0x56, 0x49, 0x45, 0x57, 0x10, 0xc5, 0x0c, 0x12,
	0x1c, 0x0a, 0x17, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4d,
	0x59, 0x53, 0x51, 0x4c, 0x5f, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x10, 0xa7, 0x0d, 0x12, 0x1b, 0x0a,
	0x16, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x4d, 0x59, 0x53,
	0x51, 0x4c, 0x5f, 0x56, 0x49, 0x45, 0x57, 0x10, 0xa9, 0x0d, 0x12, 0x25, 0x0a, 0x20, 0x45, 0x4e,
	0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x41, 0x54, 0x41, 0x42, 0x52,
	0x49, 0x43, 0x4b, 0x53, 0x5f, 0x57, 0x41, 0x52, 0x45, 0x48, 0x4f, 0x55, 0x53, 0x45, 0x10, 0x89,
	0x0e, 0x12, 0x21, 0x0a, 0x1c, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45,
	0x5f, 0x44, 0x41, 0x54, 0x41, 0x42, 0x52, 0x49, 0x43, 0x4b, 0x53, 0x5f, 0x54, 0x41, 0x42, 0x4c,
	0x45, 0x10, 0x8c, 0x0e, 0x12, 0x20, 0x0a, 0x1b, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54,
	0x59, 0x50, 0x45, 0x5f, 0x44, 0x41, 0x54, 0x41, 0x42, 0x52, 0x49, 0x43, 0x4b, 0x53, 0x5f, 0x56,
	0x49, 0x45, 0x57, 0x10, 0x8d, 0x0e, 0x12, 0x1f, 0x0a, 0x1a, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59,
	0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x41, 0x54, 0x41, 0x42, 0x52, 0x49, 0x43, 0x4b, 0x53,
	0x5f, 0x4a, 0x4f, 0x42, 0x10, 0x8f, 0x0e, 0x12, 0x24, 0x0a, 0x1f, 0x45, 0x4e, 0x54, 0x49, 0x54,
	0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x41, 0x54, 0x41, 0x42, 0x52, 0x49, 0x43, 0x4b,
	0x53, 0x5f, 0x4e, 0x4f, 0x54, 0x45, 0x42, 0x4f, 0x4f, 0x4b, 0x10, 0x91, 0x0e, 0x12, 0x21, 0x0a,
	0x1c, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x44, 0x41, 0x54,
	0x41, 0x42, 0x52, 0x49, 0x43, 0x4b, 0x53, 0x5f, 0x51, 0x55, 0x45, 0x52, 0x59, 0x10, 0x92, 0x0e,
	0x12, 0x25, 0x0a, 0x20, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f,
	0x44, 0x41, 0x54, 0x41, 0x42, 0x52, 0x49, 0x43, 0x4b, 0x53, 0x5f, 0x44, 0x41, 0x53, 0x48, 0x42,
	0x4f, 0x41, 0x52, 0x44, 0x10, 0x93, 0x0e, 0x12, 0x20, 0x0a, 0x1b, 0x45, 0x4e, 0x54, 0x49, 0x54,
	0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x53, 0x51, 0x4c, 0x4d, 0x45, 0x53, 0x48, 0x5f, 0x50,
	0x52, 0x4f, 0x4a, 0x45, 0x43, 0x54, 0x10, 0xed, 0x0e, 0x12, 0x22, 0x0a, 0x1d, 0x45, 0x4e, 0x54,
	0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x53, 0x51, 0x4c, 0x4d, 0x45, 0x53, 0x48,
	0x5f, 0x53, 0x51, 0x4c, 0x5f, 0x4d, 0x4f, 0x44, 0x45, 0x4c, 0x10, 0xee, 0x0e, 0x12, 0x25, 0x0a,
	0x20, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x53, 0x51, 0x4c,
	0x4d, 0x45, 0x53, 0x48, 0x5f, 0x50, 0x59, 0x54, 0x48, 0x4f, 0x4e, 0x5f, 0x4d, 0x4f, 0x44, 0x45,
	0x4c, 0x10, 0xef, 0x0e, 0x12, 0x21, 0x0a, 0x1c, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54,
	0x59, 0x50, 0x45, 0x5f, 0x53, 0x51, 0x4c, 0x4d, 0x45, 0x53, 0x48, 0x5f, 0x45, 0x58, 0x54, 0x45,
	0x52, 0x4e, 0x41, 0x4c, 0x10, 0xf0, 0x0e, 0x12, 0x1d, 0x0a, 0x18, 0x45, 0x4e, 0x54, 0x49, 0x54,
	0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x53, 0x51, 0x4c, 0x4d, 0x45, 0x53, 0x48, 0x5f, 0x53,
	0x45, 0x45, 0x44, 0x10, 0xf1, 0x0e, 0x12, 0x1e, 0x0a, 0x19, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59,
	0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x53, 0x51, 0x4c, 0x4d, 0x45, 0x53, 0x48, 0x5f, 0x41, 0x55,
	0x44, 0x49, 0x54, 0x10, 0xf2, 0x0e, 0x12, 0x22, 0x0a, 0x1d, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59,
	0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x53, 0x51, 0x4c, 0x4d, 0x45, 0x53, 0x48, 0x5f, 0x55, 0x4e,
	0x49, 0x54, 0x5f, 0x54, 0x45, 0x53, 0x54, 0x10, 0xf3, 0x0e, 0x12, 0x24, 0x0a, 0x1f, 0x45, 0x4e,
	0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x53, 0x51, 0x4c, 0x4d, 0x45, 0x53,
	0x48, 0x5f, 0x45, 0x4e, 0x56, 0x49, 0x52, 0x4f, 0x4e, 0x4d, 0x45, 0x4e, 0x54, 0x10, 0xf4, 0x0e,
	0x12, 0x21, 0x0a, 0x1c, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f,
	0x53, 0x51, 0x4c, 0x4d, 0x45, 0x53, 0x48, 0x5f, 0x53, 0x4e, 0x41, 0x50, 0x53, 0x48, 0x4f, 0x54,
	0x10, 0xf5, 0x0e, 0x12, 0x1d, 0x0a, 0x18, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59,
	0x50, 0x45, 0x5f, 0x44, 0x55, 0x43, 0x4b, 0x44, 0x42, 0x5f, 0x54, 0x41, 0x42, 0x4c, 0x45, 0x10,
	0xd3, 0x0f, 0x12, 0x1c, 0x0a, 0x17, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50,
	0x45, 0x5f, 0x44, 0x55, 0x43, 0x4b, 0x44, 0x42, 0x5f, 0x56, 0x49, 0x45, 0x57, 0x10, 0xd5, 0x0f,
	0x12, 0x27, 0x0a, 0x21, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f,
	0x43, 0x55, 0x53, 0x54, 0x4f, 0x4d, 0x5f, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x47, 0x45,
	0x4e, 0x45, 0x52, 0x49, 0x43, 0x10, 0xd0, 0x86, 0x03, 0x12, 0x2f, 0x0a, 0x29, 0x45, 0x4e, 0x54,
	0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x43, 0x55, 0x53, 0x54, 0x4f, 0x4d, 0x5f,
	0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x43, 0x55, 0x53, 0x54, 0x4f, 0x4d, 0x5f, 0x54, 0x59,
	0x50, 0x45, 0x5f, 0x4d, 0x49, 0x4e, 0x10, 0xd1, 0x86, 0x03, 0x12, 0x2f, 0x0a, 0x29, 0x45, 0x4e,
	0x54, 0x49, 0x54, 0x59, 0x5f, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x43, 0x55, 0x53, 0x54, 0x4f, 0x4d,
	0x5f, 0x45, 0x4e, 0x54, 0x49, 0x54, 0x59, 0x5f, 0x43, 0x55, 0x53, 0x54, 0x4f, 0x4d, 0x5f, 0x54,
	0x59, 0x50, 0x45, 0x5f, 0x4d, 0x41, 0x58, 0x10, 0xdf, 0xd4, 0x03, 0x42, 0xae, 0x01, 0x0a, 0x14,
	0x63, 0x6f, 0x6d, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65,
	0x73, 0x2e, 0x76, 0x31, 0x42, 0x10, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x54, 0x79, 0x70, 0x65,
	0x73, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a, 0x22, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62,
	0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x65, 0x74, 0x73, 0x79, 0x6e, 0x71, 0x2f, 0x61, 0x70, 0x69,
	0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f, 0x76, 0x31, 0xa2, 0x02, 0x03, 0x53,
	0x45, 0x58, 0xaa, 0x02, 0x10, 0x53, 0x79, 0x6e, 0x71, 0x2e, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69,
	0x65, 0x73, 0x2e, 0x56, 0x31, 0xca, 0x02, 0x10, 0x53, 0x79, 0x6e, 0x71, 0x5c, 0x45, 0x6e, 0x74,
	0x69, 0x74, 0x69, 0x65, 0x73, 0x5c, 0x56, 0x31, 0xe2, 0x02, 0x1c, 0x53, 0x79, 0x6e, 0x71, 0x5c,
	0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x5c, 0x56, 0x31, 0x5c, 0x47, 0x50, 0x42, 0x4d,
	0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0xea, 0x02, 0x12, 0x53, 0x79, 0x6e, 0x71, 0x3a, 0x3a,
	0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x3a, 0x3a, 0x56, 0x31, 0x62, 0x06, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_synq_entities_v1_entity_types_proto_rawDescOnce sync.Once
	file_synq_entities_v1_entity_types_proto_rawDescData []byte
)

func file_synq_entities_v1_entity_types_proto_rawDescGZIP() []byte {
	file_synq_entities_v1_entity_types_proto_rawDescOnce.Do(func() {
		file_synq_entities_v1_entity_types_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_synq_entities_v1_entity_types_proto_rawDesc), len(file_synq_entities_v1_entity_types_proto_rawDesc)))
	})
	return file_synq_entities_v1_entity_types_proto_rawDescData
}

var file_synq_entities_v1_entity_types_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_synq_entities_v1_entity_types_proto_goTypes = []any{
	(EntityType)(0), // 0: synq.entities.v1.EntityType
}
var file_synq_entities_v1_entity_types_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_synq_entities_v1_entity_types_proto_init() }
func file_synq_entities_v1_entity_types_proto_init() {
	if File_synq_entities_v1_entity_types_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_synq_entities_v1_entity_types_proto_rawDesc), len(file_synq_entities_v1_entity_types_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_synq_entities_v1_entity_types_proto_goTypes,
		DependencyIndexes: file_synq_entities_v1_entity_types_proto_depIdxs,
		EnumInfos:         file_synq_entities_v1_entity_types_proto_enumTypes,
	}.Build()
	File_synq_entities_v1_entity_types_proto = out.File
	file_synq_entities_v1_entity_types_proto_goTypes = nil
	file_synq_entities_v1_entity_types_proto_depIdxs = nil
}
