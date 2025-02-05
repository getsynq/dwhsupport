// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.4
// 	protoc        (unknown)
// source: synq/ingest/dwh/v1/dwh.proto

package v1

import (
	_ "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
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

type Fqn struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Instance      string                 `protobuf:"bytes,1,opt,name=instance,proto3" json:"instance,omitempty"`
	Database      string                 `protobuf:"bytes,2,opt,name=database,proto3" json:"database,omitempty"`
	Schema        string                 `protobuf:"bytes,3,opt,name=schema,proto3" json:"schema,omitempty"`
	Table         string                 `protobuf:"bytes,4,opt,name=table,proto3" json:"table,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Fqn) Reset() {
	*x = Fqn{}
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Fqn) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Fqn) ProtoMessage() {}

func (x *Fqn) ProtoReflect() protoreflect.Message {
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Fqn.ProtoReflect.Descriptor instead.
func (*Fqn) Descriptor() ([]byte, []int) {
	return file_synq_ingest_dwh_v1_dwh_proto_rawDescGZIP(), []int{0}
}

func (x *Fqn) GetInstance() string {
	if x != nil {
		return x.Instance
	}
	return ""
}

func (x *Fqn) GetDatabase() string {
	if x != nil {
		return x.Database
	}
	return ""
}

func (x *Fqn) GetSchema() string {
	if x != nil {
		return x.Schema
	}
	return ""
}

func (x *Fqn) GetTable() string {
	if x != nil {
		return x.Table
	}
	return ""
}

type TableInfo struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Fqn           *Fqn                   `protobuf:"bytes,1,opt,name=fqn,proto3" json:"fqn,omitempty"`
	TableType     string                 `protobuf:"bytes,2,opt,name=table_type,json=tableType,proto3" json:"table_type,omitempty"`
	IsView        bool                   `protobuf:"varint,3,opt,name=is_view,json=isView,proto3" json:"is_view,omitempty"`
	TableTags     []*Tag                 `protobuf:"bytes,4,rep,name=table_tags,json=tableTags,proto3" json:"table_tags,omitempty"`
	Description   *string                `protobuf:"bytes,5,opt,name=description,proto3,oneof" json:"description,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TableInfo) Reset() {
	*x = TableInfo{}
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TableInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TableInfo) ProtoMessage() {}

func (x *TableInfo) ProtoReflect() protoreflect.Message {
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TableInfo.ProtoReflect.Descriptor instead.
func (*TableInfo) Descriptor() ([]byte, []int) {
	return file_synq_ingest_dwh_v1_dwh_proto_rawDescGZIP(), []int{1}
}

func (x *TableInfo) GetFqn() *Fqn {
	if x != nil {
		return x.Fqn
	}
	return nil
}

func (x *TableInfo) GetTableType() string {
	if x != nil {
		return x.TableType
	}
	return ""
}

func (x *TableInfo) GetIsView() bool {
	if x != nil {
		return x.IsView
	}
	return false
}

func (x *TableInfo) GetTableTags() []*Tag {
	if x != nil {
		return x.TableTags
	}
	return nil
}

func (x *TableInfo) GetDescription() string {
	if x != nil && x.Description != nil {
		return *x.Description
	}
	return ""
}

type SqlDefinition struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Fqn           *Fqn                   `protobuf:"bytes,1,opt,name=fqn,proto3" json:"fqn,omitempty"`
	Sql           string                 `protobuf:"bytes,2,opt,name=sql,proto3" json:"sql,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *SqlDefinition) Reset() {
	*x = SqlDefinition{}
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SqlDefinition) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SqlDefinition) ProtoMessage() {}

func (x *SqlDefinition) ProtoReflect() protoreflect.Message {
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SqlDefinition.ProtoReflect.Descriptor instead.
func (*SqlDefinition) Descriptor() ([]byte, []int) {
	return file_synq_ingest_dwh_v1_dwh_proto_rawDescGZIP(), []int{2}
}

func (x *SqlDefinition) GetFqn() *Fqn {
	if x != nil {
		return x.Fqn
	}
	return nil
}

func (x *SqlDefinition) GetSql() string {
	if x != nil {
		return x.Sql
	}
	return ""
}

type Schema struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Fqn           *Fqn                   `protobuf:"bytes,1,opt,name=fqn,proto3" json:"fqn,omitempty"`
	Columns       []*SchemaColumn        `protobuf:"bytes,2,rep,name=columns,proto3" json:"columns,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Schema) Reset() {
	*x = Schema{}
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Schema) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Schema) ProtoMessage() {}

func (x *Schema) ProtoReflect() protoreflect.Message {
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Schema.ProtoReflect.Descriptor instead.
func (*Schema) Descriptor() ([]byte, []int) {
	return file_synq_ingest_dwh_v1_dwh_proto_rawDescGZIP(), []int{3}
}

func (x *Schema) GetFqn() *Fqn {
	if x != nil {
		return x.Fqn
	}
	return nil
}

func (x *Schema) GetColumns() []*SchemaColumn {
	if x != nil {
		return x.Columns
	}
	return nil
}

type SchemaColumn struct {
	state           protoimpl.MessageState `protogen:"open.v1"`
	Name            string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	NativeType      string                 `protobuf:"bytes,2,opt,name=native_type,json=nativeType,proto3" json:"native_type,omitempty"`
	OrdinalPosition int32                  `protobuf:"varint,3,opt,name=ordinal_position,json=ordinalPosition,proto3" json:"ordinal_position,omitempty"`
	Description     *string                `protobuf:"bytes,4,opt,name=description,proto3,oneof" json:"description,omitempty"`
	ColumnTags      []*Tag                 `protobuf:"bytes,5,rep,name=column_tags,json=columnTags,proto3" json:"column_tags,omitempty"`
	IsStructColumn  bool                   `protobuf:"varint,6,opt,name=is_struct_column,json=isStructColumn,proto3" json:"is_struct_column,omitempty"`
	IsArrayColumn   bool                   `protobuf:"varint,7,opt,name=is_array_column,json=isArrayColumn,proto3" json:"is_array_column,omitempty"`
	FieldSchemas    []*SchemaColumnField   `protobuf:"bytes,8,rep,name=field_schemas,json=fieldSchemas,proto3" json:"field_schemas,omitempty"`
	unknownFields   protoimpl.UnknownFields
	sizeCache       protoimpl.SizeCache
}

func (x *SchemaColumn) Reset() {
	*x = SchemaColumn{}
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SchemaColumn) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SchemaColumn) ProtoMessage() {}

func (x *SchemaColumn) ProtoReflect() protoreflect.Message {
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SchemaColumn.ProtoReflect.Descriptor instead.
func (*SchemaColumn) Descriptor() ([]byte, []int) {
	return file_synq_ingest_dwh_v1_dwh_proto_rawDescGZIP(), []int{4}
}

func (x *SchemaColumn) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *SchemaColumn) GetNativeType() string {
	if x != nil {
		return x.NativeType
	}
	return ""
}

func (x *SchemaColumn) GetOrdinalPosition() int32 {
	if x != nil {
		return x.OrdinalPosition
	}
	return 0
}

func (x *SchemaColumn) GetDescription() string {
	if x != nil && x.Description != nil {
		return *x.Description
	}
	return ""
}

func (x *SchemaColumn) GetColumnTags() []*Tag {
	if x != nil {
		return x.ColumnTags
	}
	return nil
}

func (x *SchemaColumn) GetIsStructColumn() bool {
	if x != nil {
		return x.IsStructColumn
	}
	return false
}

func (x *SchemaColumn) GetIsArrayColumn() bool {
	if x != nil {
		return x.IsArrayColumn
	}
	return false
}

func (x *SchemaColumn) GetFieldSchemas() []*SchemaColumnField {
	if x != nil {
		return x.FieldSchemas
	}
	return nil
}

type SchemaColumnField struct {
	state           protoimpl.MessageState `protogen:"open.v1"`
	Name            string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	NativeType      string                 `protobuf:"bytes,2,opt,name=native_type,json=nativeType,proto3" json:"native_type,omitempty"`
	Description     string                 `protobuf:"bytes,3,opt,name=description,proto3" json:"description,omitempty"`
	OrdinalPosition int32                  `protobuf:"varint,4,opt,name=ordinal_position,json=ordinalPosition,proto3" json:"ordinal_position,omitempty"`
	IsStruct        bool                   `protobuf:"varint,5,opt,name=is_struct,json=isStruct,proto3" json:"is_struct,omitempty"`
	IsRepeated      bool                   `protobuf:"varint,6,opt,name=is_repeated,json=isRepeated,proto3" json:"is_repeated,omitempty"`
	Fields          []*SchemaColumnField   `protobuf:"bytes,7,rep,name=fields,proto3" json:"fields,omitempty"`
	unknownFields   protoimpl.UnknownFields
	sizeCache       protoimpl.SizeCache
}

func (x *SchemaColumnField) Reset() {
	*x = SchemaColumnField{}
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SchemaColumnField) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SchemaColumnField) ProtoMessage() {}

func (x *SchemaColumnField) ProtoReflect() protoreflect.Message {
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SchemaColumnField.ProtoReflect.Descriptor instead.
func (*SchemaColumnField) Descriptor() ([]byte, []int) {
	return file_synq_ingest_dwh_v1_dwh_proto_rawDescGZIP(), []int{5}
}

func (x *SchemaColumnField) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *SchemaColumnField) GetNativeType() string {
	if x != nil {
		return x.NativeType
	}
	return ""
}

func (x *SchemaColumnField) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *SchemaColumnField) GetOrdinalPosition() int32 {
	if x != nil {
		return x.OrdinalPosition
	}
	return 0
}

func (x *SchemaColumnField) GetIsStruct() bool {
	if x != nil {
		return x.IsStruct
	}
	return false
}

func (x *SchemaColumnField) GetIsRepeated() bool {
	if x != nil {
		return x.IsRepeated
	}
	return false
}

func (x *SchemaColumnField) GetFields() []*SchemaColumnField {
	if x != nil {
		return x.Fields
	}
	return nil
}

type TableMetrics struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Fqn           *Fqn                   `protobuf:"bytes,1,opt,name=fqn,proto3" json:"fqn,omitempty"`
	RowCount      *int64                 `protobuf:"varint,2,opt,name=row_count,json=rowCount,proto3,oneof" json:"row_count,omitempty"`
	UpdatedAt     *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=updated_at,json=updatedAt,proto3,oneof" json:"updated_at,omitempty"`
	SizeBytes     *int64                 `protobuf:"varint,4,opt,name=size_bytes,json=sizeBytes,proto3,oneof" json:"size_bytes,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TableMetrics) Reset() {
	*x = TableMetrics{}
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TableMetrics) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TableMetrics) ProtoMessage() {}

func (x *TableMetrics) ProtoReflect() protoreflect.Message {
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TableMetrics.ProtoReflect.Descriptor instead.
func (*TableMetrics) Descriptor() ([]byte, []int) {
	return file_synq_ingest_dwh_v1_dwh_proto_rawDescGZIP(), []int{6}
}

func (x *TableMetrics) GetFqn() *Fqn {
	if x != nil {
		return x.Fqn
	}
	return nil
}

func (x *TableMetrics) GetRowCount() int64 {
	if x != nil && x.RowCount != nil {
		return *x.RowCount
	}
	return 0
}

func (x *TableMetrics) GetUpdatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.UpdatedAt
	}
	return nil
}

func (x *TableMetrics) GetSizeBytes() int64 {
	if x != nil && x.SizeBytes != nil {
		return *x.SizeBytes
	}
	return 0
}

type Tag struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	TagName       string                 `protobuf:"bytes,1,opt,name=tag_name,json=tagName,proto3" json:"tag_name,omitempty"`
	TagValue      string                 `protobuf:"bytes,2,opt,name=tag_value,json=tagValue,proto3" json:"tag_value,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Tag) Reset() {
	*x = Tag{}
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Tag) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Tag) ProtoMessage() {}

func (x *Tag) ProtoReflect() protoreflect.Message {
	mi := &file_synq_ingest_dwh_v1_dwh_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Tag.ProtoReflect.Descriptor instead.
func (*Tag) Descriptor() ([]byte, []int) {
	return file_synq_ingest_dwh_v1_dwh_proto_rawDescGZIP(), []int{7}
}

func (x *Tag) GetTagName() string {
	if x != nil {
		return x.TagName
	}
	return ""
}

func (x *Tag) GetTagValue() string {
	if x != nil {
		return x.TagValue
	}
	return ""
}

var File_synq_ingest_dwh_v1_dwh_proto protoreflect.FileDescriptor

var file_synq_ingest_dwh_v1_dwh_proto_rawDesc = string([]byte{
	0x0a, 0x1c, 0x73, 0x79, 0x6e, 0x71, 0x2f, 0x69, 0x6e, 0x67, 0x65, 0x73, 0x74, 0x2f, 0x64, 0x77,
	0x68, 0x2f, 0x76, 0x31, 0x2f, 0x64, 0x77, 0x68, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x12,
	0x73, 0x79, 0x6e, 0x71, 0x2e, 0x69, 0x6e, 0x67, 0x65, 0x73, 0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e,
	0x76, 0x31, 0x1a, 0x1b, 0x62, 0x75, 0x66, 0x2f, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x65,
	0x2f, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0x7b, 0x0a, 0x03, 0x46, 0x71, 0x6e, 0x12, 0x1a, 0x0a, 0x08, 0x69, 0x6e, 0x73, 0x74, 0x61,
	0x6e, 0x63, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x69, 0x6e, 0x73, 0x74, 0x61,
	0x6e, 0x63, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x12,
	0x1e, 0x0a, 0x06, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x42,
	0x06, 0xba, 0x48, 0x03, 0xc8, 0x01, 0x01, 0x52, 0x06, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x12,
	0x1c, 0x0a, 0x05, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x42, 0x06,
	0xba, 0x48, 0x03, 0xc8, 0x01, 0x01, 0x52, 0x05, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x22, 0xdd, 0x01,
	0x0a, 0x09, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x29, 0x0a, 0x03, 0x66,
	0x71, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e,
	0x69, 0x6e, 0x67, 0x65, 0x73, 0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x46, 0x71,
	0x6e, 0x52, 0x03, 0x66, 0x71, 0x6e, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f,
	0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x61, 0x62, 0x6c,
	0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x17, 0x0a, 0x07, 0x69, 0x73, 0x5f, 0x76, 0x69, 0x65, 0x77,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x08, 0x52, 0x06, 0x69, 0x73, 0x56, 0x69, 0x65, 0x77, 0x12, 0x36,
	0x0a, 0x0a, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x74, 0x61, 0x67, 0x73, 0x18, 0x04, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x17, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x69, 0x6e, 0x67, 0x65, 0x73, 0x74,
	0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x61, 0x67, 0x52, 0x09, 0x74, 0x61, 0x62,
	0x6c, 0x65, 0x54, 0x61, 0x67, 0x73, 0x12, 0x25, 0x0a, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69,
	0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x0b, 0x64,
	0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x88, 0x01, 0x01, 0x42, 0x0e, 0x0a,
	0x0c, 0x5f, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x4c, 0x0a,
	0x0d, 0x53, 0x71, 0x6c, 0x44, 0x65, 0x66, 0x69, 0x6e, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x29,
	0x0a, 0x03, 0x66, 0x71, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x73, 0x79,
	0x6e, 0x71, 0x2e, 0x69, 0x6e, 0x67, 0x65, 0x73, 0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31,
	0x2e, 0x46, 0x71, 0x6e, 0x52, 0x03, 0x66, 0x71, 0x6e, 0x12, 0x10, 0x0a, 0x03, 0x73, 0x71, 0x6c,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x73, 0x71, 0x6c, 0x22, 0x6f, 0x0a, 0x06, 0x53,
	0x63, 0x68, 0x65, 0x6d, 0x61, 0x12, 0x29, 0x0a, 0x03, 0x66, 0x71, 0x6e, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x17, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x69, 0x6e, 0x67, 0x65, 0x73, 0x74,
	0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x46, 0x71, 0x6e, 0x52, 0x03, 0x66, 0x71, 0x6e,
	0x12, 0x3a, 0x0a, 0x07, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x20, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x69, 0x6e, 0x67, 0x65, 0x73, 0x74, 0x2e,
	0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x43, 0x6f, 0x6c,
	0x75, 0x6d, 0x6e, 0x52, 0x07, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73, 0x22, 0xfd, 0x02, 0x0a,
	0x0c, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x12, 0x12, 0x0a,
	0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d,
	0x65, 0x12, 0x1f, 0x0a, 0x0b, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f, 0x74, 0x79, 0x70, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x54, 0x79,
	0x70, 0x65, 0x12, 0x29, 0x0a, 0x10, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x6c, 0x5f, 0x70, 0x6f,
	0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x52, 0x0f, 0x6f, 0x72,
	0x64, 0x69, 0x6e, 0x61, 0x6c, 0x50, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x25, 0x0a,
	0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x09, 0x48, 0x00, 0x52, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f,
	0x6e, 0x88, 0x01, 0x01, 0x12, 0x38, 0x0a, 0x0b, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x5f, 0x74,
	0x61, 0x67, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x73, 0x79, 0x6e, 0x71,
	0x2e, 0x69, 0x6e, 0x67, 0x65, 0x73, 0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x54,
	0x61, 0x67, 0x52, 0x0a, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x54, 0x61, 0x67, 0x73, 0x12, 0x28,
	0x0a, 0x10, 0x69, 0x73, 0x5f, 0x73, 0x74, 0x72, 0x75, 0x63, 0x74, 0x5f, 0x63, 0x6f, 0x6c, 0x75,
	0x6d, 0x6e, 0x18, 0x06, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0e, 0x69, 0x73, 0x53, 0x74, 0x72, 0x75,
	0x63, 0x74, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x12, 0x26, 0x0a, 0x0f, 0x69, 0x73, 0x5f, 0x61,
	0x72, 0x72, 0x61, 0x79, 0x5f, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x18, 0x07, 0x20, 0x01, 0x28,
	0x08, 0x52, 0x0d, 0x69, 0x73, 0x41, 0x72, 0x72, 0x61, 0x79, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e,
	0x12, 0x4a, 0x0a, 0x0d, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61,
	0x73, 0x18, 0x08, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x25, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x69,
	0x6e, 0x67, 0x65, 0x73, 0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x63, 0x68,
	0x65, 0x6d, 0x61, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x52, 0x0c,
	0x66, 0x69, 0x65, 0x6c, 0x64, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x73, 0x42, 0x0e, 0x0a, 0x0c,
	0x5f, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x92, 0x02, 0x0a,
	0x11, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x46, 0x69, 0x65,
	0x6c, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1f, 0x0a, 0x0b, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65,
	0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x6e, 0x61, 0x74,
	0x69, 0x76, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x20, 0x0a, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72,
	0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x64, 0x65,
	0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x29, 0x0a, 0x10, 0x6f, 0x72, 0x64,
	0x69, 0x6e, 0x61, 0x6c, 0x5f, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x05, 0x52, 0x0f, 0x6f, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x6c, 0x50, 0x6f, 0x73, 0x69,
	0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1b, 0x0a, 0x09, 0x69, 0x73, 0x5f, 0x73, 0x74, 0x72, 0x75, 0x63,
	0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x08, 0x52, 0x08, 0x69, 0x73, 0x53, 0x74, 0x72, 0x75, 0x63,
	0x74, 0x12, 0x1f, 0x0a, 0x0b, 0x69, 0x73, 0x5f, 0x72, 0x65, 0x70, 0x65, 0x61, 0x74, 0x65, 0x64,
	0x18, 0x06, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0a, 0x69, 0x73, 0x52, 0x65, 0x70, 0x65, 0x61, 0x74,
	0x65, 0x64, 0x12, 0x3d, 0x0a, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x18, 0x07, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x25, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x69, 0x6e, 0x67, 0x65, 0x73, 0x74,
	0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x43, 0x6f,
	0x6c, 0x75, 0x6d, 0x6e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x52, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64,
	0x73, 0x22, 0xeb, 0x01, 0x0a, 0x0c, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x4d, 0x65, 0x74, 0x72, 0x69,
	0x63, 0x73, 0x12, 0x29, 0x0a, 0x03, 0x66, 0x71, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x17, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x69, 0x6e, 0x67, 0x65, 0x73, 0x74, 0x2e, 0x64, 0x77,
	0x68, 0x2e, 0x76, 0x31, 0x2e, 0x46, 0x71, 0x6e, 0x52, 0x03, 0x66, 0x71, 0x6e, 0x12, 0x20, 0x0a,
	0x09, 0x72, 0x6f, 0x77, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03,
	0x48, 0x00, 0x52, 0x08, 0x72, 0x6f, 0x77, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x88, 0x01, 0x01, 0x12,
	0x3e, 0x0a, 0x0a, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x48,
	0x01, 0x52, 0x09, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x64, 0x41, 0x74, 0x88, 0x01, 0x01, 0x12,
	0x22, 0x0a, 0x0a, 0x73, 0x69, 0x7a, 0x65, 0x5f, 0x62, 0x79, 0x74, 0x65, 0x73, 0x18, 0x04, 0x20,
	0x01, 0x28, 0x03, 0x48, 0x02, 0x52, 0x09, 0x73, 0x69, 0x7a, 0x65, 0x42, 0x79, 0x74, 0x65, 0x73,
	0x88, 0x01, 0x01, 0x42, 0x0c, 0x0a, 0x0a, 0x5f, 0x72, 0x6f, 0x77, 0x5f, 0x63, 0x6f, 0x75, 0x6e,
	0x74, 0x42, 0x0d, 0x0a, 0x0b, 0x5f, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74,
	0x42, 0x0d, 0x0a, 0x0b, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x5f, 0x62, 0x79, 0x74, 0x65, 0x73, 0x22,
	0x3d, 0x0a, 0x03, 0x54, 0x61, 0x67, 0x12, 0x19, 0x0a, 0x08, 0x74, 0x61, 0x67, 0x5f, 0x6e, 0x61,
	0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x74, 0x61, 0x67, 0x4e, 0x61, 0x6d,
	0x65, 0x12, 0x1b, 0x0a, 0x09, 0x74, 0x61, 0x67, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x74, 0x61, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x42, 0xb3,
	0x01, 0x0a, 0x16, 0x63, 0x6f, 0x6d, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x69, 0x6e, 0x67, 0x65,
	0x73, 0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x42, 0x08, 0x44, 0x77, 0x68, 0x50, 0x72,
	0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a, 0x24, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x67, 0x65, 0x74, 0x73, 0x79, 0x6e, 0x71, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x69, 0x6e,
	0x67, 0x65, 0x73, 0x74, 0x2f, 0x64, 0x77, 0x68, 0x2f, 0x76, 0x31, 0xa2, 0x02, 0x03, 0x53, 0x49,
	0x44, 0xaa, 0x02, 0x12, 0x53, 0x79, 0x6e, 0x71, 0x2e, 0x49, 0x6e, 0x67, 0x65, 0x73, 0x74, 0x2e,
	0x44, 0x77, 0x68, 0x2e, 0x56, 0x31, 0xca, 0x02, 0x12, 0x53, 0x79, 0x6e, 0x71, 0x5c, 0x49, 0x6e,
	0x67, 0x65, 0x73, 0x74, 0x5c, 0x44, 0x77, 0x68, 0x5c, 0x56, 0x31, 0xe2, 0x02, 0x1e, 0x53, 0x79,
	0x6e, 0x71, 0x5c, 0x49, 0x6e, 0x67, 0x65, 0x73, 0x74, 0x5c, 0x44, 0x77, 0x68, 0x5c, 0x56, 0x31,
	0x5c, 0x47, 0x50, 0x42, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0xea, 0x02, 0x15, 0x53,
	0x79, 0x6e, 0x71, 0x3a, 0x3a, 0x49, 0x6e, 0x67, 0x65, 0x73, 0x74, 0x3a, 0x3a, 0x44, 0x77, 0x68,
	0x3a, 0x3a, 0x56, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_synq_ingest_dwh_v1_dwh_proto_rawDescOnce sync.Once
	file_synq_ingest_dwh_v1_dwh_proto_rawDescData []byte
)

func file_synq_ingest_dwh_v1_dwh_proto_rawDescGZIP() []byte {
	file_synq_ingest_dwh_v1_dwh_proto_rawDescOnce.Do(func() {
		file_synq_ingest_dwh_v1_dwh_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_synq_ingest_dwh_v1_dwh_proto_rawDesc), len(file_synq_ingest_dwh_v1_dwh_proto_rawDesc)))
	})
	return file_synq_ingest_dwh_v1_dwh_proto_rawDescData
}

var file_synq_ingest_dwh_v1_dwh_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_synq_ingest_dwh_v1_dwh_proto_goTypes = []any{
	(*Fqn)(nil),                   // 0: synq.ingest.dwh.v1.Fqn
	(*TableInfo)(nil),             // 1: synq.ingest.dwh.v1.TableInfo
	(*SqlDefinition)(nil),         // 2: synq.ingest.dwh.v1.SqlDefinition
	(*Schema)(nil),                // 3: synq.ingest.dwh.v1.Schema
	(*SchemaColumn)(nil),          // 4: synq.ingest.dwh.v1.SchemaColumn
	(*SchemaColumnField)(nil),     // 5: synq.ingest.dwh.v1.SchemaColumnField
	(*TableMetrics)(nil),          // 6: synq.ingest.dwh.v1.TableMetrics
	(*Tag)(nil),                   // 7: synq.ingest.dwh.v1.Tag
	(*timestamppb.Timestamp)(nil), // 8: google.protobuf.Timestamp
}
var file_synq_ingest_dwh_v1_dwh_proto_depIdxs = []int32{
	0,  // 0: synq.ingest.dwh.v1.TableInfo.fqn:type_name -> synq.ingest.dwh.v1.Fqn
	7,  // 1: synq.ingest.dwh.v1.TableInfo.table_tags:type_name -> synq.ingest.dwh.v1.Tag
	0,  // 2: synq.ingest.dwh.v1.SqlDefinition.fqn:type_name -> synq.ingest.dwh.v1.Fqn
	0,  // 3: synq.ingest.dwh.v1.Schema.fqn:type_name -> synq.ingest.dwh.v1.Fqn
	4,  // 4: synq.ingest.dwh.v1.Schema.columns:type_name -> synq.ingest.dwh.v1.SchemaColumn
	7,  // 5: synq.ingest.dwh.v1.SchemaColumn.column_tags:type_name -> synq.ingest.dwh.v1.Tag
	5,  // 6: synq.ingest.dwh.v1.SchemaColumn.field_schemas:type_name -> synq.ingest.dwh.v1.SchemaColumnField
	5,  // 7: synq.ingest.dwh.v1.SchemaColumnField.fields:type_name -> synq.ingest.dwh.v1.SchemaColumnField
	0,  // 8: synq.ingest.dwh.v1.TableMetrics.fqn:type_name -> synq.ingest.dwh.v1.Fqn
	8,  // 9: synq.ingest.dwh.v1.TableMetrics.updated_at:type_name -> google.protobuf.Timestamp
	10, // [10:10] is the sub-list for method output_type
	10, // [10:10] is the sub-list for method input_type
	10, // [10:10] is the sub-list for extension type_name
	10, // [10:10] is the sub-list for extension extendee
	0,  // [0:10] is the sub-list for field type_name
}

func init() { file_synq_ingest_dwh_v1_dwh_proto_init() }
func file_synq_ingest_dwh_v1_dwh_proto_init() {
	if File_synq_ingest_dwh_v1_dwh_proto != nil {
		return
	}
	file_synq_ingest_dwh_v1_dwh_proto_msgTypes[1].OneofWrappers = []any{}
	file_synq_ingest_dwh_v1_dwh_proto_msgTypes[4].OneofWrappers = []any{}
	file_synq_ingest_dwh_v1_dwh_proto_msgTypes[6].OneofWrappers = []any{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_synq_ingest_dwh_v1_dwh_proto_rawDesc), len(file_synq_ingest_dwh_v1_dwh_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_synq_ingest_dwh_v1_dwh_proto_goTypes,
		DependencyIndexes: file_synq_ingest_dwh_v1_dwh_proto_depIdxs,
		MessageInfos:      file_synq_ingest_dwh_v1_dwh_proto_msgTypes,
	}.Build()
	File_synq_ingest_dwh_v1_dwh_proto = out.File
	file_synq_ingest_dwh_v1_dwh_proto_goTypes = nil
	file_synq_ingest_dwh_v1_dwh_proto_depIdxs = nil
}
