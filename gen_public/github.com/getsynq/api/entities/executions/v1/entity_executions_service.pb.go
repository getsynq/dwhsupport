// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.4
// 	protoc        (unknown)
// source: synq/entities/executions/v1/entity_executions_service.proto

package v1

import (
	_ "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	v1 "github.com/getsynq/api/entities/v1"
	_ "github.com/getsynq/api/v1"
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

type ExecutionStatus int32

const (
	ExecutionStatus_EXECUTION_STATUS_UNSPECIFIED ExecutionStatus = 0
	ExecutionStatus_EXECUTION_STATUS_OK          ExecutionStatus = 1
	ExecutionStatus_EXECUTION_STATUS_WARN        ExecutionStatus = 2
	ExecutionStatus_EXECUTION_STATUS_ERROR       ExecutionStatus = 3
	ExecutionStatus_EXECUTION_STATUS_CRITICAL    ExecutionStatus = 4
)

// Enum value maps for ExecutionStatus.
var (
	ExecutionStatus_name = map[int32]string{
		0: "EXECUTION_STATUS_UNSPECIFIED",
		1: "EXECUTION_STATUS_OK",
		2: "EXECUTION_STATUS_WARN",
		3: "EXECUTION_STATUS_ERROR",
		4: "EXECUTION_STATUS_CRITICAL",
	}
	ExecutionStatus_value = map[string]int32{
		"EXECUTION_STATUS_UNSPECIFIED": 0,
		"EXECUTION_STATUS_OK":          1,
		"EXECUTION_STATUS_WARN":        2,
		"EXECUTION_STATUS_ERROR":       3,
		"EXECUTION_STATUS_CRITICAL":    4,
	}
)

func (x ExecutionStatus) Enum() *ExecutionStatus {
	p := new(ExecutionStatus)
	*p = x
	return p
}

func (x ExecutionStatus) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ExecutionStatus) Descriptor() protoreflect.EnumDescriptor {
	return file_synq_entities_executions_v1_entity_executions_service_proto_enumTypes[0].Descriptor()
}

func (ExecutionStatus) Type() protoreflect.EnumType {
	return &file_synq_entities_executions_v1_entity_executions_service_proto_enumTypes[0]
}

func (x ExecutionStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ExecutionStatus.Descriptor instead.
func (ExecutionStatus) EnumDescriptor() ([]byte, []int) {
	return file_synq_entities_executions_v1_entity_executions_service_proto_rawDescGZIP(), []int{0}
}

type UpsertExecutionRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Execution     *Execution             `protobuf:"bytes,1,opt,name=execution,proto3" json:"execution,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpsertExecutionRequest) Reset() {
	*x = UpsertExecutionRequest{}
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpsertExecutionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpsertExecutionRequest) ProtoMessage() {}

func (x *UpsertExecutionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpsertExecutionRequest.ProtoReflect.Descriptor instead.
func (*UpsertExecutionRequest) Descriptor() ([]byte, []int) {
	return file_synq_entities_executions_v1_entity_executions_service_proto_rawDescGZIP(), []int{0}
}

func (x *UpsertExecutionRequest) GetExecution() *Execution {
	if x != nil {
		return x.Execution
	}
	return nil
}

type UpsertExecutionResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpsertExecutionResponse) Reset() {
	*x = UpsertExecutionResponse{}
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpsertExecutionResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpsertExecutionResponse) ProtoMessage() {}

func (x *UpsertExecutionResponse) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpsertExecutionResponse.ProtoReflect.Descriptor instead.
func (*UpsertExecutionResponse) Descriptor() ([]byte, []int) {
	return file_synq_entities_executions_v1_entity_executions_service_proto_rawDescGZIP(), []int{1}
}

type Execution struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            *v1.Identifier         `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Status        ExecutionStatus        `protobuf:"varint,2,opt,name=status,proto3,enum=synq.entities.executions.v1.ExecutionStatus" json:"status,omitempty"`
	Message       string                 `protobuf:"bytes,3,opt,name=message,proto3" json:"message,omitempty"`
	CreatedAt     *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	StartedAt     *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=started_at,json=startedAt,proto3" json:"started_at,omitempty"`
	FinishedAt    *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=finished_at,json=finishedAt,proto3" json:"finished_at,omitempty"`
	Annotations   []*v1.Annotation       `protobuf:"bytes,7,rep,name=annotations,proto3" json:"annotations,omitempty"`
	Extras        []*ExecutionExtra      `protobuf:"bytes,8,rep,name=extras,proto3" json:"extras,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Execution) Reset() {
	*x = Execution{}
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Execution) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Execution) ProtoMessage() {}

func (x *Execution) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Execution.ProtoReflect.Descriptor instead.
func (*Execution) Descriptor() ([]byte, []int) {
	return file_synq_entities_executions_v1_entity_executions_service_proto_rawDescGZIP(), []int{2}
}

func (x *Execution) GetId() *v1.Identifier {
	if x != nil {
		return x.Id
	}
	return nil
}

func (x *Execution) GetStatus() ExecutionStatus {
	if x != nil {
		return x.Status
	}
	return ExecutionStatus_EXECUTION_STATUS_UNSPECIFIED
}

func (x *Execution) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

func (x *Execution) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

func (x *Execution) GetStartedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.StartedAt
	}
	return nil
}

func (x *Execution) GetFinishedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.FinishedAt
	}
	return nil
}

func (x *Execution) GetAnnotations() []*v1.Annotation {
	if x != nil {
		return x.Annotations
	}
	return nil
}

func (x *Execution) GetExtras() []*ExecutionExtra {
	if x != nil {
		return x.Extras
	}
	return nil
}

type ExecutionExtra struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Types that are valid to be assigned to Extra:
	//
	//	*ExecutionExtra_ExecutedSql
	Extra         isExecutionExtra_Extra `protobuf_oneof:"extra"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ExecutionExtra) Reset() {
	*x = ExecutionExtra{}
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ExecutionExtra) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExecutionExtra) ProtoMessage() {}

func (x *ExecutionExtra) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExecutionExtra.ProtoReflect.Descriptor instead.
func (*ExecutionExtra) Descriptor() ([]byte, []int) {
	return file_synq_entities_executions_v1_entity_executions_service_proto_rawDescGZIP(), []int{3}
}

func (x *ExecutionExtra) GetExtra() isExecutionExtra_Extra {
	if x != nil {
		return x.Extra
	}
	return nil
}

func (x *ExecutionExtra) GetExecutedSql() string {
	if x != nil {
		if x, ok := x.Extra.(*ExecutionExtra_ExecutedSql); ok {
			return x.ExecutedSql
		}
	}
	return ""
}

type isExecutionExtra_Extra interface {
	isExecutionExtra_Extra()
}

type ExecutionExtra_ExecutedSql struct {
	ExecutedSql string `protobuf:"bytes,1,opt,name=executed_sql,json=executedSql,proto3,oneof"`
}

func (*ExecutionExtra_ExecutedSql) isExecutionExtra_Extra() {}

type UpsertLogEntryRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	LogEntry      *LogEntry              `protobuf:"bytes,1,opt,name=log_entry,json=logEntry,proto3" json:"log_entry,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpsertLogEntryRequest) Reset() {
	*x = UpsertLogEntryRequest{}
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpsertLogEntryRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpsertLogEntryRequest) ProtoMessage() {}

func (x *UpsertLogEntryRequest) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpsertLogEntryRequest.ProtoReflect.Descriptor instead.
func (*UpsertLogEntryRequest) Descriptor() ([]byte, []int) {
	return file_synq_entities_executions_v1_entity_executions_service_proto_rawDescGZIP(), []int{4}
}

func (x *UpsertLogEntryRequest) GetLogEntry() *LogEntry {
	if x != nil {
		return x.LogEntry
	}
	return nil
}

type UpsertLogEntryResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *UpsertLogEntryResponse) Reset() {
	*x = UpsertLogEntryResponse{}
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpsertLogEntryResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpsertLogEntryResponse) ProtoMessage() {}

func (x *UpsertLogEntryResponse) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpsertLogEntryResponse.ProtoReflect.Descriptor instead.
func (*UpsertLogEntryResponse) Descriptor() ([]byte, []int) {
	return file_synq_entities_executions_v1_entity_executions_service_proto_rawDescGZIP(), []int{5}
}

type LogEntry struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            *v1.Identifier         `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Message       string                 `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	CreatedAt     *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	StartedAt     *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=started_at,json=startedAt,proto3" json:"started_at,omitempty"`
	FinishedAt    *timestamppb.Timestamp `protobuf:"bytes,5,opt,name=finished_at,json=finishedAt,proto3" json:"finished_at,omitempty"`
	Annotations   []*v1.Annotation       `protobuf:"bytes,6,rep,name=annotations,proto3" json:"annotations,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LogEntry) Reset() {
	*x = LogEntry{}
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LogEntry) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LogEntry) ProtoMessage() {}

func (x *LogEntry) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LogEntry.ProtoReflect.Descriptor instead.
func (*LogEntry) Descriptor() ([]byte, []int) {
	return file_synq_entities_executions_v1_entity_executions_service_proto_rawDescGZIP(), []int{6}
}

func (x *LogEntry) GetId() *v1.Identifier {
	if x != nil {
		return x.Id
	}
	return nil
}

func (x *LogEntry) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

func (x *LogEntry) GetCreatedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.CreatedAt
	}
	return nil
}

func (x *LogEntry) GetStartedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.StartedAt
	}
	return nil
}

func (x *LogEntry) GetFinishedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.FinishedAt
	}
	return nil
}

func (x *LogEntry) GetAnnotations() []*v1.Annotation {
	if x != nil {
		return x.Annotations
	}
	return nil
}

var File_synq_entities_executions_v1_entity_executions_service_proto protoreflect.FileDescriptor

var file_synq_entities_executions_v1_entity_executions_service_proto_rawDesc = string([]byte{
	0x0a, 0x3b, 0x73, 0x79, 0x6e, 0x71, 0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f,
	0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x65, 0x6e,
	0x74, 0x69, 0x74, 0x79, 0x5f, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x5f,
	0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1b, 0x73,
	0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x65, 0x78, 0x65,
	0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x76, 0x31, 0x1a, 0x1b, 0x62, 0x75, 0x66, 0x2f,
	0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x65, 0x2f, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x21, 0x73, 0x79, 0x6e, 0x71, 0x2f, 0x65,
	0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x61, 0x6e, 0x6e, 0x6f, 0x74,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x21, 0x73, 0x79, 0x6e,
	0x71, 0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x69, 0x64,
	0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x21,
	0x73, 0x79, 0x6e, 0x71, 0x2f, 0x76, 0x31, 0x2f, 0x73, 0x63, 0x6f, 0x70, 0x65, 0x5f, 0x61, 0x75,
	0x74, 0x68, 0x6f, 0x72, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0x66, 0x0a, 0x16, 0x55, 0x70, 0x73, 0x65, 0x72, 0x74, 0x45, 0x78, 0x65, 0x63, 0x75,
	0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x4c, 0x0a, 0x09, 0x65,
	0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x26,
	0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x65,
	0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x78, 0x65,
	0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x42, 0x06, 0xba, 0x48, 0x03, 0xc8, 0x01, 0x01, 0x52, 0x09,
	0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x19, 0x0a, 0x17, 0x55, 0x70, 0x73,
	0x65, 0x72, 0x74, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x22, 0xf6, 0x03, 0x0a, 0x09, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69,
	0x6f, 0x6e, 0x12, 0x34, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c,
	0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x76,
	0x31, 0x2e, 0x49, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x42, 0x06, 0xba, 0x48,
	0x03, 0xc8, 0x01, 0x01, 0x52, 0x02, 0x69, 0x64, 0x12, 0x4c, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x2c, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e,
	0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69,
	0x6f, 0x6e, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e,
	0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x42, 0x06, 0xba, 0x48, 0x03, 0xc8, 0x01, 0x01, 0x52, 0x06,
	0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x12, 0x4e, 0x0a, 0x0a, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
	0x42, 0x13, 0xba, 0x48, 0x10, 0xc8, 0x01, 0x01, 0xb2, 0x01, 0x0a, 0x38, 0x01, 0x2a, 0x06, 0x08,
	0x80, 0xb3, 0xbe, 0x8e, 0x06, 0x52, 0x09, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x41, 0x74,
	0x12, 0x39, 0x0a, 0x0a, 0x73, 0x74, 0x61, 0x72, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x05,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
	0x52, 0x09, 0x73, 0x74, 0x61, 0x72, 0x74, 0x65, 0x64, 0x41, 0x74, 0x12, 0x3b, 0x0a, 0x0b, 0x66,
	0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62,
	0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0a, 0x66, 0x69,
	0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x41, 0x74, 0x12, 0x3e, 0x0a, 0x0b, 0x61, 0x6e, 0x6e, 0x6f,
	0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x07, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1c, 0x2e,
	0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x76, 0x31,
	0x2e, 0x41, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0b, 0x61, 0x6e, 0x6e,
	0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x43, 0x0a, 0x06, 0x65, 0x78, 0x74, 0x72,
	0x61, 0x73, 0x18, 0x08, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2b, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e,
	0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69,
	0x6f, 0x6e, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e,
	0x45, 0x78, 0x74, 0x72, 0x61, 0x52, 0x06, 0x65, 0x78, 0x74, 0x72, 0x61, 0x73, 0x22, 0x3e, 0x0a,
	0x0e, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x45, 0x78, 0x74, 0x72, 0x61, 0x12,
	0x23, 0x0a, 0x0c, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x65, 0x64, 0x5f, 0x73, 0x71, 0x6c, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x0b, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x65,
	0x64, 0x53, 0x71, 0x6c, 0x42, 0x07, 0x0a, 0x05, 0x65, 0x78, 0x74, 0x72, 0x61, 0x22, 0x63, 0x0a,
	0x15, 0x55, 0x70, 0x73, 0x65, 0x72, 0x74, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x4a, 0x0a, 0x09, 0x6c, 0x6f, 0x67, 0x5f, 0x65, 0x6e,
	0x74, 0x72, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x25, 0x2e, 0x73, 0x79, 0x6e, 0x71,
	0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74,
	0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x42, 0x06, 0xba, 0x48, 0x03, 0xc8, 0x01, 0x01, 0x52, 0x08, 0x6c, 0x6f, 0x67, 0x45, 0x6e, 0x74,
	0x72, 0x79, 0x22, 0x18, 0x0a, 0x16, 0x55, 0x70, 0x73, 0x65, 0x72, 0x74, 0x4c, 0x6f, 0x67, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0xe2, 0x02, 0x0a,
	0x08, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x34, 0x0a, 0x02, 0x69, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74,
	0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x49, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66,
	0x69, 0x65, 0x72, 0x42, 0x06, 0xba, 0x48, 0x03, 0xc8, 0x01, 0x01, 0x52, 0x02, 0x69, 0x64, 0x12,
	0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x4e, 0x0a, 0x0a, 0x63, 0x72, 0x65,
	0x61, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x42, 0x13, 0xba, 0x48, 0x10, 0xc8, 0x01,
	0x01, 0xb2, 0x01, 0x0a, 0x38, 0x01, 0x2a, 0x06, 0x08, 0x80, 0xb3, 0xbe, 0x8e, 0x06, 0x52, 0x09,
	0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x41, 0x74, 0x12, 0x39, 0x0a, 0x0a, 0x73, 0x74, 0x61,
	0x72, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x73, 0x74, 0x61, 0x72, 0x74,
	0x65, 0x64, 0x41, 0x74, 0x12, 0x3b, 0x0a, 0x0b, 0x66, 0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64,
	0x5f, 0x61, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x0a, 0x66, 0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x41,
	0x74, 0x12, 0x3e, 0x0a, 0x0b, 0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73,
	0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e,
	0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x41, 0x6e, 0x6e, 0x6f, 0x74, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0b, 0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x2a, 0xa2, 0x01, 0x0a, 0x0f, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x53,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x20, 0x0a, 0x1c, 0x45, 0x58, 0x45, 0x43, 0x55, 0x54, 0x49,
	0x4f, 0x4e, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43,
	0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x17, 0x0a, 0x13, 0x45, 0x58, 0x45, 0x43, 0x55,
	0x54, 0x49, 0x4f, 0x4e, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f, 0x4f, 0x4b, 0x10, 0x01,
	0x12, 0x19, 0x0a, 0x15, 0x45, 0x58, 0x45, 0x43, 0x55, 0x54, 0x49, 0x4f, 0x4e, 0x5f, 0x53, 0x54,
	0x41, 0x54, 0x55, 0x53, 0x5f, 0x57, 0x41, 0x52, 0x4e, 0x10, 0x02, 0x12, 0x1a, 0x0a, 0x16, 0x45,
	0x58, 0x45, 0x43, 0x55, 0x54, 0x49, 0x4f, 0x4e, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f,
	0x45, 0x52, 0x52, 0x4f, 0x52, 0x10, 0x03, 0x12, 0x1d, 0x0a, 0x19, 0x45, 0x58, 0x45, 0x43, 0x55,
	0x54, 0x49, 0x4f, 0x4e, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f, 0x43, 0x52, 0x49, 0x54,
	0x49, 0x43, 0x41, 0x4c, 0x10, 0x04, 0x32, 0xa7, 0x02, 0x0a, 0x17, 0x45, 0x6e, 0x74, 0x69, 0x74,
	0x79, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x53, 0x65, 0x72, 0x76, 0x69,
	0x63, 0x65, 0x12, 0x85, 0x01, 0x0a, 0x0f, 0x55, 0x70, 0x73, 0x65, 0x72, 0x74, 0x45, 0x78, 0x65,
	0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x33, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e,
	0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x2e, 0x76, 0x31, 0x2e, 0x55, 0x70, 0x73, 0x65, 0x72, 0x74, 0x45, 0x78, 0x65, 0x63, 0x75,
	0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x34, 0x2e, 0x73, 0x79,
	0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x65, 0x78, 0x65, 0x63,
	0x75, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x55, 0x70, 0x73, 0x65, 0x72, 0x74,
	0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x22, 0x07, 0xd2, 0xb5, 0x18, 0x03, 0x0a, 0x01, 0x3c, 0x12, 0x83, 0x01, 0x0a, 0x0e, 0x55,
	0x70, 0x73, 0x65, 0x72, 0x74, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x32, 0x2e,
	0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x65, 0x78,
	0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x55, 0x70, 0x73, 0x65,
	0x72, 0x74, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x33, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65,
	0x73, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x76, 0x31, 0x2e,
	0x55, 0x70, 0x73, 0x65, 0x72, 0x74, 0x4c, 0x6f, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x08, 0xd2, 0xb5, 0x18, 0x04, 0x0a, 0x02, 0x3c, 0x3d,
	0x42, 0xfd, 0x01, 0x0a, 0x1f, 0x63, 0x6f, 0x6d, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e,
	0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x2e, 0x76, 0x31, 0x42, 0x1c, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x45, 0x78, 0x65, 0x63,
	0x75, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x50, 0x72, 0x6f,
	0x74, 0x6f, 0x50, 0x01, 0x5a, 0x2d, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d,
	0x2f, 0x67, 0x65, 0x74, 0x73, 0x79, 0x6e, 0x71, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x65, 0x6e, 0x74,
	0x69, 0x74, 0x69, 0x65, 0x73, 0x2f, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x73,
	0x2f, 0x76, 0x31, 0xa2, 0x02, 0x03, 0x53, 0x45, 0x45, 0xaa, 0x02, 0x1b, 0x53, 0x79, 0x6e, 0x71,
	0x2e, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74,
	0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x56, 0x31, 0xca, 0x02, 0x1b, 0x53, 0x79, 0x6e, 0x71, 0x5c, 0x45,
	0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x5c, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f,
	0x6e, 0x73, 0x5c, 0x56, 0x31, 0xe2, 0x02, 0x27, 0x53, 0x79, 0x6e, 0x71, 0x5c, 0x45, 0x6e, 0x74,
	0x69, 0x74, 0x69, 0x65, 0x73, 0x5c, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x73,
	0x5c, 0x56, 0x31, 0x5c, 0x47, 0x50, 0x42, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0xea,
	0x02, 0x1e, 0x53, 0x79, 0x6e, 0x71, 0x3a, 0x3a, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73,
	0x3a, 0x3a, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x3a, 0x3a, 0x56, 0x31,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_synq_entities_executions_v1_entity_executions_service_proto_rawDescOnce sync.Once
	file_synq_entities_executions_v1_entity_executions_service_proto_rawDescData []byte
)

func file_synq_entities_executions_v1_entity_executions_service_proto_rawDescGZIP() []byte {
	file_synq_entities_executions_v1_entity_executions_service_proto_rawDescOnce.Do(func() {
		file_synq_entities_executions_v1_entity_executions_service_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_synq_entities_executions_v1_entity_executions_service_proto_rawDesc), len(file_synq_entities_executions_v1_entity_executions_service_proto_rawDesc)))
	})
	return file_synq_entities_executions_v1_entity_executions_service_proto_rawDescData
}

var file_synq_entities_executions_v1_entity_executions_service_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_synq_entities_executions_v1_entity_executions_service_proto_goTypes = []any{
	(ExecutionStatus)(0),            // 0: synq.entities.executions.v1.ExecutionStatus
	(*UpsertExecutionRequest)(nil),  // 1: synq.entities.executions.v1.UpsertExecutionRequest
	(*UpsertExecutionResponse)(nil), // 2: synq.entities.executions.v1.UpsertExecutionResponse
	(*Execution)(nil),               // 3: synq.entities.executions.v1.Execution
	(*ExecutionExtra)(nil),          // 4: synq.entities.executions.v1.ExecutionExtra
	(*UpsertLogEntryRequest)(nil),   // 5: synq.entities.executions.v1.UpsertLogEntryRequest
	(*UpsertLogEntryResponse)(nil),  // 6: synq.entities.executions.v1.UpsertLogEntryResponse
	(*LogEntry)(nil),                // 7: synq.entities.executions.v1.LogEntry
	(*v1.Identifier)(nil),           // 8: synq.entities.v1.Identifier
	(*timestamppb.Timestamp)(nil),   // 9: google.protobuf.Timestamp
	(*v1.Annotation)(nil),           // 10: synq.entities.v1.Annotation
}
var file_synq_entities_executions_v1_entity_executions_service_proto_depIdxs = []int32{
	3,  // 0: synq.entities.executions.v1.UpsertExecutionRequest.execution:type_name -> synq.entities.executions.v1.Execution
	8,  // 1: synq.entities.executions.v1.Execution.id:type_name -> synq.entities.v1.Identifier
	0,  // 2: synq.entities.executions.v1.Execution.status:type_name -> synq.entities.executions.v1.ExecutionStatus
	9,  // 3: synq.entities.executions.v1.Execution.created_at:type_name -> google.protobuf.Timestamp
	9,  // 4: synq.entities.executions.v1.Execution.started_at:type_name -> google.protobuf.Timestamp
	9,  // 5: synq.entities.executions.v1.Execution.finished_at:type_name -> google.protobuf.Timestamp
	10, // 6: synq.entities.executions.v1.Execution.annotations:type_name -> synq.entities.v1.Annotation
	4,  // 7: synq.entities.executions.v1.Execution.extras:type_name -> synq.entities.executions.v1.ExecutionExtra
	7,  // 8: synq.entities.executions.v1.UpsertLogEntryRequest.log_entry:type_name -> synq.entities.executions.v1.LogEntry
	8,  // 9: synq.entities.executions.v1.LogEntry.id:type_name -> synq.entities.v1.Identifier
	9,  // 10: synq.entities.executions.v1.LogEntry.created_at:type_name -> google.protobuf.Timestamp
	9,  // 11: synq.entities.executions.v1.LogEntry.started_at:type_name -> google.protobuf.Timestamp
	9,  // 12: synq.entities.executions.v1.LogEntry.finished_at:type_name -> google.protobuf.Timestamp
	10, // 13: synq.entities.executions.v1.LogEntry.annotations:type_name -> synq.entities.v1.Annotation
	1,  // 14: synq.entities.executions.v1.EntityExecutionsService.UpsertExecution:input_type -> synq.entities.executions.v1.UpsertExecutionRequest
	5,  // 15: synq.entities.executions.v1.EntityExecutionsService.UpsertLogEntry:input_type -> synq.entities.executions.v1.UpsertLogEntryRequest
	2,  // 16: synq.entities.executions.v1.EntityExecutionsService.UpsertExecution:output_type -> synq.entities.executions.v1.UpsertExecutionResponse
	6,  // 17: synq.entities.executions.v1.EntityExecutionsService.UpsertLogEntry:output_type -> synq.entities.executions.v1.UpsertLogEntryResponse
	16, // [16:18] is the sub-list for method output_type
	14, // [14:16] is the sub-list for method input_type
	14, // [14:14] is the sub-list for extension type_name
	14, // [14:14] is the sub-list for extension extendee
	0,  // [0:14] is the sub-list for field type_name
}

func init() { file_synq_entities_executions_v1_entity_executions_service_proto_init() }
func file_synq_entities_executions_v1_entity_executions_service_proto_init() {
	if File_synq_entities_executions_v1_entity_executions_service_proto != nil {
		return
	}
	file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes[3].OneofWrappers = []any{
		(*ExecutionExtra_ExecutedSql)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_synq_entities_executions_v1_entity_executions_service_proto_rawDesc), len(file_synq_entities_executions_v1_entity_executions_service_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_synq_entities_executions_v1_entity_executions_service_proto_goTypes,
		DependencyIndexes: file_synq_entities_executions_v1_entity_executions_service_proto_depIdxs,
		EnumInfos:         file_synq_entities_executions_v1_entity_executions_service_proto_enumTypes,
		MessageInfos:      file_synq_entities_executions_v1_entity_executions_service_proto_msgTypes,
	}.Build()
	File_synq_entities_executions_v1_entity_executions_service_proto = out.File
	file_synq_entities_executions_v1_entity_executions_service_proto_goTypes = nil
	file_synq_entities_executions_v1_entity_executions_service_proto_depIdxs = nil
}
