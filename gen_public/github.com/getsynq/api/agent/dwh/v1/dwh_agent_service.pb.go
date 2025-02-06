// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        (unknown)
// source: synq/agent/dwh/v1/dwh_agent_service.proto

package v1

import (
	_ "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
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

type LogLevel int32

const (
	LogLevel_LOG_LEVEL_UNSPECIFIED LogLevel = 0
	LogLevel_LOG_LEVEL_INFO        LogLevel = 1
	LogLevel_LOG_LEVEL_WARN        LogLevel = 2
	LogLevel_LOG_LEVEL_ERROR       LogLevel = 3
)

// Enum value maps for LogLevel.
var (
	LogLevel_name = map[int32]string{
		0: "LOG_LEVEL_UNSPECIFIED",
		1: "LOG_LEVEL_INFO",
		2: "LOG_LEVEL_WARN",
		3: "LOG_LEVEL_ERROR",
	}
	LogLevel_value = map[string]int32{
		"LOG_LEVEL_UNSPECIFIED": 0,
		"LOG_LEVEL_INFO":        1,
		"LOG_LEVEL_WARN":        2,
		"LOG_LEVEL_ERROR":       3,
	}
)

func (x LogLevel) Enum() *LogLevel {
	p := new(LogLevel)
	*p = x
	return p
}

func (x LogLevel) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (LogLevel) Descriptor() protoreflect.EnumDescriptor {
	return file_synq_agent_dwh_v1_dwh_agent_service_proto_enumTypes[0].Descriptor()
}

func (LogLevel) Type() protoreflect.EnumType {
	return &file_synq_agent_dwh_v1_dwh_agent_service_proto_enumTypes[0]
}

func (x LogLevel) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use LogLevel.Descriptor instead.
func (LogLevel) EnumDescriptor() ([]byte, []int) {
	return file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescGZIP(), []int{0}
}

type ConnectRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Types that are valid to be assigned to Message:
	//
	//	*ConnectRequest_Hello
	//	*ConnectRequest_Log
	Message       isConnectRequest_Message `protobuf_oneof:"message"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ConnectRequest) Reset() {
	*x = ConnectRequest{}
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ConnectRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConnectRequest) ProtoMessage() {}

func (x *ConnectRequest) ProtoReflect() protoreflect.Message {
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConnectRequest.ProtoReflect.Descriptor instead.
func (*ConnectRequest) Descriptor() ([]byte, []int) {
	return file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescGZIP(), []int{0}
}

func (x *ConnectRequest) GetMessage() isConnectRequest_Message {
	if x != nil {
		return x.Message
	}
	return nil
}

func (x *ConnectRequest) GetHello() *Hello {
	if x != nil {
		if x, ok := x.Message.(*ConnectRequest_Hello); ok {
			return x.Hello
		}
	}
	return nil
}

func (x *ConnectRequest) GetLog() *Log {
	if x != nil {
		if x, ok := x.Message.(*ConnectRequest_Log); ok {
			return x.Log
		}
	}
	return nil
}

type isConnectRequest_Message interface {
	isConnectRequest_Message()
}

type ConnectRequest_Hello struct {
	Hello *Hello `protobuf:"bytes,1,opt,name=hello,proto3,oneof"`
}

type ConnectRequest_Log struct {
	Log *Log `protobuf:"bytes,2,opt,name=log,proto3,oneof"`
}

func (*ConnectRequest_Hello) isConnectRequest_Message() {}

func (*ConnectRequest_Log) isConnectRequest_Message() {}

type ConnectResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Tasks         []*AgentTask           `protobuf:"bytes,1,rep,name=tasks,proto3" json:"tasks,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ConnectResponse) Reset() {
	*x = ConnectResponse{}
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ConnectResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConnectResponse) ProtoMessage() {}

func (x *ConnectResponse) ProtoReflect() protoreflect.Message {
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConnectResponse.ProtoReflect.Descriptor instead.
func (*ConnectResponse) Descriptor() ([]byte, []int) {
	return file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescGZIP(), []int{1}
}

func (x *ConnectResponse) GetTasks() []*AgentTask {
	if x != nil {
		return x.Tasks
	}
	return nil
}

type Hello struct {
	state                protoimpl.MessageState       `protogen:"open.v1"`
	Name                 string                       `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	BuildVersion         string                       `protobuf:"bytes,2,opt,name=build_version,json=buildVersion,proto3" json:"build_version,omitempty"`
	BuildTime            string                       `protobuf:"bytes,3,opt,name=build_time,json=buildTime,proto3" json:"build_time,omitempty"`
	AvailableConnections []*Hello_AvailableConnection `protobuf:"bytes,4,rep,name=available_connections,json=availableConnections,proto3" json:"available_connections,omitempty"`
	unknownFields        protoimpl.UnknownFields
	sizeCache            protoimpl.SizeCache
}

func (x *Hello) Reset() {
	*x = Hello{}
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Hello) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Hello) ProtoMessage() {}

func (x *Hello) ProtoReflect() protoreflect.Message {
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Hello.ProtoReflect.Descriptor instead.
func (*Hello) Descriptor() ([]byte, []int) {
	return file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescGZIP(), []int{2}
}

func (x *Hello) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Hello) GetBuildVersion() string {
	if x != nil {
		return x.BuildVersion
	}
	return ""
}

func (x *Hello) GetBuildTime() string {
	if x != nil {
		return x.BuildTime
	}
	return ""
}

func (x *Hello) GetAvailableConnections() []*Hello_AvailableConnection {
	if x != nil {
		return x.AvailableConnections
	}
	return nil
}

type Log struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Time          *timestamppb.Timestamp `protobuf:"bytes,1,opt,name=time,proto3" json:"time,omitempty"`
	Message       string                 `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	Level         LogLevel               `protobuf:"varint,3,opt,name=level,proto3,enum=synq.agent.dwh.v1.LogLevel" json:"level,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Log) Reset() {
	*x = Log{}
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Log) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Log) ProtoMessage() {}

func (x *Log) ProtoReflect() protoreflect.Message {
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Log.ProtoReflect.Descriptor instead.
func (*Log) Descriptor() ([]byte, []int) {
	return file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescGZIP(), []int{3}
}

func (x *Log) GetTime() *timestamppb.Timestamp {
	if x != nil {
		return x.Time
	}
	return nil
}

func (x *Log) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

func (x *Log) GetLevel() LogLevel {
	if x != nil {
		return x.Level
	}
	return LogLevel_LOG_LEVEL_UNSPECIFIED
}

type AgentTask struct {
	state        protoimpl.MessageState `protogen:"open.v1"`
	ConnectionId string                 `protobuf:"bytes,1,opt,name=connection_id,json=connectionId,proto3" json:"connection_id,omitempty"`
	TaskId       string                 `protobuf:"bytes,2,opt,name=task_id,json=taskId,proto3" json:"task_id,omitempty"`
	// Types that are valid to be assigned to Command:
	//
	//	*AgentTask_FetchFullCatalog
	//	*AgentTask_FetchFullMetrics
	Command       isAgentTask_Command `protobuf_oneof:"command"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *AgentTask) Reset() {
	*x = AgentTask{}
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *AgentTask) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AgentTask) ProtoMessage() {}

func (x *AgentTask) ProtoReflect() protoreflect.Message {
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AgentTask.ProtoReflect.Descriptor instead.
func (*AgentTask) Descriptor() ([]byte, []int) {
	return file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescGZIP(), []int{4}
}

func (x *AgentTask) GetConnectionId() string {
	if x != nil {
		return x.ConnectionId
	}
	return ""
}

func (x *AgentTask) GetTaskId() string {
	if x != nil {
		return x.TaskId
	}
	return ""
}

func (x *AgentTask) GetCommand() isAgentTask_Command {
	if x != nil {
		return x.Command
	}
	return nil
}

func (x *AgentTask) GetFetchFullCatalog() *FetchFullCatalogCommand {
	if x != nil {
		if x, ok := x.Command.(*AgentTask_FetchFullCatalog); ok {
			return x.FetchFullCatalog
		}
	}
	return nil
}

func (x *AgentTask) GetFetchFullMetrics() *FetchFullMetricsCommand {
	if x != nil {
		if x, ok := x.Command.(*AgentTask_FetchFullMetrics); ok {
			return x.FetchFullMetrics
		}
	}
	return nil
}

type isAgentTask_Command interface {
	isAgentTask_Command()
}

type AgentTask_FetchFullCatalog struct {
	FetchFullCatalog *FetchFullCatalogCommand `protobuf:"bytes,3,opt,name=fetch_full_catalog,json=fetchFullCatalog,proto3,oneof"`
}

type AgentTask_FetchFullMetrics struct {
	FetchFullMetrics *FetchFullMetricsCommand `protobuf:"bytes,4,opt,name=fetch_full_metrics,json=fetchFullMetrics,proto3,oneof"`
}

func (*AgentTask_FetchFullCatalog) isAgentTask_Command() {}

func (*AgentTask_FetchFullMetrics) isAgentTask_Command() {}

type FetchFullCatalogCommand struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FetchFullCatalogCommand) Reset() {
	*x = FetchFullCatalogCommand{}
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FetchFullCatalogCommand) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FetchFullCatalogCommand) ProtoMessage() {}

func (x *FetchFullCatalogCommand) ProtoReflect() protoreflect.Message {
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FetchFullCatalogCommand.ProtoReflect.Descriptor instead.
func (*FetchFullCatalogCommand) Descriptor() ([]byte, []int) {
	return file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescGZIP(), []int{5}
}

type FetchFullMetricsCommand struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *FetchFullMetricsCommand) Reset() {
	*x = FetchFullMetricsCommand{}
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *FetchFullMetricsCommand) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FetchFullMetricsCommand) ProtoMessage() {}

func (x *FetchFullMetricsCommand) ProtoReflect() protoreflect.Message {
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FetchFullMetricsCommand.ProtoReflect.Descriptor instead.
func (*FetchFullMetricsCommand) Descriptor() ([]byte, []int) {
	return file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescGZIP(), []int{6}
}

type Hello_AvailableConnection struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Identifier which will receive commands
	ConnectionId string `protobuf:"bytes,1,opt,name=connection_id,json=connectionId,proto3" json:"connection_id,omitempty"`
	// User provided connection name
	Name string `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	// Indicates that agent has that connection disabled
	Disabled bool `protobuf:"varint,3,opt,name=disabled,proto3" json:"disabled,omitempty"`
	// Type of the DWH, e.g. bigquery, duckdb, clickhouse
	Type string `protobuf:"bytes,4,opt,name=type,proto3" json:"type,omitempty"`
	// This will be hostname in most cases
	Instance string `protobuf:"bytes,5,opt,name=instance,proto3" json:"instance,omitempty"`
	// Enabled databases to query if connection supports multiple databases, can be empty.
	Databases     []string `protobuf:"bytes,6,rep,name=databases,proto3" json:"databases,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Hello_AvailableConnection) Reset() {
	*x = Hello_AvailableConnection{}
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Hello_AvailableConnection) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Hello_AvailableConnection) ProtoMessage() {}

func (x *Hello_AvailableConnection) ProtoReflect() protoreflect.Message {
	mi := &file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Hello_AvailableConnection.ProtoReflect.Descriptor instead.
func (*Hello_AvailableConnection) Descriptor() ([]byte, []int) {
	return file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescGZIP(), []int{2, 0}
}

func (x *Hello_AvailableConnection) GetConnectionId() string {
	if x != nil {
		return x.ConnectionId
	}
	return ""
}

func (x *Hello_AvailableConnection) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Hello_AvailableConnection) GetDisabled() bool {
	if x != nil {
		return x.Disabled
	}
	return false
}

func (x *Hello_AvailableConnection) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *Hello_AvailableConnection) GetInstance() string {
	if x != nil {
		return x.Instance
	}
	return ""
}

func (x *Hello_AvailableConnection) GetDatabases() []string {
	if x != nil {
		return x.Databases
	}
	return nil
}

var File_synq_agent_dwh_v1_dwh_agent_service_proto protoreflect.FileDescriptor

var file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDesc = string([]byte{
	0x0a, 0x29, 0x73, 0x79, 0x6e, 0x71, 0x2f, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x2f, 0x64, 0x77, 0x68,
	0x2f, 0x76, 0x31, 0x2f, 0x64, 0x77, 0x68, 0x5f, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x5f, 0x73, 0x65,
	0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x11, 0x73, 0x79, 0x6e,
	0x71, 0x2e, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x1a, 0x1b,
	0x62, 0x75, 0x66, 0x2f, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x65, 0x2f, 0x76, 0x61, 0x6c,
	0x69, 0x64, 0x61, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x21, 0x73, 0x79,
	0x6e, 0x71, 0x2f, 0x76, 0x31, 0x2f, 0x73, 0x63, 0x6f, 0x70, 0x65, 0x5f, 0x61, 0x75, 0x74, 0x68,
	0x6f, 0x72, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22,
	0x79, 0x0a, 0x0e, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x12, 0x30, 0x0a, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x18, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x2e, 0x64, 0x77,
	0x68, 0x2e, 0x76, 0x31, 0x2e, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x48, 0x00, 0x52, 0x05, 0x68, 0x65,
	0x6c, 0x6c, 0x6f, 0x12, 0x2a, 0x0a, 0x03, 0x6c, 0x6f, 0x67, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x16, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x2e, 0x64, 0x77,
	0x68, 0x2e, 0x76, 0x31, 0x2e, 0x4c, 0x6f, 0x67, 0x48, 0x00, 0x52, 0x03, 0x6c, 0x6f, 0x67, 0x42,
	0x09, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x45, 0x0a, 0x0f, 0x43, 0x6f,
	0x6e, 0x6e, 0x65, 0x63, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x32, 0x0a,
	0x05, 0x74, 0x61, 0x73, 0x6b, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x73,
	0x79, 0x6e, 0x71, 0x2e, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31,
	0x2e, 0x41, 0x67, 0x65, 0x6e, 0x74, 0x54, 0x61, 0x73, 0x6b, 0x52, 0x05, 0x74, 0x61, 0x73, 0x6b,
	0x73, 0x22, 0xfd, 0x02, 0x0a, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x12, 0x12, 0x0a, 0x04, 0x6e,
	0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12,
	0x23, 0x0a, 0x0d, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x56, 0x65, 0x72,
	0x73, 0x69, 0x6f, 0x6e, 0x12, 0x1d, 0x0a, 0x0a, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x5f, 0x74, 0x69,
	0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x62, 0x75, 0x69, 0x6c, 0x64, 0x54,
	0x69, 0x6d, 0x65, 0x12, 0x61, 0x0a, 0x15, 0x61, 0x76, 0x61, 0x69, 0x6c, 0x61, 0x62, 0x6c, 0x65,
	0x5f, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x04, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x2c, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x2e,
	0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2e, 0x41, 0x76, 0x61,
	0x69, 0x6c, 0x61, 0x62, 0x6c, 0x65, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x52, 0x14, 0x61, 0x76, 0x61, 0x69, 0x6c, 0x61, 0x62, 0x6c, 0x65, 0x43, 0x6f, 0x6e, 0x6e, 0x65,
	0x63, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x1a, 0xb8, 0x01, 0x0a, 0x13, 0x41, 0x76, 0x61, 0x69, 0x6c,
	0x61, 0x62, 0x6c, 0x65, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x23,
	0x0a, 0x0d, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f,
	0x6e, 0x49, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x69, 0x73, 0x61, 0x62,
	0x6c, 0x65, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x08, 0x52, 0x08, 0x64, 0x69, 0x73, 0x61, 0x62,
	0x6c, 0x65, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x69, 0x6e, 0x73, 0x74, 0x61,
	0x6e, 0x63, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x69, 0x6e, 0x73, 0x74, 0x61,
	0x6e, 0x63, 0x65, 0x12, 0x1c, 0x0a, 0x09, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65, 0x73,
	0x18, 0x06, 0x20, 0x03, 0x28, 0x09, 0x52, 0x09, 0x64, 0x61, 0x74, 0x61, 0x62, 0x61, 0x73, 0x65,
	0x73, 0x22, 0x82, 0x01, 0x0a, 0x03, 0x4c, 0x6f, 0x67, 0x12, 0x2e, 0x0a, 0x04, 0x74, 0x69, 0x6d,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x52, 0x04, 0x74, 0x69, 0x6d, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73,
	0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x65, 0x12, 0x31, 0x0a, 0x05, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x0e, 0x32, 0x1b, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x2e,
	0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x4c, 0x6f, 0x67, 0x4c, 0x65, 0x76, 0x65, 0x6c, 0x52,
	0x05, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x22, 0x99, 0x02, 0x0a, 0x09, 0x41, 0x67, 0x65, 0x6e, 0x74,
	0x54, 0x61, 0x73, 0x6b, 0x12, 0x23, 0x0a, 0x0d, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69,
	0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x63, 0x6f, 0x6e,
	0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x24, 0x0a, 0x07, 0x74, 0x61, 0x73,
	0x6b, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x42, 0x0b, 0xba, 0x48, 0x08, 0xc8,
	0x01, 0x01, 0x72, 0x03, 0xb0, 0x01, 0x01, 0x52, 0x06, 0x74, 0x61, 0x73, 0x6b, 0x49, 0x64, 0x12,
	0x5a, 0x0a, 0x12, 0x66, 0x65, 0x74, 0x63, 0x68, 0x5f, 0x66, 0x75, 0x6c, 0x6c, 0x5f, 0x63, 0x61,
	0x74, 0x61, 0x6c, 0x6f, 0x67, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2a, 0x2e, 0x73, 0x79,
	0x6e, 0x71, 0x2e, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x2e,
	0x46, 0x65, 0x74, 0x63, 0x68, 0x46, 0x75, 0x6c, 0x6c, 0x43, 0x61, 0x74, 0x61, 0x6c, 0x6f, 0x67,
	0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x48, 0x00, 0x52, 0x10, 0x66, 0x65, 0x74, 0x63, 0x68,
	0x46, 0x75, 0x6c, 0x6c, 0x43, 0x61, 0x74, 0x61, 0x6c, 0x6f, 0x67, 0x12, 0x5a, 0x0a, 0x12, 0x66,
	0x65, 0x74, 0x63, 0x68, 0x5f, 0x66, 0x75, 0x6c, 0x6c, 0x5f, 0x6d, 0x65, 0x74, 0x72, 0x69, 0x63,
	0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2a, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x61,
	0x67, 0x65, 0x6e, 0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x46, 0x65, 0x74, 0x63,
	0x68, 0x46, 0x75, 0x6c, 0x6c, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x43, 0x6f, 0x6d, 0x6d,
	0x61, 0x6e, 0x64, 0x48, 0x00, 0x52, 0x10, 0x66, 0x65, 0x74, 0x63, 0x68, 0x46, 0x75, 0x6c, 0x6c,
	0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73, 0x42, 0x09, 0x0a, 0x07, 0x63, 0x6f, 0x6d, 0x6d, 0x61,
	0x6e, 0x64, 0x22, 0x19, 0x0a, 0x17, 0x46, 0x65, 0x74, 0x63, 0x68, 0x46, 0x75, 0x6c, 0x6c, 0x43,
	0x61, 0x74, 0x61, 0x6c, 0x6f, 0x67, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x22, 0x19, 0x0a,
	0x17, 0x46, 0x65, 0x74, 0x63, 0x68, 0x46, 0x75, 0x6c, 0x6c, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63,
	0x73, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x2a, 0x62, 0x0a, 0x08, 0x4c, 0x6f, 0x67, 0x4c,
	0x65, 0x76, 0x65, 0x6c, 0x12, 0x19, 0x0a, 0x15, 0x4c, 0x4f, 0x47, 0x5f, 0x4c, 0x45, 0x56, 0x45,
	0x4c, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12,
	0x12, 0x0a, 0x0e, 0x4c, 0x4f, 0x47, 0x5f, 0x4c, 0x45, 0x56, 0x45, 0x4c, 0x5f, 0x49, 0x4e, 0x46,
	0x4f, 0x10, 0x01, 0x12, 0x12, 0x0a, 0x0e, 0x4c, 0x4f, 0x47, 0x5f, 0x4c, 0x45, 0x56, 0x45, 0x4c,
	0x5f, 0x57, 0x41, 0x52, 0x4e, 0x10, 0x02, 0x12, 0x13, 0x0a, 0x0f, 0x4c, 0x4f, 0x47, 0x5f, 0x4c,
	0x45, 0x56, 0x45, 0x4c, 0x5f, 0x45, 0x52, 0x52, 0x4f, 0x52, 0x10, 0x03, 0x32, 0x70, 0x0a, 0x0f,
	0x44, 0x77, 0x68, 0x41, 0x67, 0x65, 0x6e, 0x74, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12,
	0x5d, 0x0a, 0x07, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x12, 0x21, 0x2e, 0x73, 0x79, 0x6e,
	0x71, 0x2e, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x43,
	0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x22, 0x2e,
	0x73, 0x79, 0x6e, 0x71, 0x2e, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76,
	0x31, 0x2e, 0x43, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x22, 0x07, 0xd2, 0xb5, 0x18, 0x03, 0x0a, 0x01, 0x2b, 0x28, 0x01, 0x30, 0x01, 0x42, 0xb9,
	0x01, 0x0a, 0x15, 0x63, 0x6f, 0x6d, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x61, 0x67, 0x65, 0x6e,
	0x74, 0x2e, 0x64, 0x77, 0x68, 0x2e, 0x76, 0x31, 0x42, 0x14, 0x44, 0x77, 0x68, 0x41, 0x67, 0x65,
	0x6e, 0x74, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x01,
	0x5a, 0x23, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x65, 0x74,
	0x73, 0x79, 0x6e, 0x71, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x61, 0x67, 0x65, 0x6e, 0x74, 0x2f, 0x64,
	0x77, 0x68, 0x2f, 0x76, 0x31, 0xa2, 0x02, 0x03, 0x53, 0x41, 0x44, 0xaa, 0x02, 0x11, 0x53, 0x79,
	0x6e, 0x71, 0x2e, 0x41, 0x67, 0x65, 0x6e, 0x74, 0x2e, 0x44, 0x77, 0x68, 0x2e, 0x56, 0x31, 0xca,
	0x02, 0x11, 0x53, 0x79, 0x6e, 0x71, 0x5c, 0x41, 0x67, 0x65, 0x6e, 0x74, 0x5c, 0x44, 0x77, 0x68,
	0x5c, 0x56, 0x31, 0xe2, 0x02, 0x1d, 0x53, 0x79, 0x6e, 0x71, 0x5c, 0x41, 0x67, 0x65, 0x6e, 0x74,
	0x5c, 0x44, 0x77, 0x68, 0x5c, 0x56, 0x31, 0x5c, 0x47, 0x50, 0x42, 0x4d, 0x65, 0x74, 0x61, 0x64,
	0x61, 0x74, 0x61, 0xea, 0x02, 0x14, 0x53, 0x79, 0x6e, 0x71, 0x3a, 0x3a, 0x41, 0x67, 0x65, 0x6e,
	0x74, 0x3a, 0x3a, 0x44, 0x77, 0x68, 0x3a, 0x3a, 0x56, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
})

var (
	file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescOnce sync.Once
	file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescData []byte
)

func file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescGZIP() []byte {
	file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescOnce.Do(func() {
		file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDesc), len(file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDesc)))
	})
	return file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDescData
}

var file_synq_agent_dwh_v1_dwh_agent_service_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_synq_agent_dwh_v1_dwh_agent_service_proto_goTypes = []any{
	(LogLevel)(0),                     // 0: synq.agent.dwh.v1.LogLevel
	(*ConnectRequest)(nil),            // 1: synq.agent.dwh.v1.ConnectRequest
	(*ConnectResponse)(nil),           // 2: synq.agent.dwh.v1.ConnectResponse
	(*Hello)(nil),                     // 3: synq.agent.dwh.v1.Hello
	(*Log)(nil),                       // 4: synq.agent.dwh.v1.Log
	(*AgentTask)(nil),                 // 5: synq.agent.dwh.v1.AgentTask
	(*FetchFullCatalogCommand)(nil),   // 6: synq.agent.dwh.v1.FetchFullCatalogCommand
	(*FetchFullMetricsCommand)(nil),   // 7: synq.agent.dwh.v1.FetchFullMetricsCommand
	(*Hello_AvailableConnection)(nil), // 8: synq.agent.dwh.v1.Hello.AvailableConnection
	(*timestamppb.Timestamp)(nil),     // 9: google.protobuf.Timestamp
}
var file_synq_agent_dwh_v1_dwh_agent_service_proto_depIdxs = []int32{
	3, // 0: synq.agent.dwh.v1.ConnectRequest.hello:type_name -> synq.agent.dwh.v1.Hello
	4, // 1: synq.agent.dwh.v1.ConnectRequest.log:type_name -> synq.agent.dwh.v1.Log
	5, // 2: synq.agent.dwh.v1.ConnectResponse.tasks:type_name -> synq.agent.dwh.v1.AgentTask
	8, // 3: synq.agent.dwh.v1.Hello.available_connections:type_name -> synq.agent.dwh.v1.Hello.AvailableConnection
	9, // 4: synq.agent.dwh.v1.Log.time:type_name -> google.protobuf.Timestamp
	0, // 5: synq.agent.dwh.v1.Log.level:type_name -> synq.agent.dwh.v1.LogLevel
	6, // 6: synq.agent.dwh.v1.AgentTask.fetch_full_catalog:type_name -> synq.agent.dwh.v1.FetchFullCatalogCommand
	7, // 7: synq.agent.dwh.v1.AgentTask.fetch_full_metrics:type_name -> synq.agent.dwh.v1.FetchFullMetricsCommand
	1, // 8: synq.agent.dwh.v1.DwhAgentService.Connect:input_type -> synq.agent.dwh.v1.ConnectRequest
	2, // 9: synq.agent.dwh.v1.DwhAgentService.Connect:output_type -> synq.agent.dwh.v1.ConnectResponse
	9, // [9:10] is the sub-list for method output_type
	8, // [8:9] is the sub-list for method input_type
	8, // [8:8] is the sub-list for extension type_name
	8, // [8:8] is the sub-list for extension extendee
	0, // [0:8] is the sub-list for field type_name
}

func init() { file_synq_agent_dwh_v1_dwh_agent_service_proto_init() }
func file_synq_agent_dwh_v1_dwh_agent_service_proto_init() {
	if File_synq_agent_dwh_v1_dwh_agent_service_proto != nil {
		return
	}
	file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[0].OneofWrappers = []any{
		(*ConnectRequest_Hello)(nil),
		(*ConnectRequest_Log)(nil),
	}
	file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes[4].OneofWrappers = []any{
		(*AgentTask_FetchFullCatalog)(nil),
		(*AgentTask_FetchFullMetrics)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDesc), len(file_synq_agent_dwh_v1_dwh_agent_service_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_synq_agent_dwh_v1_dwh_agent_service_proto_goTypes,
		DependencyIndexes: file_synq_agent_dwh_v1_dwh_agent_service_proto_depIdxs,
		EnumInfos:         file_synq_agent_dwh_v1_dwh_agent_service_proto_enumTypes,
		MessageInfos:      file_synq_agent_dwh_v1_dwh_agent_service_proto_msgTypes,
	}.Build()
	File_synq_agent_dwh_v1_dwh_agent_service_proto = out.File
	file_synq_agent_dwh_v1_dwh_agent_service_proto_goTypes = nil
	file_synq_agent_dwh_v1_dwh_agent_service_proto_depIdxs = nil
}
