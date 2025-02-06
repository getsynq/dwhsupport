// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        (unknown)
// source: synq/entities/lineage/v1/lineage.proto

package v1

import (
	v1 "github.com/getsynq/api/entities/v1"
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

type NodePosition int32

const (
	NodePosition_NODE_POSITION_UNSPECIFIED NodePosition = 0
	NodePosition_NODE_POSITION_START_NODE  NodePosition = 1 // Node is one of the requested start point.
	NodePosition_NODE_POSITION_UPSTREAM    NodePosition = 2 // Node is upstream of the requested start point.
	NodePosition_NODE_POSITION_DOWNSTREAM  NodePosition = 3 // Node is downstream of the requested start point.
)

// Enum value maps for NodePosition.
var (
	NodePosition_name = map[int32]string{
		0: "NODE_POSITION_UNSPECIFIED",
		1: "NODE_POSITION_START_NODE",
		2: "NODE_POSITION_UPSTREAM",
		3: "NODE_POSITION_DOWNSTREAM",
	}
	NodePosition_value = map[string]int32{
		"NODE_POSITION_UNSPECIFIED": 0,
		"NODE_POSITION_START_NODE":  1,
		"NODE_POSITION_UPSTREAM":    2,
		"NODE_POSITION_DOWNSTREAM":  3,
	}
)

func (x NodePosition) Enum() *NodePosition {
	p := new(NodePosition)
	*p = x
	return p
}

func (x NodePosition) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (NodePosition) Descriptor() protoreflect.EnumDescriptor {
	return file_synq_entities_lineage_v1_lineage_proto_enumTypes[0].Descriptor()
}

func (NodePosition) Type() protoreflect.EnumType {
	return &file_synq_entities_lineage_v1_lineage_proto_enumTypes[0]
}

func (x NodePosition) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use NodePosition.Descriptor instead.
func (NodePosition) EnumDescriptor() ([]byte, []int) {
	return file_synq_entities_lineage_v1_lineage_proto_rawDescGZIP(), []int{0}
}

type CllState int32

const (
	// Unspecified state.
	CllState_CLL_STATE_UNSPECIFIED CllState = 0
	// Parsing of the asset SQL failed. No upstream dependencies can be found.
	CllState_CLL_STATE_PARSE_FAILED CllState = 1
	// Extraction of the asset SQL failed. Some unsupported SQL features may be used. Some details might be missing.
	CllState_CLL_STATE_EXTRACTION_FAILED CllState = 2
	// Not all columns or tables were found upstream, lineage is not complete.
	CllState_CLL_STATE_RESOLUTION_FAILED CllState = 3
	// No known issues present.
	CllState_CLL_STATE_OK CllState = 10
)

// Enum value maps for CllState.
var (
	CllState_name = map[int32]string{
		0:  "CLL_STATE_UNSPECIFIED",
		1:  "CLL_STATE_PARSE_FAILED",
		2:  "CLL_STATE_EXTRACTION_FAILED",
		3:  "CLL_STATE_RESOLUTION_FAILED",
		10: "CLL_STATE_OK",
	}
	CllState_value = map[string]int32{
		"CLL_STATE_UNSPECIFIED":       0,
		"CLL_STATE_PARSE_FAILED":      1,
		"CLL_STATE_EXTRACTION_FAILED": 2,
		"CLL_STATE_RESOLUTION_FAILED": 3,
		"CLL_STATE_OK":                10,
	}
)

func (x CllState) Enum() *CllState {
	p := new(CllState)
	*p = x
	return p
}

func (x CllState) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (CllState) Descriptor() protoreflect.EnumDescriptor {
	return file_synq_entities_lineage_v1_lineage_proto_enumTypes[1].Descriptor()
}

func (CllState) Type() protoreflect.EnumType {
	return &file_synq_entities_lineage_v1_lineage_proto_enumTypes[1]
}

func (x CllState) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use CllState.Descriptor instead.
func (CllState) EnumDescriptor() ([]byte, []int) {
	return file_synq_entities_lineage_v1_lineage_proto_rawDescGZIP(), []int{1}
}

// Lineage defines the lineage of table-like entities.
type Lineage struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Nodes in the lineage with their identities and columns.
	Nodes []*LineageNode `protobuf:"bytes,1,rep,name=nodes,proto3" json:"nodes,omitempty"`
	// All edges in the lineage between nodes.
	// This can be parsed to create a graph of all the nodes.
	NodeDependencies []*NodeDependency `protobuf:"bytes,2,rep,name=node_dependencies,json=nodeDependencies,proto3" json:"node_dependencies,omitempty"`
	// Indicates whether the lineage was filtered for column level lineage (CLL).
	IsCll bool `protobuf:"varint,3,opt,name=is_cll,json=isCll,proto3" json:"is_cll,omitempty"`
	// Dependencies between columns. Populated only for CLL.
	ColumnDependencies []*ColumnDependency `protobuf:"bytes,4,rep,name=column_dependencies,json=columnDependencies,proto3" json:"column_dependencies,omitempty"`
	unknownFields      protoimpl.UnknownFields
	sizeCache          protoimpl.SizeCache
}

func (x *Lineage) Reset() {
	*x = Lineage{}
	mi := &file_synq_entities_lineage_v1_lineage_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Lineage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Lineage) ProtoMessage() {}

func (x *Lineage) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_lineage_v1_lineage_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Lineage.ProtoReflect.Descriptor instead.
func (*Lineage) Descriptor() ([]byte, []int) {
	return file_synq_entities_lineage_v1_lineage_proto_rawDescGZIP(), []int{0}
}

func (x *Lineage) GetNodes() []*LineageNode {
	if x != nil {
		return x.Nodes
	}
	return nil
}

func (x *Lineage) GetNodeDependencies() []*NodeDependency {
	if x != nil {
		return x.NodeDependencies
	}
	return nil
}

func (x *Lineage) GetIsCll() bool {
	if x != nil {
		return x.IsCll
	}
	return false
}

func (x *Lineage) GetColumnDependencies() []*ColumnDependency {
	if x != nil {
		return x.ColumnDependencies
	}
	return nil
}

// Indicates data flow between nodes.
// Source nodes are used to compute value of target nodes.
type NodeDependency struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	SourceNodeIdx uint32                 `protobuf:"varint,1,opt,name=source_node_idx,json=sourceNodeIdx,proto3" json:"source_node_idx,omitempty"` // Index of source node in the lineage nodes list.
	TargetNodeIdx uint32                 `protobuf:"varint,2,opt,name=target_node_idx,json=targetNodeIdx,proto3" json:"target_node_idx,omitempty"` // Index of target node in the lineage nodes list.
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *NodeDependency) Reset() {
	*x = NodeDependency{}
	mi := &file_synq_entities_lineage_v1_lineage_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *NodeDependency) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NodeDependency) ProtoMessage() {}

func (x *NodeDependency) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_lineage_v1_lineage_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NodeDependency.ProtoReflect.Descriptor instead.
func (*NodeDependency) Descriptor() ([]byte, []int) {
	return file_synq_entities_lineage_v1_lineage_proto_rawDescGZIP(), []int{1}
}

func (x *NodeDependency) GetSourceNodeIdx() uint32 {
	if x != nil {
		return x.SourceNodeIdx
	}
	return 0
}

func (x *NodeDependency) GetTargetNodeIdx() uint32 {
	if x != nil {
		return x.TargetNodeIdx
	}
	return 0
}

// Indicates data flow between columns.
// Source columns are used to compute value of target columns.
type ColumnDependency struct {
	state              protoimpl.MessageState `protogen:"open.v1"`
	SourceNodeIdx      uint32                 `protobuf:"varint,1,opt,name=source_node_idx,json=sourceNodeIdx,proto3" json:"source_node_idx,omitempty"` // Index of source node in the lineage nodes list.
	SourceNodeColumnId string                 `protobuf:"bytes,2,opt,name=source_node_column_id,json=sourceNodeColumnId,proto3" json:"source_node_column_id,omitempty"`
	TargetNodeIdx      uint32                 `protobuf:"varint,3,opt,name=target_node_idx,json=targetNodeIdx,proto3" json:"target_node_idx,omitempty"` // Index of target node in the lineage nodes list.
	TargetNodeColumnId string                 `protobuf:"bytes,4,opt,name=target_node_column_id,json=targetNodeColumnId,proto3" json:"target_node_column_id,omitempty"`
	unknownFields      protoimpl.UnknownFields
	sizeCache          protoimpl.SizeCache
}

func (x *ColumnDependency) Reset() {
	*x = ColumnDependency{}
	mi := &file_synq_entities_lineage_v1_lineage_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ColumnDependency) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ColumnDependency) ProtoMessage() {}

func (x *ColumnDependency) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_lineage_v1_lineage_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ColumnDependency.ProtoReflect.Descriptor instead.
func (*ColumnDependency) Descriptor() ([]byte, []int) {
	return file_synq_entities_lineage_v1_lineage_proto_rawDescGZIP(), []int{2}
}

func (x *ColumnDependency) GetSourceNodeIdx() uint32 {
	if x != nil {
		return x.SourceNodeIdx
	}
	return 0
}

func (x *ColumnDependency) GetSourceNodeColumnId() string {
	if x != nil {
		return x.SourceNodeColumnId
	}
	return ""
}

func (x *ColumnDependency) GetTargetNodeIdx() uint32 {
	if x != nil {
		return x.TargetNodeIdx
	}
	return 0
}

func (x *ColumnDependency) GetTargetNodeColumnId() string {
	if x != nil {
		return x.TargetNodeColumnId
	}
	return ""
}

// Node in a lineage graph representing one or more entities (e.g. database table).
type LineageNode struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// All entities which have the same identity as this node. Must be at least one item.
	// These are sorted by closeness to the type of the start point entities.
	// e.g. if requesting lineage of a DBT source, first entity should be from DBT, similarly when viewing table it will be other tables.
	Ids []*v1.Identifier `protobuf:"bytes,1,rep,name=ids,proto3" json:"ids,omitempty"`
	// Position of the node in the lineage.
	Position NodePosition `protobuf:"varint,2,opt,name=position,proto3,enum=synq.entities.lineage.v1.NodePosition" json:"position,omitempty"`
	// Populated only for Column Level Lineage (CLL).
	CllDetails    *CllDetails `protobuf:"bytes,3,opt,name=cll_details,json=cllDetails,proto3,oneof" json:"cll_details,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *LineageNode) Reset() {
	*x = LineageNode{}
	mi := &file_synq_entities_lineage_v1_lineage_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *LineageNode) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LineageNode) ProtoMessage() {}

func (x *LineageNode) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_lineage_v1_lineage_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LineageNode.ProtoReflect.Descriptor instead.
func (*LineageNode) Descriptor() ([]byte, []int) {
	return file_synq_entities_lineage_v1_lineage_proto_rawDescGZIP(), []int{3}
}

func (x *LineageNode) GetIds() []*v1.Identifier {
	if x != nil {
		return x.Ids
	}
	return nil
}

func (x *LineageNode) GetPosition() NodePosition {
	if x != nil {
		return x.Position
	}
	return NodePosition_NODE_POSITION_UNSPECIFIED
}

func (x *LineageNode) GetCllDetails() *CllDetails {
	if x != nil {
		return x.CllDetails
	}
	return nil
}

type CllDetails struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Column details for CLL.
	Columns []*Column `protobuf:"bytes,1,rep,name=columns,proto3" json:"columns,omitempty"`
	// State of the CLL parse. UNSPECIFIED if CLL was not requested.
	CllState CllState `protobuf:"varint,2,opt,name=cll_state,json=cllState,proto3,enum=synq.entities.lineage.v1.CllState" json:"cll_state,omitempty"`
	// Messages related to CLL.
	// e.g. Description of parse errors, etc.
	CllMessages   []string `protobuf:"bytes,3,rep,name=cll_messages,json=cllMessages,proto3" json:"cll_messages,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *CllDetails) Reset() {
	*x = CllDetails{}
	mi := &file_synq_entities_lineage_v1_lineage_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CllDetails) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CllDetails) ProtoMessage() {}

func (x *CllDetails) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_lineage_v1_lineage_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CllDetails.ProtoReflect.Descriptor instead.
func (*CllDetails) Descriptor() ([]byte, []int) {
	return file_synq_entities_lineage_v1_lineage_proto_rawDescGZIP(), []int{4}
}

func (x *CllDetails) GetColumns() []*Column {
	if x != nil {
		return x.Columns
	}
	return nil
}

func (x *CllDetails) GetCllState() CllState {
	if x != nil {
		return x.CllState
	}
	return CllState_CLL_STATE_UNSPECIFIED
}

func (x *CllDetails) GetCllMessages() []string {
	if x != nil {
		return x.CllMessages
	}
	return nil
}

// Column in a table-like asset (used in CLL mode).
type Column struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	ColumnId      string                 `protobuf:"bytes,1,opt,name=column_id,json=columnId,proto3" json:"column_id,omitempty"`             // ID string for the column. This is the parsed column name.
	Name          *string                `protobuf:"bytes,2,opt,name=name,proto3,oneof" json:"name,omitempty"`                               // Original column name as fetched from the table.
	NativeType    *string                `protobuf:"bytes,3,opt,name=native_type,json=nativeType,proto3,oneof" json:"native_type,omitempty"` // Column type as fetched from the table.
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Column) Reset() {
	*x = Column{}
	mi := &file_synq_entities_lineage_v1_lineage_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Column) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Column) ProtoMessage() {}

func (x *Column) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_lineage_v1_lineage_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Column.ProtoReflect.Descriptor instead.
func (*Column) Descriptor() ([]byte, []int) {
	return file_synq_entities_lineage_v1_lineage_proto_rawDescGZIP(), []int{5}
}

func (x *Column) GetColumnId() string {
	if x != nil {
		return x.ColumnId
	}
	return ""
}

func (x *Column) GetName() string {
	if x != nil && x.Name != nil {
		return *x.Name
	}
	return ""
}

func (x *Column) GetNativeType() string {
	if x != nil && x.NativeType != nil {
		return *x.NativeType
	}
	return ""
}

var File_synq_entities_lineage_v1_lineage_proto protoreflect.FileDescriptor

var file_synq_entities_lineage_v1_lineage_proto_rawDesc = string([]byte{
	0x0a, 0x26, 0x73, 0x79, 0x6e, 0x71, 0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f,
	0x6c, 0x69, 0x6e, 0x65, 0x61, 0x67, 0x65, 0x2f, 0x76, 0x31, 0x2f, 0x6c, 0x69, 0x6e, 0x65, 0x61,
	0x67, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x18, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65,
	0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x6c, 0x69, 0x6e, 0x65, 0x61, 0x67, 0x65, 0x2e,
	0x76, 0x31, 0x1a, 0x21, 0x73, 0x79, 0x6e, 0x71, 0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65,
	0x73, 0x2f, 0x76, 0x31, 0x2f, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x91, 0x02, 0x0a, 0x07, 0x4c, 0x69, 0x6e, 0x65, 0x61, 0x67,
	0x65, 0x12, 0x3b, 0x0a, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x25, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73,
	0x2e, 0x6c, 0x69, 0x6e, 0x65, 0x61, 0x67, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x4c, 0x69, 0x6e, 0x65,
	0x61, 0x67, 0x65, 0x4e, 0x6f, 0x64, 0x65, 0x52, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x12, 0x55,
	0x0a, 0x11, 0x6e, 0x6f, 0x64, 0x65, 0x5f, 0x64, 0x65, 0x70, 0x65, 0x6e, 0x64, 0x65, 0x6e, 0x63,
	0x69, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x28, 0x2e, 0x73, 0x79, 0x6e, 0x71,
	0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x6c, 0x69, 0x6e, 0x65, 0x61, 0x67,
	0x65, 0x2e, 0x76, 0x31, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x44, 0x65, 0x70, 0x65, 0x6e, 0x64, 0x65,
	0x6e, 0x63, 0x79, 0x52, 0x10, 0x6e, 0x6f, 0x64, 0x65, 0x44, 0x65, 0x70, 0x65, 0x6e, 0x64, 0x65,
	0x6e, 0x63, 0x69, 0x65, 0x73, 0x12, 0x15, 0x0a, 0x06, 0x69, 0x73, 0x5f, 0x63, 0x6c, 0x6c, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x08, 0x52, 0x05, 0x69, 0x73, 0x43, 0x6c, 0x6c, 0x12, 0x5b, 0x0a, 0x13,
	0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x5f, 0x64, 0x65, 0x70, 0x65, 0x6e, 0x64, 0x65, 0x6e, 0x63,
	0x69, 0x65, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2a, 0x2e, 0x73, 0x79, 0x6e, 0x71,
	0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x6c, 0x69, 0x6e, 0x65, 0x61, 0x67,
	0x65, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x44, 0x65, 0x70, 0x65, 0x6e,
	0x64, 0x65, 0x6e, 0x63, 0x79, 0x52, 0x12, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x44, 0x65, 0x70,
	0x65, 0x6e, 0x64, 0x65, 0x6e, 0x63, 0x69, 0x65, 0x73, 0x22, 0x60, 0x0a, 0x0e, 0x4e, 0x6f, 0x64,
	0x65, 0x44, 0x65, 0x70, 0x65, 0x6e, 0x64, 0x65, 0x6e, 0x63, 0x79, 0x12, 0x26, 0x0a, 0x0f, 0x73,
	0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x6e, 0x6f, 0x64, 0x65, 0x5f, 0x69, 0x64, 0x78, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0d, 0x52, 0x0d, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x4e, 0x6f, 0x64, 0x65,
	0x49, 0x64, 0x78, 0x12, 0x26, 0x0a, 0x0f, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x5f, 0x6e, 0x6f,
	0x64, 0x65, 0x5f, 0x69, 0x64, 0x78, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0d, 0x74, 0x61,
	0x72, 0x67, 0x65, 0x74, 0x4e, 0x6f, 0x64, 0x65, 0x49, 0x64, 0x78, 0x22, 0xc8, 0x01, 0x0a, 0x10,
	0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x44, 0x65, 0x70, 0x65, 0x6e, 0x64, 0x65, 0x6e, 0x63, 0x79,
	0x12, 0x26, 0x0a, 0x0f, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x5f, 0x6e, 0x6f, 0x64, 0x65, 0x5f,
	0x69, 0x64, 0x78, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0d, 0x73, 0x6f, 0x75, 0x72, 0x63,
	0x65, 0x4e, 0x6f, 0x64, 0x65, 0x49, 0x64, 0x78, 0x12, 0x31, 0x0a, 0x15, 0x73, 0x6f, 0x75, 0x72,
	0x63, 0x65, 0x5f, 0x6e, 0x6f, 0x64, 0x65, 0x5f, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x5f, 0x69,
	0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x12, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x4e,
	0x6f, 0x64, 0x65, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x49, 0x64, 0x12, 0x26, 0x0a, 0x0f, 0x74,
	0x61, 0x72, 0x67, 0x65, 0x74, 0x5f, 0x6e, 0x6f, 0x64, 0x65, 0x5f, 0x69, 0x64, 0x78, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x0d, 0x52, 0x0d, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x4e, 0x6f, 0x64, 0x65,
	0x49, 0x64, 0x78, 0x12, 0x31, 0x0a, 0x15, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x5f, 0x6e, 0x6f,
	0x64, 0x65, 0x5f, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x12, 0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x4e, 0x6f, 0x64, 0x65, 0x43, 0x6f,
	0x6c, 0x75, 0x6d, 0x6e, 0x49, 0x64, 0x22, 0xdd, 0x01, 0x0a, 0x0b, 0x4c, 0x69, 0x6e, 0x65, 0x61,
	0x67, 0x65, 0x4e, 0x6f, 0x64, 0x65, 0x12, 0x2e, 0x0a, 0x03, 0x69, 0x64, 0x73, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74,
	0x69, 0x65, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x49, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65,
	0x72, 0x52, 0x03, 0x69, 0x64, 0x73, 0x12, 0x42, 0x0a, 0x08, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x69,
	0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x26, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e,
	0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x6c, 0x69, 0x6e, 0x65, 0x61, 0x67, 0x65,
	0x2e, 0x76, 0x31, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x50, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e,
	0x52, 0x08, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x4a, 0x0a, 0x0b, 0x63, 0x6c,
	0x6c, 0x5f, 0x64, 0x65, 0x74, 0x61, 0x69, 0x6c, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x24, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e,
	0x6c, 0x69, 0x6e, 0x65, 0x61, 0x67, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x6c, 0x6c, 0x44, 0x65,
	0x74, 0x61, 0x69, 0x6c, 0x73, 0x48, 0x00, 0x52, 0x0a, 0x63, 0x6c, 0x6c, 0x44, 0x65, 0x74, 0x61,
	0x69, 0x6c, 0x73, 0x88, 0x01, 0x01, 0x42, 0x0e, 0x0a, 0x0c, 0x5f, 0x63, 0x6c, 0x6c, 0x5f, 0x64,
	0x65, 0x74, 0x61, 0x69, 0x6c, 0x73, 0x22, 0xac, 0x01, 0x0a, 0x0a, 0x43, 0x6c, 0x6c, 0x44, 0x65,
	0x74, 0x61, 0x69, 0x6c, 0x73, 0x12, 0x3a, 0x0a, 0x07, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e,
	0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x6c, 0x69, 0x6e, 0x65, 0x61, 0x67, 0x65, 0x2e, 0x76,
	0x31, 0x2e, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x52, 0x07, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e,
	0x73, 0x12, 0x3f, 0x0a, 0x09, 0x63, 0x6c, 0x6c, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0e, 0x32, 0x22, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69,
	0x74, 0x69, 0x65, 0x73, 0x2e, 0x6c, 0x69, 0x6e, 0x65, 0x61, 0x67, 0x65, 0x2e, 0x76, 0x31, 0x2e,
	0x43, 0x6c, 0x6c, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x08, 0x63, 0x6c, 0x6c, 0x53, 0x74, 0x61,
	0x74, 0x65, 0x12, 0x21, 0x0a, 0x0c, 0x63, 0x6c, 0x6c, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0b, 0x63, 0x6c, 0x6c, 0x4d, 0x65, 0x73,
	0x73, 0x61, 0x67, 0x65, 0x73, 0x22, 0x7d, 0x0a, 0x06, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x12,
	0x1b, 0x0a, 0x09, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x08, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x49, 0x64, 0x12, 0x17, 0x0a, 0x04,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x04, 0x6e, 0x61,
	0x6d, 0x65, 0x88, 0x01, 0x01, 0x12, 0x24, 0x0a, 0x0b, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f,
	0x74, 0x79, 0x70, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x48, 0x01, 0x52, 0x0a, 0x6e, 0x61,
	0x74, 0x69, 0x76, 0x65, 0x54, 0x79, 0x70, 0x65, 0x88, 0x01, 0x01, 0x42, 0x07, 0x0a, 0x05, 0x5f,
	0x6e, 0x61, 0x6d, 0x65, 0x42, 0x0e, 0x0a, 0x0c, 0x5f, 0x6e, 0x61, 0x74, 0x69, 0x76, 0x65, 0x5f,
	0x74, 0x79, 0x70, 0x65, 0x2a, 0x85, 0x01, 0x0a, 0x0c, 0x4e, 0x6f, 0x64, 0x65, 0x50, 0x6f, 0x73,
	0x69, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x1d, 0x0a, 0x19, 0x4e, 0x4f, 0x44, 0x45, 0x5f, 0x50, 0x4f,
	0x53, 0x49, 0x54, 0x49, 0x4f, 0x4e, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49,
	0x45, 0x44, 0x10, 0x00, 0x12, 0x1c, 0x0a, 0x18, 0x4e, 0x4f, 0x44, 0x45, 0x5f, 0x50, 0x4f, 0x53,
	0x49, 0x54, 0x49, 0x4f, 0x4e, 0x5f, 0x53, 0x54, 0x41, 0x52, 0x54, 0x5f, 0x4e, 0x4f, 0x44, 0x45,
	0x10, 0x01, 0x12, 0x1a, 0x0a, 0x16, 0x4e, 0x4f, 0x44, 0x45, 0x5f, 0x50, 0x4f, 0x53, 0x49, 0x54,
	0x49, 0x4f, 0x4e, 0x5f, 0x55, 0x50, 0x53, 0x54, 0x52, 0x45, 0x41, 0x4d, 0x10, 0x02, 0x12, 0x1c,
	0x0a, 0x18, 0x4e, 0x4f, 0x44, 0x45, 0x5f, 0x50, 0x4f, 0x53, 0x49, 0x54, 0x49, 0x4f, 0x4e, 0x5f,
	0x44, 0x4f, 0x57, 0x4e, 0x53, 0x54, 0x52, 0x45, 0x41, 0x4d, 0x10, 0x03, 0x2a, 0x95, 0x01, 0x0a,
	0x08, 0x43, 0x6c, 0x6c, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x19, 0x0a, 0x15, 0x43, 0x4c, 0x4c,
	0x5f, 0x53, 0x54, 0x41, 0x54, 0x45, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49,
	0x45, 0x44, 0x10, 0x00, 0x12, 0x1a, 0x0a, 0x16, 0x43, 0x4c, 0x4c, 0x5f, 0x53, 0x54, 0x41, 0x54,
	0x45, 0x5f, 0x50, 0x41, 0x52, 0x53, 0x45, 0x5f, 0x46, 0x41, 0x49, 0x4c, 0x45, 0x44, 0x10, 0x01,
	0x12, 0x1f, 0x0a, 0x1b, 0x43, 0x4c, 0x4c, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x45, 0x5f, 0x45, 0x58,
	0x54, 0x52, 0x41, 0x43, 0x54, 0x49, 0x4f, 0x4e, 0x5f, 0x46, 0x41, 0x49, 0x4c, 0x45, 0x44, 0x10,
	0x02, 0x12, 0x1f, 0x0a, 0x1b, 0x43, 0x4c, 0x4c, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x45, 0x5f, 0x52,
	0x45, 0x53, 0x4f, 0x4c, 0x55, 0x54, 0x49, 0x4f, 0x4e, 0x5f, 0x46, 0x41, 0x49, 0x4c, 0x45, 0x44,
	0x10, 0x03, 0x12, 0x10, 0x0a, 0x0c, 0x43, 0x4c, 0x4c, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x45, 0x5f,
	0x4f, 0x4b, 0x10, 0x0a, 0x42, 0xdb, 0x01, 0x0a, 0x1c, 0x63, 0x6f, 0x6d, 0x2e, 0x73, 0x79, 0x6e,
	0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x6c, 0x69, 0x6e, 0x65, 0x61,
	0x67, 0x65, 0x2e, 0x76, 0x31, 0x42, 0x0c, 0x4c, 0x69, 0x6e, 0x65, 0x61, 0x67, 0x65, 0x50, 0x72,
	0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a, 0x2a, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x67, 0x65, 0x74, 0x73, 0x79, 0x6e, 0x71, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x65, 0x6e,
	0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f, 0x6c, 0x69, 0x6e, 0x65, 0x61, 0x67, 0x65, 0x2f, 0x76,
	0x31, 0xa2, 0x02, 0x03, 0x53, 0x45, 0x4c, 0xaa, 0x02, 0x18, 0x53, 0x79, 0x6e, 0x71, 0x2e, 0x45,
	0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x4c, 0x69, 0x6e, 0x65, 0x61, 0x67, 0x65, 0x2e,
	0x56, 0x31, 0xca, 0x02, 0x18, 0x53, 0x79, 0x6e, 0x71, 0x5c, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69,
	0x65, 0x73, 0x5c, 0x4c, 0x69, 0x6e, 0x65, 0x61, 0x67, 0x65, 0x5c, 0x56, 0x31, 0xe2, 0x02, 0x24,
	0x53, 0x79, 0x6e, 0x71, 0x5c, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x5c, 0x4c, 0x69,
	0x6e, 0x65, 0x61, 0x67, 0x65, 0x5c, 0x56, 0x31, 0x5c, 0x47, 0x50, 0x42, 0x4d, 0x65, 0x74, 0x61,
	0x64, 0x61, 0x74, 0x61, 0xea, 0x02, 0x1b, 0x53, 0x79, 0x6e, 0x71, 0x3a, 0x3a, 0x45, 0x6e, 0x74,
	0x69, 0x74, 0x69, 0x65, 0x73, 0x3a, 0x3a, 0x4c, 0x69, 0x6e, 0x65, 0x61, 0x67, 0x65, 0x3a, 0x3a,
	0x56, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_synq_entities_lineage_v1_lineage_proto_rawDescOnce sync.Once
	file_synq_entities_lineage_v1_lineage_proto_rawDescData []byte
)

func file_synq_entities_lineage_v1_lineage_proto_rawDescGZIP() []byte {
	file_synq_entities_lineage_v1_lineage_proto_rawDescOnce.Do(func() {
		file_synq_entities_lineage_v1_lineage_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_synq_entities_lineage_v1_lineage_proto_rawDesc), len(file_synq_entities_lineage_v1_lineage_proto_rawDesc)))
	})
	return file_synq_entities_lineage_v1_lineage_proto_rawDescData
}

var file_synq_entities_lineage_v1_lineage_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_synq_entities_lineage_v1_lineage_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_synq_entities_lineage_v1_lineage_proto_goTypes = []any{
	(NodePosition)(0),        // 0: synq.entities.lineage.v1.NodePosition
	(CllState)(0),            // 1: synq.entities.lineage.v1.CllState
	(*Lineage)(nil),          // 2: synq.entities.lineage.v1.Lineage
	(*NodeDependency)(nil),   // 3: synq.entities.lineage.v1.NodeDependency
	(*ColumnDependency)(nil), // 4: synq.entities.lineage.v1.ColumnDependency
	(*LineageNode)(nil),      // 5: synq.entities.lineage.v1.LineageNode
	(*CllDetails)(nil),       // 6: synq.entities.lineage.v1.CllDetails
	(*Column)(nil),           // 7: synq.entities.lineage.v1.Column
	(*v1.Identifier)(nil),    // 8: synq.entities.v1.Identifier
}
var file_synq_entities_lineage_v1_lineage_proto_depIdxs = []int32{
	5, // 0: synq.entities.lineage.v1.Lineage.nodes:type_name -> synq.entities.lineage.v1.LineageNode
	3, // 1: synq.entities.lineage.v1.Lineage.node_dependencies:type_name -> synq.entities.lineage.v1.NodeDependency
	4, // 2: synq.entities.lineage.v1.Lineage.column_dependencies:type_name -> synq.entities.lineage.v1.ColumnDependency
	8, // 3: synq.entities.lineage.v1.LineageNode.ids:type_name -> synq.entities.v1.Identifier
	0, // 4: synq.entities.lineage.v1.LineageNode.position:type_name -> synq.entities.lineage.v1.NodePosition
	6, // 5: synq.entities.lineage.v1.LineageNode.cll_details:type_name -> synq.entities.lineage.v1.CllDetails
	7, // 6: synq.entities.lineage.v1.CllDetails.columns:type_name -> synq.entities.lineage.v1.Column
	1, // 7: synq.entities.lineage.v1.CllDetails.cll_state:type_name -> synq.entities.lineage.v1.CllState
	8, // [8:8] is the sub-list for method output_type
	8, // [8:8] is the sub-list for method input_type
	8, // [8:8] is the sub-list for extension type_name
	8, // [8:8] is the sub-list for extension extendee
	0, // [0:8] is the sub-list for field type_name
}

func init() { file_synq_entities_lineage_v1_lineage_proto_init() }
func file_synq_entities_lineage_v1_lineage_proto_init() {
	if File_synq_entities_lineage_v1_lineage_proto != nil {
		return
	}
	file_synq_entities_lineage_v1_lineage_proto_msgTypes[3].OneofWrappers = []any{}
	file_synq_entities_lineage_v1_lineage_proto_msgTypes[5].OneofWrappers = []any{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_synq_entities_lineage_v1_lineage_proto_rawDesc), len(file_synq_entities_lineage_v1_lineage_proto_rawDesc)),
			NumEnums:      2,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_synq_entities_lineage_v1_lineage_proto_goTypes,
		DependencyIndexes: file_synq_entities_lineage_v1_lineage_proto_depIdxs,
		EnumInfos:         file_synq_entities_lineage_v1_lineage_proto_enumTypes,
		MessageInfos:      file_synq_entities_lineage_v1_lineage_proto_msgTypes,
	}.Build()
	File_synq_entities_lineage_v1_lineage_proto = out.File
	file_synq_entities_lineage_v1_lineage_proto_goTypes = nil
	file_synq_entities_lineage_v1_lineage_proto_depIdxs = nil
}
