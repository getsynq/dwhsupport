// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.5
// 	protoc        (unknown)
// source: synq/entities/entities/v1/entities_service.proto

package v1

import (
	_ "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	v1 "github.com/getsynq/api/entities/v1"
	_ "github.com/getsynq/api/v1"
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

// GetEntityRequest is the request message for the GetEntity method.
type GetEntityRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Identifier of the entity to get.
	Id            *v1.Identifier `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetEntityRequest) Reset() {
	*x = GetEntityRequest{}
	mi := &file_synq_entities_entities_v1_entities_service_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetEntityRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetEntityRequest) ProtoMessage() {}

func (x *GetEntityRequest) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_entities_v1_entities_service_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetEntityRequest.ProtoReflect.Descriptor instead.
func (*GetEntityRequest) Descriptor() ([]byte, []int) {
	return file_synq_entities_entities_v1_entities_service_proto_rawDescGZIP(), []int{0}
}

func (x *GetEntityRequest) GetId() *v1.Identifier {
	if x != nil {
		return x.Id
	}
	return nil
}

// GetEntityResponse is the response message for the GetEntity method.
type GetEntityResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The entity that was retrieved.
	Entity        *v1.Entity `protobuf:"bytes,1,opt,name=entity,proto3" json:"entity,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *GetEntityResponse) Reset() {
	*x = GetEntityResponse{}
	mi := &file_synq_entities_entities_v1_entities_service_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetEntityResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetEntityResponse) ProtoMessage() {}

func (x *GetEntityResponse) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_entities_v1_entities_service_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetEntityResponse.ProtoReflect.Descriptor instead.
func (*GetEntityResponse) Descriptor() ([]byte, []int) {
	return file_synq_entities_entities_v1_entities_service_proto_rawDescGZIP(), []int{1}
}

func (x *GetEntityResponse) GetEntity() *v1.Entity {
	if x != nil {
		return x.Entity
	}
	return nil
}

// BatchGetEntitiesRequest is the request message for the BatchGetEntities method.
type BatchGetEntitiesRequest struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// Identifiers of the entities to get.
	Ids           []*v1.Identifier `protobuf:"bytes,1,rep,name=ids,proto3" json:"ids,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *BatchGetEntitiesRequest) Reset() {
	*x = BatchGetEntitiesRequest{}
	mi := &file_synq_entities_entities_v1_entities_service_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BatchGetEntitiesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchGetEntitiesRequest) ProtoMessage() {}

func (x *BatchGetEntitiesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_entities_v1_entities_service_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchGetEntitiesRequest.ProtoReflect.Descriptor instead.
func (*BatchGetEntitiesRequest) Descriptor() ([]byte, []int) {
	return file_synq_entities_entities_v1_entities_service_proto_rawDescGZIP(), []int{2}
}

func (x *BatchGetEntitiesRequest) GetIds() []*v1.Identifier {
	if x != nil {
		return x.Ids
	}
	return nil
}

// BatchGetEntitiesResponse is the response message for the BatchGetEntities method.
type BatchGetEntitiesResponse struct {
	state protoimpl.MessageState `protogen:"open.v1"`
	// The entities that were retrieved.
	Entities      []*v1.Entity `protobuf:"bytes,1,rep,name=entities,proto3" json:"entities,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *BatchGetEntitiesResponse) Reset() {
	*x = BatchGetEntitiesResponse{}
	mi := &file_synq_entities_entities_v1_entities_service_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BatchGetEntitiesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BatchGetEntitiesResponse) ProtoMessage() {}

func (x *BatchGetEntitiesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_synq_entities_entities_v1_entities_service_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BatchGetEntitiesResponse.ProtoReflect.Descriptor instead.
func (*BatchGetEntitiesResponse) Descriptor() ([]byte, []int) {
	return file_synq_entities_entities_v1_entities_service_proto_rawDescGZIP(), []int{3}
}

func (x *BatchGetEntitiesResponse) GetEntities() []*v1.Entity {
	if x != nil {
		return x.Entities
	}
	return nil
}

var File_synq_entities_entities_v1_entities_service_proto protoreflect.FileDescriptor

var file_synq_entities_entities_v1_entities_service_proto_rawDesc = string([]byte{
	0x0a, 0x30, 0x73, 0x79, 0x6e, 0x71, 0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f,
	0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x65, 0x6e, 0x74, 0x69,
	0x74, 0x69, 0x65, 0x73, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x19, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65,
	0x73, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x76, 0x31, 0x1a, 0x1b, 0x62,
	0x75, 0x66, 0x2f, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x65, 0x2f, 0x76, 0x61, 0x6c, 0x69,
	0x64, 0x61, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1d, 0x73, 0x79, 0x6e, 0x71,
	0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x65, 0x6e, 0x74,
	0x69, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x21, 0x73, 0x79, 0x6e, 0x71, 0x2f,
	0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f, 0x76, 0x31, 0x2f, 0x69, 0x64, 0x65, 0x6e,
	0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x21, 0x73, 0x79,
	0x6e, 0x71, 0x2f, 0x76, 0x31, 0x2f, 0x73, 0x63, 0x6f, 0x70, 0x65, 0x5f, 0x61, 0x75, 0x74, 0x68,
	0x6f, 0x72, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22,
	0x48, 0x0a, 0x10, 0x47, 0x65, 0x74, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x34, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x1c, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e,
	0x76, 0x31, 0x2e, 0x49, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x42, 0x06, 0xba,
	0x48, 0x03, 0xc8, 0x01, 0x01, 0x52, 0x02, 0x69, 0x64, 0x22, 0x45, 0x0a, 0x11, 0x47, 0x65, 0x74,
	0x45, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x30,
	0x0a, 0x06, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18,
	0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x76,
	0x31, 0x2e, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x52, 0x06, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x79,
	0x22, 0x51, 0x0a, 0x17, 0x42, 0x61, 0x74, 0x63, 0x68, 0x47, 0x65, 0x74, 0x45, 0x6e, 0x74, 0x69,
	0x74, 0x69, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x36, 0x0a, 0x03, 0x69,
	0x64, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e,
	0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x49, 0x64, 0x65, 0x6e,
	0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x42, 0x06, 0xba, 0x48, 0x03, 0xc8, 0x01, 0x01, 0x52, 0x03,
	0x69, 0x64, 0x73, 0x22, 0x50, 0x0a, 0x18, 0x42, 0x61, 0x74, 0x63, 0x68, 0x47, 0x65, 0x74, 0x45,
	0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
	0x34, 0x0a, 0x08, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x18, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65,
	0x73, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x52, 0x08, 0x65, 0x6e, 0x74,
	0x69, 0x74, 0x69, 0x65, 0x73, 0x32, 0x89, 0x02, 0x0a, 0x0f, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69,
	0x65, 0x73, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x6f, 0x0a, 0x09, 0x47, 0x65, 0x74,
	0x45, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x12, 0x2b, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e,
	0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e,
	0x76, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x2c, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74,
	0x69, 0x65, 0x73, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x76, 0x31, 0x2e,
	0x47, 0x65, 0x74, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x22, 0x07, 0xd2, 0xb5, 0x18, 0x03, 0x0a, 0x01, 0x1f, 0x12, 0x84, 0x01, 0x0a, 0x10, 0x42,
	0x61, 0x74, 0x63, 0x68, 0x47, 0x65, 0x74, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x12,
	0x32, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e,
	0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x76, 0x31, 0x2e, 0x42, 0x61, 0x74, 0x63,
	0x68, 0x47, 0x65, 0x74, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x1a, 0x33, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74,
	0x69, 0x65, 0x73, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x76, 0x31, 0x2e,
	0x42, 0x61, 0x74, 0x63, 0x68, 0x47, 0x65, 0x74, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x07, 0xd2, 0xb5, 0x18, 0x03, 0x0a, 0x01,
	0x1f, 0x42, 0xe9, 0x01, 0x0a, 0x1d, 0x63, 0x6f, 0x6d, 0x2e, 0x73, 0x79, 0x6e, 0x71, 0x2e, 0x65,
	0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73,
	0x2e, 0x76, 0x31, 0x42, 0x14, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x53, 0x65, 0x72,
	0x76, 0x69, 0x63, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a, 0x2b, 0x67, 0x69, 0x74,
	0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x65, 0x74, 0x73, 0x79, 0x6e, 0x71, 0x2f,
	0x61, 0x70, 0x69, 0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2f, 0x65, 0x6e, 0x74,
	0x69, 0x74, 0x69, 0x65, 0x73, 0x2f, 0x76, 0x31, 0xa2, 0x02, 0x03, 0x53, 0x45, 0x45, 0xaa, 0x02,
	0x19, 0x53, 0x79, 0x6e, 0x71, 0x2e, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x45,
	0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x2e, 0x56, 0x31, 0xca, 0x02, 0x19, 0x53, 0x79, 0x6e,
	0x71, 0x5c, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x5c, 0x45, 0x6e, 0x74, 0x69, 0x74,
	0x69, 0x65, 0x73, 0x5c, 0x56, 0x31, 0xe2, 0x02, 0x25, 0x53, 0x79, 0x6e, 0x71, 0x5c, 0x45, 0x6e,
	0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x5c, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x5c,
	0x56, 0x31, 0x5c, 0x47, 0x50, 0x42, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0xea, 0x02,
	0x1c, 0x53, 0x79, 0x6e, 0x71, 0x3a, 0x3a, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x3a,
	0x3a, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x69, 0x65, 0x73, 0x3a, 0x3a, 0x56, 0x31, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_synq_entities_entities_v1_entities_service_proto_rawDescOnce sync.Once
	file_synq_entities_entities_v1_entities_service_proto_rawDescData []byte
)

func file_synq_entities_entities_v1_entities_service_proto_rawDescGZIP() []byte {
	file_synq_entities_entities_v1_entities_service_proto_rawDescOnce.Do(func() {
		file_synq_entities_entities_v1_entities_service_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_synq_entities_entities_v1_entities_service_proto_rawDesc), len(file_synq_entities_entities_v1_entities_service_proto_rawDesc)))
	})
	return file_synq_entities_entities_v1_entities_service_proto_rawDescData
}

var file_synq_entities_entities_v1_entities_service_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_synq_entities_entities_v1_entities_service_proto_goTypes = []any{
	(*GetEntityRequest)(nil),         // 0: synq.entities.entities.v1.GetEntityRequest
	(*GetEntityResponse)(nil),        // 1: synq.entities.entities.v1.GetEntityResponse
	(*BatchGetEntitiesRequest)(nil),  // 2: synq.entities.entities.v1.BatchGetEntitiesRequest
	(*BatchGetEntitiesResponse)(nil), // 3: synq.entities.entities.v1.BatchGetEntitiesResponse
	(*v1.Identifier)(nil),            // 4: synq.entities.v1.Identifier
	(*v1.Entity)(nil),                // 5: synq.entities.v1.Entity
}
var file_synq_entities_entities_v1_entities_service_proto_depIdxs = []int32{
	4, // 0: synq.entities.entities.v1.GetEntityRequest.id:type_name -> synq.entities.v1.Identifier
	5, // 1: synq.entities.entities.v1.GetEntityResponse.entity:type_name -> synq.entities.v1.Entity
	4, // 2: synq.entities.entities.v1.BatchGetEntitiesRequest.ids:type_name -> synq.entities.v1.Identifier
	5, // 3: synq.entities.entities.v1.BatchGetEntitiesResponse.entities:type_name -> synq.entities.v1.Entity
	0, // 4: synq.entities.entities.v1.EntitiesService.GetEntity:input_type -> synq.entities.entities.v1.GetEntityRequest
	2, // 5: synq.entities.entities.v1.EntitiesService.BatchGetEntities:input_type -> synq.entities.entities.v1.BatchGetEntitiesRequest
	1, // 6: synq.entities.entities.v1.EntitiesService.GetEntity:output_type -> synq.entities.entities.v1.GetEntityResponse
	3, // 7: synq.entities.entities.v1.EntitiesService.BatchGetEntities:output_type -> synq.entities.entities.v1.BatchGetEntitiesResponse
	6, // [6:8] is the sub-list for method output_type
	4, // [4:6] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_synq_entities_entities_v1_entities_service_proto_init() }
func file_synq_entities_entities_v1_entities_service_proto_init() {
	if File_synq_entities_entities_v1_entities_service_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_synq_entities_entities_v1_entities_service_proto_rawDesc), len(file_synq_entities_entities_v1_entities_service_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_synq_entities_entities_v1_entities_service_proto_goTypes,
		DependencyIndexes: file_synq_entities_entities_v1_entities_service_proto_depIdxs,
		MessageInfos:      file_synq_entities_entities_v1_entities_service_proto_msgTypes,
	}.Build()
	File_synq_entities_entities_v1_entities_service_proto = out.File
	file_synq_entities_entities_v1_entities_service_proto_goTypes = nil
	file_synq_entities_entities_v1_entities_service_proto_depIdxs = nil
}
