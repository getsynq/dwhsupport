// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             (unknown)
// source: synq/entities/custom/v1/checks_relationships_service.proto

package v1

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	ChecksRelationshipsService_UpsertCheckRelationships_FullMethodName = "/synq.entities.custom.v1.ChecksRelationshipsService/UpsertCheckRelationships"
	ChecksRelationshipsService_DeleteCheckRelationships_FullMethodName = "/synq.entities.custom.v1.ChecksRelationshipsService/DeleteCheckRelationships"
)

// ChecksRelationshipsServiceClient is the client API for ChecksRelationshipsService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ChecksRelationshipsServiceClient interface {
	UpsertCheckRelationships(ctx context.Context, in *UpsertCheckRelationshipsRequest, opts ...grpc.CallOption) (*UpsertCheckRelationshipsResponse, error)
	DeleteCheckRelationships(ctx context.Context, in *DeleteCheckRelationshipsRequest, opts ...grpc.CallOption) (*DeleteCheckRelationshipsResponse, error)
}

type checksRelationshipsServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewChecksRelationshipsServiceClient(cc grpc.ClientConnInterface) ChecksRelationshipsServiceClient {
	return &checksRelationshipsServiceClient{cc}
}

func (c *checksRelationshipsServiceClient) UpsertCheckRelationships(ctx context.Context, in *UpsertCheckRelationshipsRequest, opts ...grpc.CallOption) (*UpsertCheckRelationshipsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(UpsertCheckRelationshipsResponse)
	err := c.cc.Invoke(ctx, ChecksRelationshipsService_UpsertCheckRelationships_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *checksRelationshipsServiceClient) DeleteCheckRelationships(ctx context.Context, in *DeleteCheckRelationshipsRequest, opts ...grpc.CallOption) (*DeleteCheckRelationshipsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(DeleteCheckRelationshipsResponse)
	err := c.cc.Invoke(ctx, ChecksRelationshipsService_DeleteCheckRelationships_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ChecksRelationshipsServiceServer is the server API for ChecksRelationshipsService service.
// All implementations must embed UnimplementedChecksRelationshipsServiceServer
// for forward compatibility.
type ChecksRelationshipsServiceServer interface {
	UpsertCheckRelationships(context.Context, *UpsertCheckRelationshipsRequest) (*UpsertCheckRelationshipsResponse, error)
	DeleteCheckRelationships(context.Context, *DeleteCheckRelationshipsRequest) (*DeleteCheckRelationshipsResponse, error)
	mustEmbedUnimplementedChecksRelationshipsServiceServer()
}

// UnimplementedChecksRelationshipsServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedChecksRelationshipsServiceServer struct{}

func (UnimplementedChecksRelationshipsServiceServer) UpsertCheckRelationships(context.Context, *UpsertCheckRelationshipsRequest) (*UpsertCheckRelationshipsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpsertCheckRelationships not implemented")
}
func (UnimplementedChecksRelationshipsServiceServer) DeleteCheckRelationships(context.Context, *DeleteCheckRelationshipsRequest) (*DeleteCheckRelationshipsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteCheckRelationships not implemented")
}
func (UnimplementedChecksRelationshipsServiceServer) mustEmbedUnimplementedChecksRelationshipsServiceServer() {
}
func (UnimplementedChecksRelationshipsServiceServer) testEmbeddedByValue() {}

// UnsafeChecksRelationshipsServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ChecksRelationshipsServiceServer will
// result in compilation errors.
type UnsafeChecksRelationshipsServiceServer interface {
	mustEmbedUnimplementedChecksRelationshipsServiceServer()
}

func RegisterChecksRelationshipsServiceServer(s grpc.ServiceRegistrar, srv ChecksRelationshipsServiceServer) {
	// If the following call pancis, it indicates UnimplementedChecksRelationshipsServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&ChecksRelationshipsService_ServiceDesc, srv)
}

func _ChecksRelationshipsService_UpsertCheckRelationships_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpsertCheckRelationshipsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChecksRelationshipsServiceServer).UpsertCheckRelationships(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ChecksRelationshipsService_UpsertCheckRelationships_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChecksRelationshipsServiceServer).UpsertCheckRelationships(ctx, req.(*UpsertCheckRelationshipsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ChecksRelationshipsService_DeleteCheckRelationships_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteCheckRelationshipsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ChecksRelationshipsServiceServer).DeleteCheckRelationships(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ChecksRelationshipsService_DeleteCheckRelationships_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ChecksRelationshipsServiceServer).DeleteCheckRelationships(ctx, req.(*DeleteCheckRelationshipsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ChecksRelationshipsService_ServiceDesc is the grpc.ServiceDesc for ChecksRelationshipsService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ChecksRelationshipsService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "synq.entities.custom.v1.ChecksRelationshipsService",
	HandlerType: (*ChecksRelationshipsServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "UpsertCheckRelationships",
			Handler:    _ChecksRelationshipsService_UpsertCheckRelationships_Handler,
		},
		{
			MethodName: "DeleteCheckRelationships",
			Handler:    _ChecksRelationshipsService_DeleteCheckRelationships_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "synq/entities/custom/v1/checks_relationships_service.proto",
}
