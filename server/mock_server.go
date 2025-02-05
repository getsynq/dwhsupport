package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	agentdwhv1grpc "github.com/getsynq/api/agent/dwh/v1"
	ingestdwhv1 "github.com/getsynq/api/ingest/dwh/v1"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func StartMockServer(ctx context.Context) {

	go startMockServerInternal(ctx)
}

func startMockServerInternal(ctx context.Context) {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		logrus.Fatalf("failed to listen on port 50051: %v", err)
	}

	var serverOpts []grpc.ServerOption
	s := grpc.NewServer(serverOpts...)
	agentdwhv1grpc.RegisterDwhAgentServiceServer(s, &dwhAgentServiceServerMock{})
	ingestdwhv1.RegisterDwhServiceServer(s, &dwhServiceServerMock{})

	// Create channel for server errors
	errChan := make(chan error, 1)

	// Start server in a goroutine
	go func() {
		logrus.Info("listening on ", lis.Addr())
		if err := s.Serve(lis); err != nil {
			errChan <- err
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		logrus.Info("shutting down mock server...")
		s.GracefulStop()
	case err := <-errChan:
		logrus.Fatalf("failed to serve: %v", err)
	}
}

var _ agentdwhv1grpc.DwhAgentServiceServer = &dwhAgentServiceServerMock{}

type dwhAgentServiceServerMock struct {
	agentdwhv1grpc.UnimplementedDwhAgentServiceServer
}

func (m dwhAgentServiceServerMock) Connect(g grpc.BidiStreamingServer[agentdwhv1grpc.ConnectRequest, agentdwhv1grpc.ConnectResponse]) error {
	log := logrus.WithField("server", true)
	for {
		req, err := g.Recv()
		if err != nil {
			return err
		}
		log.Info(req)
		switch t := req.Message.(type) {
		case *agentdwhv1grpc.ConnectRequest_Hello:
			log.Info(t.Hello)
			for i := 0; i < 10; i++ {
				var tasks []*agentdwhv1grpc.AgentTask
				for _, connection := range t.Hello.AvailableConnections {
					tasks = append(tasks, &agentdwhv1grpc.AgentTask{
						ConnectionId: connection.ConnectionId,
						TaskId:       uuid.NewString(),
						Command: &agentdwhv1grpc.AgentTask_FetchFullCatalog{
							FetchFullCatalog: &agentdwhv1grpc.FetchFullCatalogCommand{},
						},
					})
					tasks = append(tasks, &agentdwhv1grpc.AgentTask{
						ConnectionId: connection.ConnectionId,
						TaskId:       uuid.NewString(),
						Command: &agentdwhv1grpc.AgentTask_FetchFullMetrics{
							FetchFullMetrics: &agentdwhv1grpc.FetchFullMetricsCommand{},
						},
					})
				}
				if err := g.Send(&agentdwhv1grpc.ConnectResponse{
					Tasks: tasks,
				}); err != nil {
					return err
				}
			}
		}
	}
}

var _ ingestdwhv1.DwhServiceServer = &dwhServiceServerMock{}

type dwhServiceServerMock struct {
	ingestdwhv1.UnimplementedDwhServiceServer
}

func (d dwhServiceServerMock) IngestTableInformation(ctx context.Context, request *ingestdwhv1.IngestTableInformationRequest) (*ingestdwhv1.IngestTableInformationResponse, error) {
	d.dumpRequest(request)
	return &ingestdwhv1.IngestTableInformationResponse{}, nil
}

func (d dwhServiceServerMock) IngestSqlDefinitions(ctx context.Context, request *ingestdwhv1.IngestSqlDefinitionsRequest) (*ingestdwhv1.IngestSqlDefinitionsResponse, error) {
	d.dumpRequest(request)
	return &ingestdwhv1.IngestSqlDefinitionsResponse{}, nil
}

func (d dwhServiceServerMock) IngestSchemas(ctx context.Context, request *ingestdwhv1.IngestSchemasRequest) (*ingestdwhv1.IngestSchemasResponse, error) {
	d.dumpRequest(request)
	return &ingestdwhv1.IngestSchemasResponse{}, nil
}

type IngestRequest interface {
	proto.Message
	GetConnectionId() string
	GetUploadId() string
	GetStateAt() *timestamppb.Timestamp
}

func (d dwhServiceServerMock) dumpRequest(request IngestRequest) {

	fileName := fmt.Sprintf("ingest_%T_%s_%s_%s_%s.json", request, request.GetConnectionId(), request.GetUploadId(), request.GetStateAt().AsTime().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339Nano))

	marshaller := protojson.MarshalOptions{
		Multiline: true,
	}
	if err := os.WriteFile(fileName, []byte(marshaller.Format(request)), 0644); err != nil {
		logrus.Errorf("failed to write file %s: %v", fileName, err)
	}
}
