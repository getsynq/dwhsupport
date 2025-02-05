package server

import (
	"context"
	"net"

	agentdwhv1grpc "github.com/getsynq/api/agent/dwh/v1"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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
	agentdwhv1grpc.RegisterDwhAgentServiceServer(s, &mockServer{})

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

var _ agentdwhv1grpc.DwhAgentServiceServer = &mockServer{}

type mockServer struct {
	agentdwhv1grpc.UnimplementedDwhAgentServiceServer
}

func (m mockServer) Connect(g grpc.BidiStreamingServer[agentdwhv1grpc.ConnectRequest, agentdwhv1grpc.ConnectResponse]) error {
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
