package service

import (
	"context"
	"sync"
	"time"

	agentdwhv1grpc "github.com/getsynq/api/agent/dwh/v1"
	"github.com/getsynq/synq-dwh/build"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	reconnectDelay = 5 * time.Second
	messageBuffer  = 1000
)

type ConnectionService struct {
	client    agentdwhv1grpc.DwhAgentServiceClient
	config    *agentdwhv1grpc.Config
	msgQueue  chan *agentdwhv1grpc.ConnectResponse
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	isRunning bool
}

func NewConnectionService(client agentdwhv1grpc.DwhAgentServiceClient, config *agentdwhv1grpc.Config) *ConnectionService {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConnectionService{
		client:   client,
		config:   config,
		msgQueue: make(chan *agentdwhv1grpc.ConnectResponse, messageBuffer),
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (s *ConnectionService) Start() {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return
	}
	s.isRunning = true
	s.mu.Unlock()

	go s.maintainConnection()
}

func (s *ConnectionService) Stop() {
	s.cancel()
}

func (s *ConnectionService) GetMessageChan() <-chan *agentdwhv1grpc.ConnectResponse {
	return s.msgQueue
}

func (s *ConnectionService) maintainConnection() {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			if err := s.connect(); err != nil {
				logrus.Errorf("Connection error: %v, reconnecting in %v", err, reconnectDelay)
				time.Sleep(reconnectDelay)
				continue
			}
		}
	}
}

func (s *ConnectionService) connect() error {
	bidi, err := s.client.Connect(s.ctx)
	if err != nil {
		return err
	}
	defer bidi.CloseSend()

	// Send initial hello message
	err = bidi.Send(&agentdwhv1grpc.ConnectRequest{
		Message: &agentdwhv1grpc.ConnectRequest_Hello{
			Hello: createHelloMessage(s.config),
		},
	})
	if err != nil {
		return err
	}

	// Start receiving messages
	for {
		msg, err := bidi.Recv()
		if err != nil {
			return err
		}
		logrus.WithField("message", messageDump(msg)).Debug("Received message")

		select {
		case s.msgQueue <- msg:
		default:
			logrus.WithField("message", messageDump(msg)).Warn("Message queue full, dropping message")
		}
	}
}

func messageDump(msg proto.Message) string {
	return protojson.Format(msg)
}

func createHelloMessage(conf *agentdwhv1grpc.Config) *agentdwhv1grpc.Hello {
	return &agentdwhv1grpc.Hello{
		Name:                 conf.GetAgent().GetName(),
		BuildVersion:         build.Version,
		BuildTime:            build.Time,
		AvailableConnections: configConnectionsToAvailableConnections(conf),
	}
}

func configConnectionsToAvailableConnections(conf *agentdwhv1grpc.Config) []*agentdwhv1grpc.Hello_AvailableConnection {
	var res []*agentdwhv1grpc.Hello_AvailableConnection
	for connectionId, connection := range conf.GetConnections() {
		res = append(res, &agentdwhv1grpc.Hello_AvailableConnection{
			ConnectionId: connectionId,
			Name:         connection.GetName(),
			Instance:     "",
			Databases:    nil,
		})
	}
	return res
}
