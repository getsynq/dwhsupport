package service

import (
	"context"
	"sync"
	"time"

	agentdwhv1 "github.com/getsynq/api/agent/dwh/v1"
	"github.com/getsynq/synq-dwh/build"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	reconnectDelay = 5 * time.Second
	messageBuffer  = 1000
)

type ConnectionService struct {
	client    agentdwhv1.DwhAgentServiceClient
	config    *agentdwhv1.Config
	msgQueue  chan *agentdwhv1.ConnectResponse
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	isRunning bool
}

func NewConnectionService(client agentdwhv1.DwhAgentServiceClient, config *agentdwhv1.Config) *ConnectionService {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConnectionService{
		client:   client,
		config:   config,
		msgQueue: make(chan *agentdwhv1.ConnectResponse, messageBuffer),
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

func (s *ConnectionService) GetMessageChan() <-chan *agentdwhv1.ConnectResponse {
	return s.msgQueue
}

func (s *ConnectionService) maintainConnection() {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
			if err := s.connect(); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
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
	err = bidi.Send(&agentdwhv1.ConnectRequest{
		Message: &agentdwhv1.ConnectRequest_Hello{
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
			logrus.WithField("message", messageDump(msg)).Error("Message queue full, dropping message")
		}
	}
}

func messageDump(msg proto.Message) string {
	return protojson.Format(msg)
}

func createHelloMessage(conf *agentdwhv1.Config) *agentdwhv1.Hello {
	return &agentdwhv1.Hello{
		Name:                 conf.GetAgent().GetName(),
		BuildVersion:         build.Version,
		BuildTime:            build.Time,
		AvailableConnections: configConnectionsToAvailableConnections(conf),
	}
}

func configConnectionsToAvailableConnections(conf *agentdwhv1.Config) []*agentdwhv1.Hello_AvailableConnection {
	var res []*agentdwhv1.Hello_AvailableConnection
	for connectionId, connection := range conf.GetConnections() {
		res = append(res, createAvailableConnection(connectionId, connection))
	}
	return res
}

func createAvailableConnection(connectionId string, connection *agentdwhv1.Config_Connection) *agentdwhv1.Hello_AvailableConnection {
	conn := &agentdwhv1.Hello_AvailableConnection{
		ConnectionId: connectionId,
		Name:         connection.GetName(),
		Disabled:     connection.GetDisabled(),
	}

	switch t := connection.Config.(type) {
	case *agentdwhv1.Config_Connection_Bigquery:
		conn.Instance = t.Bigquery.GetProjectId()
		conn.Type = "bigquery"
	case *agentdwhv1.Config_Connection_Clickhouse:
		conn.Instance = t.Clickhouse.GetHost()
		conn.Type = "clickhouse"
	case *agentdwhv1.Config_Connection_Databricks:
		conn.Instance = t.Databricks.GetWorkspaceUrl()
		conn.Type = "databricks"
	case *agentdwhv1.Config_Connection_Mysql:
		conn.Instance = t.Mysql.GetHost()
		conn.Type = "mysql"
		conn.Databases = append(conn.Databases, t.Mysql.GetDatabase())
	case *agentdwhv1.Config_Connection_Postgres:
		conn.Instance = t.Postgres.GetHost()
		conn.Type = "postgres"
		conn.Databases = append(conn.Databases, t.Postgres.GetDatabase())
	case *agentdwhv1.Config_Connection_Redshift:
		conn.Instance = t.Redshift.GetHost()
		conn.Type = "redshift"
		conn.Databases = append(conn.Databases, t.Redshift.GetDatabase())
	case *agentdwhv1.Config_Connection_Snowflake:
		conn.Instance = t.Snowflake.GetAccount()
		conn.Type = "snowflake"
		conn.Databases = append(conn.Databases, t.Snowflake.GetDatabases()...)
	}

	return conn
}
