package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	agentdwhv1grpc "github.com/getsynq/api/agent/dwh/v1"
	ingestdwhv1 "github.com/getsynq/api/ingest/dwh/v1"
	"github.com/getsynq/synq-dwh/config"
	"github.com/getsynq/synq-dwh/server"
	"github.com/getsynq/synq-dwh/service"
	"github.com/getsynq/synq-dwh/synq"
	"github.com/sirupsen/logrus"
)

//go:generate bash build/version.sh
func main() {

	conf, err := config.LoadConfig()
	if err != nil {
		config.ExplainError(err)
		os.Exit(1)
	}
	setupLogger(conf.GetAgent())

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)
	defer cancel()

	server.StartMockServer(ctx)

	grpcConnection, err := synq.NewGrpcConnection(ctx, conf)
	if err != nil {
		logrus.Panic(err)
	}

	agentServiceClient := agentdwhv1grpc.NewDwhAgentServiceClient(grpcConnection)
	dwhServiceClient := ingestdwhv1.NewDwhServiceClient(grpcConnection)

	// Create and start connection service
	connectionService := service.NewConnectionService(agentServiceClient, conf)
	connectionService.Start()
	defer connectionService.Stop()

	// Create work pool with workers for each database connection
	workPool := service.NewWorkPool(conf.Connections, dwhServiceClient)
	defer workPool.Stop()

	// Process messages from the queue and distribute to appropriate workers
	for {
		select {
		case <-ctx.Done():
			connectionService.Stop()
			return
		case msg := <-connectionService.GetMessageChan():
			// Get the appropriate worker for the database connection
			for _, task := range msg.GetTasks() {
				command := taskToDatabaseCommand(task)
				if command == nil {
					continue
				}
				worker := workPool.GetWorker(task.ConnectionId)
				if worker != nil {
					worker.EnqueueMessage(command)
				} else {
					logrus.Errorf("No worker found for database ID: %s", task.ConnectionId)
				}
			}
		}
	}
}

func setupLogger(agentConf *agentdwhv1grpc.Config_Agent) {
	if agentConf.GetLogReportCaller() {
		logrus.SetReportCaller(true)
	}
	if agentConf.GetLogJson() {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}

	switch agentConf.GetLogLevel() {
	case agentdwhv1grpc.Config_Agent_LOG_LEVEL_DEBUG:
		logrus.SetLevel(logrus.DebugLevel)
	case agentdwhv1grpc.Config_Agent_LOG_LEVEL_TRACE:
		logrus.SetLevel(logrus.TraceLevel)
	case agentdwhv1grpc.Config_Agent_LOG_LEVEL_INFO:
		logrus.SetLevel(logrus.InfoLevel)
	case agentdwhv1grpc.Config_Agent_LOG_LEVEL_WARN:
		logrus.SetLevel(logrus.WarnLevel)
	case agentdwhv1grpc.Config_Agent_LOG_LEVEL_ERROR:
		logrus.SetLevel(logrus.ErrorLevel)
	}
}

func taskToDatabaseCommand(task *agentdwhv1grpc.AgentTask) service.DatabaseCommand {
	if task == nil || task.Command == nil {
		return nil
	}
	switch task.Command.(type) {
	case *agentdwhv1grpc.AgentTask_FetchFullCatalog:
		return &service.FetchFullCatalog{}
	case *agentdwhv1grpc.AgentTask_FetchFullMetrics:
		return &service.FetchFullMetrics{}
	default:
		logrus.Errorf("Unknown command: %v, you need to upgrade to latest version of synq-dwh", task.Command)
		return nil
	}

}
