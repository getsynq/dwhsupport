package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	agentdwhv1grpc "github.com/getsynq/api/agent/dwh/v1"
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
		logrus.Panic(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)
	defer cancel()

	server.StartMockServer(ctx)

	grpcConnection, err := synq.NewGrpcConnection(ctx, conf)
	if err != nil {
		logrus.Panic(err)
	}

	agentServiceClient := agentdwhv1grpc.NewDwhAgentServiceClient(grpcConnection)

	// Create and start connection service
	connectionService := service.NewConnectionService(agentServiceClient, conf)
	connectionService.Start()
	defer connectionService.Stop()

	// Create work pool with workers for each database connection
	workPool := service.NewWorkPool(conf.Connections)
	defer workPool.Stop()

	// Process messages from the queue and distribute to appropriate workers
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-connectionService.GetMessageChan():
			// Get the appropriate worker for the database connection
			for _, task := range msg.GetTasks() {
				worker := workPool.GetWorker(task.ConnectionId)
				if worker != nil {
					worker.EnqueueMessage(task)
				} else {
					logrus.Errorf("No worker found for database ID: %s", task.ConnectionId)
				}
			}
		}
	}
}
