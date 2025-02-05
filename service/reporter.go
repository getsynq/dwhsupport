package service

import (
	"context"

	agentdwhv1 "github.com/getsynq/api/agent/dwh/v1"
)

type LogReporter interface {
	Report(ctx context.Context, connectionId string, severity agentdwhv1.LogLevel, message string) error
}
