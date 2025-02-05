package service

import (
	"context"

	agentdwhv1 "github.com/getsynq/api/agent/dwh/v1"
)

type Reporter interface {
	Report(ctx context.Context, databaseId string, severity agentdwhv1.LogSeverity, message string) error
}
