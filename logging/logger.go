package logging

import (
	"context"

	"github.com/sirupsen/logrus"
)

type (
	loggerKey struct{}
)

func WithLogger(ctx context.Context, logger logrus.FieldLogger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

func GetLogger(ctx context.Context) logrus.FieldLogger {
	v := ctx.Value(loggerKey{})
	if v == nil {
		return logrus.StandardLogger()
	}
	return v.(logrus.FieldLogger)
}
