package config

import (
	"fmt"
	"strings"

	"github.com/bufbuild/protovalidate-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func ExplainError(err error) {

	var validationErr *protovalidate.ValidationError
	if errors.As(err, &validationErr) {
		logrus.Error("config failed validations")
		for _, violation := range validationErr.Violations {
			bldr := &strings.Builder{}
			bldr.WriteString(" - ")
			if fieldPath := protovalidate.FieldPathString(violation.Proto.GetField()); fieldPath != "" {
				bldr.WriteString(fieldPath)
				bldr.WriteString(": ")
			}
			_, _ = fmt.Fprintf(bldr, "%s [%s]",
				violation.Proto.GetMessage(),
				violation.Proto.GetConstraintId())
			logrus.Error(bldr.String())
		}
		return
	}
	logrus.Error(err)
}
