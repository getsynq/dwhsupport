package cmd

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/cli/internal/output"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// withScrapper builds a scrapper from the connection config, runs fn, and
// closes the scrapper. It is the common preamble for every warehouse command.
func withScrapper(cmd *cobra.Command, fn func(ctx context.Context, s scrapper.Scrapper) error) error {
	ctx := cmd.Context()
	s, err := buildScrapper(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := s.Close(); cerr != nil {
			logrus.WithError(cerr).Warn("failed to close scrapper")
		}
	}()
	return fn(ctx, s)
}

// emitList prints a list of rows with the given columns, or an explicit empty
// notice to stderr so an agent can tell "no data" apart from a silent failure.
func emitList[T any](what string, items []T, cols output.Columns) error {
	if len(items) == 0 {
		fmt.Fprintf(output.ErrOut, "0 %s\n", what)
	}
	return output.PrintList(items, cols)
}
