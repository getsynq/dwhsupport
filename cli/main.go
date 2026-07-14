// Command dwhctl is a universal command-line interface over the Coalesce Quality dwhsupport
// Scrapper interface. It lets any team — regardless of language — reuse the same
// warehouse catalog and metadata-metric extraction machinery through a single
// binary, with structured, automation-friendly output.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/getsynq/dwhsupport/cli/cmd"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cmd.ExecuteContext(ctx)
}
