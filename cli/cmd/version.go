package cmd

import (
	"runtime"

	"github.com/getsynq/dwhsupport/cli/internal/output"
	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the dwhctl version",
	RunE: func(*cobra.Command, []string) error {
		return output.Print(map[string]any{
			"version": version,
			"go":      runtime.Version(),
			"os":      runtime.GOOS,
			"arch":    runtime.GOARCH,
		}, output.Columns{
			{Header: "version", Path: ".version", Default: true},
			{Header: "go", Path: ".go", Default: true},
			{Header: "os", Path: ".os", Default: true},
			{Header: "arch", Path: ".arch", Default: true},
		})
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
