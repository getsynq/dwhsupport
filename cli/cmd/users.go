package cmd

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/cli/internal/output"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/spf13/cobra"
)

var usersCmd = &cobra.Command{
	Use:     "users",
	Aliases: []string{"platform-users", "logins"},
	Short:   "List the platform's users (logins) with type, email, roles and other facts",
	Long: `List the platform's users with every fact the platform states: type,
email, display name, disabled, created, last login, default role and roles.

How complete the listing is, which facts could not be read and why, go to
stderr. A dialect without a user listing reports so and prints nothing; a role
that may not read the listing fails with the platform's permission error.`,
	Example: "  dwhctl users --config conn.yaml\n" +
		"  dwhctl users -c conn.yaml -o json --wide",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			if !s.Capabilities().PlatformUsers.Supported {
				return emitListErr("users", platformUserColumns, []*scrapper.PlatformUser(nil), scrapper.ErrUnsupported)
			}
			users, err := s.QueryPlatformUsers(ctx)
			if err != nil {
				return emitListErr("users", platformUserColumns, []*scrapper.PlatformUser(nil), err)
			}
			reportPlatformUsers(users, s.Capabilities().PlatformUsers.Grant)
			return emitList("users", users.Users, platformUserColumns)
		})
	},
}

// reportPlatformUsers writes what the listing says about itself to stderr, so
// stdout stays the list of users alone.
func reportPlatformUsers(users *scrapper.PlatformUsers, grant string) {
	line := "users: " + string(users.Completeness)
	if users.CompletenessReason != "" {
		line += " (" + users.CompletenessReason + ")"
	}
	fmt.Fprintln(output.ErrOut, line)
	for _, f := range users.SkippedFacts {
		fmt.Fprintf(output.ErrOut, "skipped %s: %s\n", f.Fact, f.Reason)
	}
	if users.Completeness != scrapper.PlatformUsersComplete {
		fmt.Fprintf(output.ErrOut, "grant for the full listing: %s\n", grant)
	}
}

var platformUserColumns = output.Columns{
	{Header: "login", Path: ".login", Default: true},
	{Header: "type", Path: ".type", Default: true},
	{Header: "email", Path: ".email", Default: true},
	{Header: "disabled", Path: ".disabled", Default: true},
	{Header: "roles", Path: ".roles", Default: true},
	{Header: "default_role", Path: ".default_role", Default: false},
	{Header: "display_name", Path: ".display_name", Default: false},
	{Header: "last_login_at", Path: ".last_login_at", Default: false},
	{Header: "created_at", Path: ".created_at", Default: false},
	{Header: "platform_id", Path: ".platform_id", Default: false},
	{Header: "comment", Path: ".comment", Default: false},
}

func init() {
	rootCmd.AddCommand(usersCmd)
}
