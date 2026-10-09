package cmd

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/cli/internal/output"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/spf13/cobra"
)

var flagUsersBySource bool

var usersCmd = &cobra.Command{
	Use:     "users",
	Aliases: []string{"platform-users", "logins"},
	Short:   "List the platform's users (logins) with type, email, roles and other facts",
	Long: `List the platform's users with every fact the platform states: type,
email, display name, disabled, created, last login, default role and roles.

A platform may have several sources of users (a catalog view and a command, the
warehouse and its API). By default they are reconciled into one list; with
--by-source each source's own rows are printed, with a source column.

How complete each listing is, which facts could not be read and why, go to
stderr. A dialect without a user listing reports so and prints nothing; a role
that may not read any source fails with the platform's permission error.`,
	Example: "  dwhctl users --config conn.yaml\n" +
		"  dwhctl users -c conn.yaml -o json --wide\n" +
		"  dwhctl users -c conn.yaml --by-source",
	RunE: func(cmd *cobra.Command, _ []string) error {
		return withScrapper(cmd, func(ctx context.Context, s scrapper.Scrapper) error {
			if !s.Capabilities().PlatformUsers.Supported {
				return emitListErr("users", platformUserColumns, []*scrapper.PlatformUser(nil), scrapper.ErrUnsupported)
			}
			result, err := s.QueryPlatformUsers(ctx)
			if err != nil {
				return emitListErr("users", platformUserColumns, []*scrapper.PlatformUser(nil), err)
			}
			grant := s.Capabilities().PlatformUsers.Grant
			if flagUsersBySource {
				var rows []sourcedPlatformUser
				for _, src := range result.Sources {
					reportPlatformUsers(src, grant)
					for _, u := range src.Users {
						rows = append(rows, sourcedPlatformUser{Source: src.Source, PlatformUser: u})
					}
				}
				return emitList("users", rows, sourcedPlatformUserColumns)
			}
			reconciled := result.Reconcile()
			reportPlatformUsers(reconciled, grant)
			return emitList("users", reconciled.Users, platformUserColumns)
		})
	},
}

// sourcedPlatformUser is a user as one source listed it.
type sourcedPlatformUser struct {
	Source string `json:"source"`
	*scrapper.PlatformUser
}

// reportPlatformUsers writes what a listing says about itself to stderr, so
// stdout stays the list of users alone.
func reportPlatformUsers(users *scrapper.PlatformUserListing, grant string) {
	switch {
	case users.Refused != "":
		fmt.Fprintf(output.ErrOut, "%s: refused (%s)\n", users.Source, users.Refused)
		fmt.Fprintf(output.ErrOut, "%s: grant for the full listing: %s\n", users.Source, grant)
		return
	case users.Unavailable != "":
		fmt.Fprintf(output.ErrOut, "%s: unavailable on this platform (%s)\n", users.Source, users.Unavailable)
		return
	case users.Failed != "":
		fmt.Fprintf(output.ErrOut, "%s: failed (%s)\n", users.Source, users.Failed)
		return
	}
	line := users.Source + ": " + string(users.Completeness)
	if users.CompletenessReason != "" {
		line += " (" + users.CompletenessReason + ")"
	}
	fmt.Fprintln(output.ErrOut, line)
	// A grant is named only when one would help: the listing is incomplete,
	// or a fact was refused rather than missing from the platform.
	needsGrant := users.Completeness != scrapper.PlatformUsersComplete
	for _, f := range users.SkippedFacts {
		fmt.Fprintf(output.ErrOut, "%s: skipped %s (%s): %s\n", users.Source, f.Fact, f.Kind, f.Reason)
		needsGrant = needsGrant || f.Kind == scrapper.PlatformUserSkipRefused
	}
	if needsGrant {
		fmt.Fprintf(output.ErrOut, "%s: grant for the full listing: %s\n", users.Source, grant)
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

var sourcedPlatformUserColumns = append(output.Columns{{Header: "source", Path: ".source", Default: true}}, platformUserColumns...)

func init() {
	usersCmd.Flags().BoolVar(&flagUsersBySource, "by-source", false, "print each source's own users instead of the reconciled list")
	rootCmd.AddCommand(usersCmd)
}
