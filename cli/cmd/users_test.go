package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/getsynq/dwhsupport/cli/internal/output"
	"github.com/getsynq/dwhsupport/scrapper"
)

func captureErrOut(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := output.ErrOut
	output.ErrOut = &buf
	t.Cleanup(func() { output.ErrOut = prev })
	return &buf
}

func TestReportPlatformUsers(t *testing.T) {
	t.Run("a complete listing names no grant", func(t *testing.T) {
		buf := captureErrOut(t)
		users := &scrapper.PlatformUsers{
			Completeness: scrapper.PlatformUsersComplete,
			Users:        []*scrapper.PlatformUser{{Login: "A"}},
		}
		users.Skip(scrapper.PlatformUserFactEmail, "the platform keeps no email")
		reportPlatformUsers(users.Finish(), "GRANT X")

		want := "users: complete\nskipped email: the platform keeps no email\n"
		if buf.String() != want {
			t.Fatalf("got %q, want %q", buf.String(), want)
		}
	})

	t.Run("an incomplete listing says why and names the grant", func(t *testing.T) {
		buf := captureErrOut(t)
		reportPlatformUsers((&scrapper.PlatformUsers{Completeness: scrapper.PlatformUsersComplete}).Finish(), "GRANT X")

		for _, want := range []string{"users: empty (", "grant for the full listing: GRANT X\n"} {
			if !strings.Contains(buf.String(), want) {
				t.Errorf("%q does not contain %q", buf.String(), want)
			}
		}
	})
}
