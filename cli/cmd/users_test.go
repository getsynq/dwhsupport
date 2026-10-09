package cmd

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/getsynq/dwhsupport/cli/internal/output"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
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
		users := &scrapper.PlatformUserListing{
			Source:       "p.view",
			Completeness: scrapper.PlatformUsersComplete,
			Users:        []*scrapper.PlatformUser{{Login: "A"}},
		}
		users.Skip(scrapper.PlatformUserFactEmail, scrapper.PlatformUserSkipUnavailable, "the platform keeps no email")
		users.Skip(scrapper.PlatformUserFactRoles, scrapper.PlatformUserSkipFailed, "timeout")
		reportPlatformUsers(users.Finish(), "GRANT X")

		want := "p.view: complete\n" +
			"p.view: skipped email (unavailable): the platform keeps no email\n" +
			"p.view: skipped roles (failed): timeout\n"
		if buf.String() != want {
			t.Fatalf("got %q, want %q", buf.String(), want)
		}
	})

	t.Run("a complete listing with a refused fact names the grant", func(t *testing.T) {
		buf := captureErrOut(t)
		users := &scrapper.PlatformUserListing{
			Source:       "p.view",
			Completeness: scrapper.PlatformUsersComplete,
			Users:        []*scrapper.PlatformUser{{Login: "A"}},
		}
		users.Skip(scrapper.PlatformUserFactRoles, scrapper.PlatformUserSkipRefused, "role grants were refused")
		reportPlatformUsers(users.Finish(), "GRANT X")

		want := "p.view: complete\n" +
			"p.view: skipped roles (refused): role grants were refused\n" +
			"p.view: grant for the full listing: GRANT X\n"
		if buf.String() != want {
			t.Fatalf("got %q, want %q", buf.String(), want)
		}
	})

	t.Run("an incomplete listing says why and names the grant", func(t *testing.T) {
		buf := captureErrOut(t)
		reportPlatformUsers(scrapper.NewPlatformUsers().Reconcile(), "GRANT X")

		for _, want := range []string{"reconciled: empty (", "reconciled: grant for the full listing: GRANT X\n"} {
			if !strings.Contains(buf.String(), want) {
				t.Errorf("%q does not contain %q", buf.String(), want)
			}
		}
	})

	t.Run("a refused source says so and names the grant", func(t *testing.T) {
		buf := captureErrOut(t)
		reportPlatformUsers(scrapper.RefusedPlatformUserSource("p.api", scrapper.PlatformUserSourceAPI, errors.New("403")), "GRANT X")

		if want := "p.api: refused (403)\np.api: grant for the full listing: GRANT X\n"; buf.String() != want {
			t.Fatalf("got %q, want %q", buf.String(), want)
		}
	})

	t.Run("an unavailable or failed source names no grant", func(t *testing.T) {
		buf := captureErrOut(t)
		reportPlatformUsers(scrapper.UnavailablePlatformUserSource("p.old", scrapper.PlatformUserSourceSQL, errors.New("no such view")), "GRANT X")
		reportPlatformUsers(scrapper.PlatformUserSourceError("p.api", scrapper.PlatformUserSourceAPI, errors.New("503"), nil, nil), "GRANT X")

		want := "p.old: unavailable on this platform (no such view)\np.api: failed (503)\n"
		if buf.String() != want {
			t.Fatalf("got %q, want %q", buf.String(), want)
		}
	})
}

func TestSourcedPlatformUserJSON(t *testing.T) {
	data, err := json.Marshal(sourcedPlatformUser{Source: "p.view", PlatformUser: &scrapper.PlatformUser{Login: "A", Roles: []string{"R"}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"source":"p.view","login":"A","roles":["R"]}`; string(data) != want {
		t.Fatalf("got %s, want %s", data, want)
	}
}
