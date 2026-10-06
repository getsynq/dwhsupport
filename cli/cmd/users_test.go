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
		users.Skip(scrapper.PlatformUserFactEmail, "the platform keeps no email")
		reportPlatformUsers(users.Finish(), "GRANT X")

		want := "p.view: complete\np.view: skipped email: the platform keeps no email\n"
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

	t.Run("a refused source says so and nothing else", func(t *testing.T) {
		buf := captureErrOut(t)
		reportPlatformUsers(scrapper.RefusedPlatformUserSource("p.api", scrapper.PlatformUserSourceAPI, errors.New("403")), "GRANT X")

		if want := "p.api: refused (403)\n"; buf.String() != want {
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
