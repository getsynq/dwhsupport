package output

import (
	"os"

	"github.com/pkg/errors"
)

// UseFile redirects rendered output (Out) to the file at path, creating or
// truncating it. It returns a close function that must be called to flush and
// close the file and restore the previous writer. This lets a command write
// machine-readable results straight to a file without a shell redirect, while
// logs continue to flow to stderr (ErrOut) untouched.
//
// A path of "" or "-" is a no-op that leaves Out pointing at stdout.
func UseFile(path string) (func() error, error) {
	if path == "" || path == "-" {
		return func() error { return nil }, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.Wrapf(err, "opening output file %q", path)
	}
	prev := Out
	Out = f
	return func() error {
		Out = prev
		return f.Close()
	}, nil
}
