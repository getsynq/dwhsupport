package output

import (
	"io"
	"os"
)

// Out is the destination for rendered output (tables, JSON, YAML, ...).
// Defaults to stdout; override in tests or when embedding the printer.
var Out io.Writer = os.Stdout

// ErrOut is the destination for human-oriented side messages (headlines,
// "no results" notices) that must not pollute a structured stdout stream.
// Defaults to stderr.
var ErrOut io.Writer = os.Stderr
