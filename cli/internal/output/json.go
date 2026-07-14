package output

import (
	"encoding/json"
	"fmt"

	"github.com/itchyny/gojq"
	toon "github.com/toon-format/toon-go"
	"gopkg.in/yaml.v3"
)

func printJSON(data any) error {
	out, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(Out, string(out))
	return err
}

func printYAML(data any) error {
	out, err := yaml.Marshal(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(Out, string(out))
	return err
}

// printTOON emits the data as TOON (Token-Oriented Object Notation). Compared
// to JSON or YAML, TOON is ~2x more token-efficient for tabular data, making it
// the preferred format when piping CLI output into an LLM context window.
func printTOON(data any) error {
	out, err := toon.Marshal(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(Out, string(out))
	return err
}

func printJQ(data any, expr string) error {
	query, err := gojq.Parse(expr)
	if err != nil {
		return fmt.Errorf("invalid jq expression: %w", err)
	}

	iter := query.Run(data)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, isErr := v.(error); isErr {
			return fmt.Errorf("jq error: %w", err)
		}
		switch val := v.(type) {
		case string:
			fmt.Fprintln(Out, val)
		default:
			out, err := json.MarshalIndent(val, "", "  ")
			if err != nil {
				return err
			}
			fmt.Fprintln(Out, string(out))
		}
	}
	return nil
}
