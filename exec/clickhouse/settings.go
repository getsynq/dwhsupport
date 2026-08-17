package clickhouse

import (
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// defaultSettings are the ClickHouse server settings applied to every connection
// opened by this package. They guard our own scrapes; a caller-supplied setting of
// the same name replaces the entry, so the connection ends up with whatever the
// warehouse owner asked for.
func defaultSettings() clickhouse.Settings {
	return clickhouse.Settings{
		"max_execution_time": 60,
		"max_query_size":     10000000,
	}
}

// buildSettings layers conf settings on top of defaultSettings.
func buildSettings(conf map[string]string) clickhouse.Settings {
	settings := defaultSettings()
	for name, value := range conf {
		settings[name] = coerceSettingValue(value)
	}
	return settings
}

// coerceSettingValue turns a string-valued setting into the type the driver uses
// for the same setting parsed out of a DSN (see parseDSN in the driver's
// clickhouse_options.go): the two boolean spellings become 1 and 0, anything that
// parses as an integer becomes an int, and everything else passes through as a
// string. So a given name/value pair behaves the same whether it arrives in a DSN
// query string or through ClickhouseConf.Settings.
//
// The one deliberate difference: the DSN parser lowercases every value before it
// gets here, because a DSN cannot distinguish a flag from free text. We only
// lowercase for the boolean comparison and otherwise keep the value as written,
// which matters for settings whose value is text — log_comment, for one.
func coerceSettingValue(value string) any {
	switch strings.ToLower(value) {
	case "true":
		return int(1)
	case "false":
		return int(0)
	}
	if n, err := strconv.Atoi(value); err == nil {
		return n
	}
	return value
}
