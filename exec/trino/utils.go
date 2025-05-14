package trino

import "strings"

func trimRightSemicolons(sql string) string {
	return strings.TrimRight(sql, ";")
}
