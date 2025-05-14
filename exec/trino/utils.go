package trino

import "strings"

func trimRightSemicolons(sql string) string {
	sql = strings.TrimSpace(sql)
	return strings.TrimRight(sql, ";")
}
