package scrapper

import (
	"github.com/getsynq/dwhsupport/rowscan"
	"github.com/jmoiron/sqlx"
)

// RowScanner is rowscan.Scanner, re-exported under the name the catalog scans
// and their downstream consumers already use.
//
// The scanner itself lives in its own leaf package because the executor layer
// scans through it too, and exec must not depend on scrapper — see
// exec/stdsql/helpers.go.
type RowScanner[T any] = rowscan.Scanner[T]

// NewRowScanner plans how the columns of an open result set map onto T.
func NewRowScanner[T any](rows *sqlx.Rows) (*RowScanner[T], error) {
	return rowscan.New[T](rows)
}
