package mysql

import (
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsPermissionError(t *testing.T) {
	for _, n := range []uint16{1044, 1142, 1143, 1227} {
		assert.Truef(t, IsPermissionError(errors.Wrap(&mysql.MySQLError{Number: n}, "query")), "error %d", n)
	}
	// Unknown column and unknown table are not about privileges.
	for _, n := range []uint16{1054, 1146, 1064} {
		assert.Falsef(t, IsPermissionError(&mysql.MySQLError{Number: n}), "error %d", n)
	}
	assert.False(t, IsPermissionError(nil))
	assert.False(t, IsPermissionError(errors.New("SELECT command denied")))
}
