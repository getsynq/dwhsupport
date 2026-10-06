package mssql

import (
	"testing"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIsUnavailableError(t *testing.T) {
	for _, n := range []int32{207, 208, 195, 4121} {
		assert.Truef(t, IsUnavailableError(errors.Wrap(mssql.Error{Number: n}, "query")), "error %d", n)
	}
	assert.False(t, IsUnavailableError(mssql.Error{Number: 229, Message: "The SELECT permission was denied"}))
	assert.False(t, IsUnavailableError(mssql.Error{Number: 102, Message: "Incorrect syntax"}))
	assert.False(t, IsUnavailableError(errors.New("Invalid object name 'x'")), "only the server's error number counts")
	assert.False(t, IsUnavailableError(nil))
}
