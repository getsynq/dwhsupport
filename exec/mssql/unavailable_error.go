package mssql

import (
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/pkg/errors"
)

// Server error numbers for an object or column this server, version or
// edition does not have.
const (
	errInvalidColumnName = 207 // Invalid column name '%s'.
	errInvalidObjectName = 208 // Invalid object name '%s'.
	errUnknownFunction   = 195 // '%s' is not a recognized built-in function name.
	errUnknownObjectFunc = 4121
)

// IsUnavailableError reports whether err says the server has no such object,
// column or function: a catalog view or column this version or edition
// (Azure SQL Edge, Azure SQL Database, Fabric) lacks. No grant fixes it,
// unlike a permission error, and retrying does not either.
func IsUnavailableError(err error) bool {
	var sqlErr mssql.Error
	if !errors.As(err, &sqlErr) {
		return false
	}
	switch sqlErr.Number {
	case errInvalidColumnName, errInvalidObjectName, errUnknownFunction, errUnknownObjectFunc:
		return true
	}
	return false
}
