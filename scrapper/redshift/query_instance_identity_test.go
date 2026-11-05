package redshift

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsMissingCurrentNamespace(t *testing.T) {
	assert.True(t, isMissingCurrentNamespace(errors.New(`pq: function current_namespace() does not exist`)))
	assert.True(t, isMissingCurrentNamespace(errors.New(`ERROR: column "current_namespace" does not exist`)))
	assert.False(t, isMissingCurrentNamespace(errors.New(`pq: permission denied for relation pg_class`)))
	assert.False(t, isMissingCurrentNamespace(errors.New(`dial tcp: i/o timeout`)))
}
