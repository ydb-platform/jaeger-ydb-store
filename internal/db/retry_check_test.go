package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yandex-cloud/ydb-go-sdk/v2"
)

func TestCheckGeneric_Check(t *testing.T) {
	c := checkGeneric{base: &ydb.DefaultRetryChecker}
	e := &ydb.OpError{Reason: ydb.StatusGenericError}
	m := c.Check(e)
	assert.True(t, m.Retriable())
	assert.True(t, m.MustBackoff())
}
