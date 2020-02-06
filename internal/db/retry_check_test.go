package db

import (
	"github.com/stretchr/testify/assert"
	"github.com/yandex-cloud/ydb-go-sdk"
	"testing"
)

func TestCheckGeneric_Check(t *testing.T) {
	c := checkGeneric{base: &ydb.DefaultRetryChecker}
	e := &ydb.OpError{Reason: ydb.StatusGenericError}
	m := c.Check(e)
	assert.True(t, m.Retriable())
	assert.True(t, m.MustBackoff())
}
