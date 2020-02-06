package db

import "github.com/yandex-cloud/ydb-go-sdk"

type checkGeneric struct {
	base Checkable
}

func (c *checkGeneric) Check(err error) ydb.RetryMode {
	if ydb.IsOpError(err, ydb.StatusGenericError) {
		return ydb.RetryAvailable | ydb.RetryBackoff
	}
	return c.base.Check(err)
}
