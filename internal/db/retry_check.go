package db

import "github.com/yandex-cloud/ydb-go-sdk/v2"

type checkGeneric struct {
	base Checkable
}

func (c *checkGeneric) Check(err error) ydb.RetryMode {
	if ydb.IsOpError(err, ydb.StatusGenericError) {
		return ydb.RetryAvailable | ydb.RetryBackoff
	}
	return c.base.Check(err)
}
