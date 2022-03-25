package db

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func IssueContainsMessage(err error, search string) bool {
	result := false
	ydb.IterateByIssues(err, func(message string, code Ydb.StatusIds_StatusCode, severity uint32) {
		if strings.Contains(message, search) {
			result = true
		}
	})
	return result
}
