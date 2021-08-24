package db

import (
	"strings"

	"github.com/yandex-cloud/ydb-go-sdk/v2"
)

func IssueContainsMessage(it ydb.IssueIterator, msg string) bool {
	for i := 0; i < it.Len(); i++ {
		issue, nested := it.Get(i)
		if strings.Contains(issue.Message, msg) {
			return true
		}
		if nested != nil && IssueContainsMessage(nested, msg) {
			return true
		}
	}
	return false
}
