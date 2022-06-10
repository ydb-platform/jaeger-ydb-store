package dbmodel

import (
	"bytes"

	farm "github.com/dgryski/go-farm"
)

func HashTagIndex(service, key, value string, bucket uint8) uint64 {
	return HashBucketData(bucket, service, key, value)
}

func HashBucketData(bucket uint8, lst ...string) uint64 {
	buf := new(bytes.Buffer)
	for _, s := range lst {
		buf.WriteString(s)
	}
	buf.WriteByte(bucket)
	return farm.Hash64(buf.Bytes())
}

func HashData(lst ...string) uint64 {
	buf := new(bytes.Buffer)
	for _, s := range lst {
		buf.WriteString(s)
	}
	return farm.Hash64(buf.Bytes())
}
