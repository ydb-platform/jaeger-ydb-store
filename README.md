# YDB storage plugin for Jaeger

[![PkgGoDev](https://pkg.go.dev/badge/github.com/ydb-platform/jaeger-ydb-store)](https://pkg.go.dev/github.com/ydb-platform/jaeger-ydb-store)
[![GoDoc](https://godoc.org/github.com/ydb-platform/jaeger-ydb-store?status.svg)](https://godoc.org/github.com/ydb-platform/jaeger-ydb-store)
![tests](https://github.com/ydb-platform/jaeger-ydb-store/workflows/tests/badge.svg?branch=master)
![lint](https://github.com/ydb-platform/jaeger-ydb-store/workflows/lint/badge.svg?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/ydb-platform/jaeger-ydb-store)](https://goreportcard.com/report/github.com/ydb-platform/jaeger-ydb-store)
[![codecov](https://codecov.io/gh/yandex-cloud/jaeger-ydb-store/branch/master/graph/badge.svg)](https://app.codecov.io/gh/yandex-cloud/jaeger-ydb-store)

This is a storage backend using [YDB](https://ydb.tech/) for [Jaeger](https://github.com/jaegertracing/jaeger)

## components

- collector/query: these are docker images with jaeger-collector/jaeger-query and plugin built-in into image
- watcher (see cmd/schema/watcher): this is schema-watcher that creates new tables for spans/indexes and removes old ones

## docker images

- `cr.yandex/yc/jaeger-ydb-collector`
- `cr.yandex/yc/jaeger-ydb-query`
- `cr.yandex/yc/jaeger-ydb-watcher`

## how to run

```
cp docker-compose.example.yml docker-compose.yml
# edit docker-compose yml to set db dsn
docker-compose up -d
```

## environment variables

Name | Type | Default | Description
--- | --- | --- | ---
`YDB_ADDRESS` | `string` | | db endpoint host:port to connect to
`YDB_SA_META_AUTH` | `bool` | `false` | use metadata to authorize requests ([documentation](https://cloud.yandex.com/docs/compute/operations/vm-connect/auth-inside-vm#auth-inside-vm))
`YDB_SA_ID` | `string` | | service account id for Yandex.Cloud authorization (doc on service accounts: https://cloud.yandex.com/docs/iam/concepts/users/service-accounts)
`YDB_SA_KEY_ID` | `string` | | service account key id for Yandex.Cloud authorization
`YDB_SA_PRIVATE_KEY_FILE` | `string` | | path to service account private key for Yandex.Cloud authorization
`YDB_PATH` | `string` | | database path
`YDB_FOLDER` | `string` | | folder to store data in)
`YDB_CONNECT_TIMEOUT` | `duration` | `10s` | db connect timeout
`YDB_WRITE_TIMEOUT` | `duration`| `1s` | write queries timeout
`YDB_READ_TIMEOUT` | `duration` | `10s` | read queries timeout
`YDB_READ_QUERY_PARALLEL` | `integer` | `16` | controls number of parallel read subqueries
`YDB_READ_OP_LIMIT` | `integer` | `5000` | max operation names to fetch for service
`YDB_READ_SVC_LIMIT` | `integer` | `1000` | max service names to fetch
`YDB_POOL_SIZE` | `integer` | `100` | db session pool size
`YDB_QUERY_CACHE_SIZE` | `integer` | `50` | db query cache size
`YDB_WRITER_BUFFER_SIZE` | `integer` | `1000` | span buffer size for batch writer
`YDB_WRITER_BATCH_SIZE` | `integer` | `100` | number of spans in batch write calls
`YDB_WRITER_BATCH_WORKERS` | `integer` | `10` | number of workers processing batch writes
`YDB_INDEXER_BUFFER_SIZE` | `integer` | `1000` | span buffer size for indexer
`YDB_INDEXER_MAX_TRACES` | `integer` | `100` | maximum trace_id count in a sinigle index record
`YDB_INDEXER_MAX_TTL` | `duration` | `5s` | maximum amount of time for indexer to batch trace_idы for index records
`YDB_SCHEMA_NUM_PARTITIONS` | `integer` | `10` | number of partitioned tables per day. Changing it requires recreating full data set
`YDB_TOKEN` | `string` | | auth token for internal purposes

Configuration options can be passed via config file. Use `--grpc-storage-plugin.configuration-file` to pass configuration to YDB Plugin. In case of watcher use `--config` for the same purpose.  

## schema watcher configuration

Name | Type | Default | Description
--- | --- | --- | ---
`WATCHER_AGE` | `duration` | `24h` | delete partition tables older than this value
`WATCHER_INTERVAL` | `duration` | `5m` | check interval
`YDB_FEATURE_SPLIT_BY_LOAD` | `bool` | `false` | enable table split by load feature
`YDB_FEATURE_COMPRESSION` | `bool` | `false` | enable table compression feature, used for span storage

## conference talks

- https://youtu.be/nyt_e4ULrUo?t=660
- https://youtu.be/hXH_tRBxFnA?t=11471
