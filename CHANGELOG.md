# Changes by version

1.7.0 (2022-01-03)
------------------

* updated ydb-go-sdk from v3.4.1 to v3.6.2
* updated ydb-go-yc from v0.2.1 to v0.4.2
* fixed code with latest ydb-go-sdk
* added integration tests instead docker with ydb
* added issue templates
* fixed some linter issues

1.6.0 (2021-12-24)
------------------

* update ydb-go-sdk to v3
* increase num results limit for querying service names
* add config flag for reading config file
* fix initial partition size on table schema creation

1.5.1 (2021-04-28)
------------------

### Improvements
* increase query limit for op names

1.5.0 (2021-04-28)
------------------

### Changes
* Updated [README](README.md)
* Updated Go to 1.16.3
* Added YDB feature flags (see [README](README.md) for description) for Watcher
  - `YDB_FEATURE_SPLIT_BY_LOAD`
  - `YDB_FEATURE_COMPRESSION`

### Improvements
* Started using `OnlineReadOnly` + `AllowInconsistentReads` in Query
  instead of `SerializableReadWrite` isolation level
* Added `DISTINCT` to a couple of search queries 

1.4.3 (2021-04-21)
------------------

### Changes
* Skip outdated spans

1.4.2 (2021-02-18)
------------------

### Changes
* span writer: svc+op cache size

1.4.1 (2020-12-08)
------------------

### Changes
* schema watcher: cache created tables

1.4.0 (2020-09-30)
------------------

### Changes
* add archive storage support
* bump jaeger base image to 1.20.0

1.3.1 (2020-07-10)
------------------

### Changes
* idx_tag_v2 schema watcher defaults
* remove old idx_tag schema from creation
* don't write batch overflow error to log
* bump golang to 1.14.4
* bump jaeger base image to 1.18.1

1.3.0 (2020-07-10)
------------------

### Breaking changes
* idx_tag_v2: reduce number of index records, not compatible with old dataset

### Improvements
* configurable threadpool for FindTraces query
* configurable number of daily partitioned tables

1.2.0 (2020-04-16)
------------------

### Breaking changes
* update for jaeger 1.17, change operation_names index to support client/server spans

### Improvements
* update ydb sdk to fix bad sessions leaking
* update IAM client
* use P2C balancing method for ydb client


1.1.0 (2020-02-14)
------------------


### Breaking changes
* tag indexer: index service+operation_name+tag in addition to service+tag, breaks searching through old dataset

1.0.0 (2020-02-06)
------------------
* Initial release
