# Changes by version

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
