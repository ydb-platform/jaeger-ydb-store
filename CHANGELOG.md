# Changes by version

1.3 (2020-07-10)
----------------

### Breaking changes
* idx_tag_v2: reduce number of index records, not compatible with old dataset

## Improvements
* configurable threadpool for FindTraces query
* configurable number of daily partitioned tables

1.2 (2020-04-16)
----------------

### Breaking changes
* update for jaeger 1.17, change operation_names index to support client/server spans

### Improvements
* update ydb sdk to fix bad sessions leaking
* update IAM client
* use P2C balancing method for ydb client


1.1 (2020-02-14)
----------------


### Breaking changes
* tag indexer: index service+operation_name+tag in addition to service+tag, breaks searching through old dataset

1.0 (2020-02-06)
----------------
* Initial release
