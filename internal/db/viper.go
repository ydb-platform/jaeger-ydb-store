package db

const (
	KeyYdbAddress = "ydb.address"
	KeyYdbPath    = "ydb.path"
	KeyYdbFolder  = "ydb.folder"
	KeyYdbToken   = "ydb.token"

	KeyYdbSaPrivateKeyFile = "ydb.sa.private-key-file"
	KeyYdbSaMetaAuth       = "ydb.sa.meta-auth"
	KeyYdbSaId             = "ydb.sa.id"
	KeyYdbSaKeyID          = "ydb.sa.key-id"

	KeyYdbCAFile = "ydb.ca-file"

	KeyYdbConnectTimeout      = "ydb.connect-timeout"
	KeyYdbWriteTimeout        = "ydb.write-timeout"
	KeyYdbWriteAttemptTimeout = "ydb.write-attempt-timeout"

	KeyYdbReadTimeout       = "ydb.read-timeout"
	KeyYdbReadQueryParallel = "ydb.read-query-parallel"
	KeyYdbReadOpLimit       = "ydb.read-op-limit"
	KeyYdbReadSvcLimit      = "ydb.read-svc-limit"

	KeyYdbPoolSize = "ydb.pool-size"

	KeyYdbQueryCacheSize = "ydb.query-cache-size"

	KeyYdbWriterBufferSize   = "ydb.writer.buffer-size"
	KeyYdbWriterBatchSize    = "ydb.writer.batch-size"
	KeyYdbWriterBatchWorkers = "ydb.writer.batch-workers"
	// KeyYdbWriterMaxSpanAge controls max age for accepted spans.
	// Each span older than time.Now() - KeyYdbWriterMaxSpanAge will be neglected.
	// Defaults to zero which effectively means any span age is good.
	KeyYdbWriterMaxSpanAge     = "ydb.writer.max-span-age"
	KeyYdbWriterSvcOpCacheSize = "ydb.writer.service-name-operation-cache-size"

	KeyYdbIndexerBufferSize = "ydb.indexer.buffer-size"
	KeyYdbIndexerMaxTraces  = "ydb.indexer.max-traces"
	KeyYdbIndexerMaxTTL     = "ydb.indexer.max-ttl"

	KeyYDBPartitionSize      = "ydb.partition-size"
	KeyYDBFeatureSplitByLoad = "ydb.feature.split-by-load"
	KeyYDBFeatureCompression = "ydb.feature.compression"

	KeyYdbLogScope = "ydb.log.scope"
)

const (
	KeyIAMEndpoint = "iam.endpoint"
)
