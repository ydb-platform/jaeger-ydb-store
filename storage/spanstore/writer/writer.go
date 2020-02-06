package writer

import (
	"context"
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/batch"
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/indexer"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"go.uber.org/zap"
)

// SpanWriter handles all span/indexer writes to YDB
type SpanWriter struct {
	opts              SpanWriterOptions
	pool              *table.SessionPool
	logger            *zap.Logger
	spanBatch         *batch.Queue
	indexer           *indexer.Indexer
	nameCache         *lru.Cache
	invalidateMetrics *invalidSpanMetrics
}

// NewSpanWriter creates writer interface implementation for YDB
func NewSpanWriter(pool *table.SessionPool, metricsFactory metrics.Factory, logger *zap.Logger, opts SpanWriterOptions) *SpanWriter {
	cache, _ := lru.New(256) // it's ok to ignore this error for negative size
	batchOpts := batch.Options{
		BufferSize:   opts.BufferSize,
		BatchSize:    opts.BatchSize,
		BatchWorkers: opts.BatchWorkers,
	}
	writerOpts := BatchWriterOptions{
		WriteTimeout: opts.WriteTimeout,
		DbPath:       opts.DbPath,
	}
	batchWriter := NewBatchWriter(pool, metricsFactory, logger, writerOpts)
	bq := batch.NewQueue(batchOpts, metricsFactory.Namespace(metrics.NSOptions{Name: "spans"}), batchWriter)
	bq.Init()
	idx := indexer.StartIndexer(pool, metricsFactory, logger, indexer.Options{
		DbPath:       opts.DbPath,
		BufferSize:   opts.IndexerBufferSize,
		MaxTraces:    opts.IndexerMaxTraces,
		MaxTTL:       opts.IndexerTTL,
		WriteTimeout: opts.WriteTimeout,
		Batch:        batchOpts,
	})
	return &SpanWriter{
		opts:              opts,
		pool:              pool,
		logger:            logger,
		spanBatch:         bq,
		indexer:           idx,
		nameCache:         cache,
		invalidateMetrics: newInvalidSpanMetrics(metricsFactory),
	}
}

// WriteSpan saves the span into YDB
func (s *SpanWriter) WriteSpan(span *model.Span) error {
	if span.StartTime.Unix() == 0 || span.StartTime.IsZero() {
		s.invalidateMetrics.Inc(span.Process.ServiceName, span.OperationName)
		return nil
	}
	err := s.spanBatch.Add(span)
	if err != nil {
		switch err {
		case batch.ErrOverflow:
			return nil
		default:
			return err
		}
	}

	_ = s.indexer.Add(span)

	return s.saveServiceNameAndOperationName(span.Process.ServiceName, span.OperationName)
}

func (s *SpanWriter) saveServiceNameAndOperationName(serviceName, operationName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.opts.WriteTimeout)
	defer cancel()

	if exists, _ := s.nameCache.ContainsOrAdd(serviceName, true); !exists {
		data := ydb.ListValue(ydb.StructValue(
			ydb.StructFieldValue("service_name", ydb.UTF8Value(serviceName)),
		))
		err := table.Retry(ctx, s.pool, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
			return session.BulkUpsert(ctx, s.opts.DbPath.FullTable("service_names"), data)
		}))
		if err != nil {
			return err
		}
	}
	if operationName == "" {
		return nil
	}
	if exists, _ := s.nameCache.ContainsOrAdd(serviceName+"-"+operationName, true); !exists {
		data := ydb.ListValue(ydb.StructValue(
			ydb.StructFieldValue("service_name", ydb.UTF8Value(serviceName)),
			ydb.StructFieldValue("operation_name", ydb.UTF8Value(operationName)),
		))
		err := table.Retry(ctx, s.pool, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
			return session.BulkUpsert(ctx, s.opts.DbPath.FullTable("operation_names"), data)
		}))
		if err != nil {
			return err
		}
	}
	return nil
}
