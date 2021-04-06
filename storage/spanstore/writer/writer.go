package writer

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"go.uber.org/zap"

	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/batch"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/indexer"
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
	oudatedCounter    metrics.Counter
}

// NewSpanWriter creates writer interface implementation for YDB
func NewSpanWriter(pool *table.SessionPool, metricsFactory metrics.Factory, logger *zap.Logger, opts SpanWriterOptions) *SpanWriter {
	cache, _ := lru.New(opts.OpCacheSize) // it's ok to ignore this error for negative size
	batchOpts := batch.Options{
		BufferSize:   opts.BufferSize,
		BatchSize:    opts.BatchSize,
		BatchWorkers: opts.BatchWorkers,
	}
	writerOpts := BatchWriterOptions{
		WriteTimeout: opts.WriteTimeout,
		DbPath:       opts.DbPath,
	}
	var batchWriter batch.Writer
	if opts.ArchiveWriter {
		batchWriter = NewArchiveWriter(pool, metricsFactory, logger, writerOpts)
	} else {
		batchWriter = NewBatchWriter(pool, metricsFactory, logger, writerOpts)
	}
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
		oudatedCounter:    metricsFactory.Counter(metrics.Options{Name: "outdated"}),
	}
}

// WriteSpan saves the span into YDB
func (s *SpanWriter) WriteSpan(ctx context.Context, span *model.Span) error {
	if s.opts.MaxSpanAge != time.Duration(0) && time.Now().Sub(span.StartTime) > s.opts.MaxSpanAge {
		s.oudatedCounter.Inc(1)
		return nil
	}

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

	if !s.opts.ArchiveWriter {
		_ = s.indexer.Add(span)
	}

	return s.saveServiceNameAndOperationName(span)
}

func (s *SpanWriter) saveServiceNameAndOperationName(span *model.Span) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.opts.WriteTimeout)
	defer cancel()

	serviceName := span.GetProcess().GetServiceName()
	operationName := span.GetOperationName()
	kind, _ := span.GetSpanKind()
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
	if exists, _ := s.nameCache.ContainsOrAdd(serviceName+"-"+operationName+"-"+kind, true); !exists {
		data := ydb.ListValue(ydb.StructValue(
			ydb.StructFieldValue("service_name", ydb.UTF8Value(serviceName)),
			ydb.StructFieldValue("operation_name", ydb.UTF8Value(operationName)),
			ydb.StructFieldValue("span_kind", ydb.UTF8Value(kind)),
		))
		err := table.Retry(ctx, s.pool, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
			return session.BulkUpsert(ctx, s.opts.DbPath.FullTable("operation_names_v2"), data)
		}))
		if err != nil {
			return err
		}
	}
	return nil
}
