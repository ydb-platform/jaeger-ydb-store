package writer

import (
	"context"
	"time"

	"github.com/hashicorp/go-hclog"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"

	"github.com/ydb-platform/jaeger-ydb-store/internal/db"
	"github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/batch"
	"github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/indexer"
)

// SpanWriter handles all span/indexer writes to YDB
type SpanWriter struct {
	opts              SpanWriterOptions
	pool              table.Client
	logger            *zap.Logger
	jaegerLogger      hclog.Logger
	spanBatch         *batch.Queue
	indexer           *indexer.Indexer
	nameCache         *lru.Cache
	invalidateMetrics *invalidSpanMetrics
}

// NewSpanWriter creates writer interface implementation for YDB
func NewSpanWriter(pool table.Client, metricsFactory metrics.Factory, logger *zap.Logger, jaegerLogger hclog.Logger, opts SpanWriterOptions) *SpanWriter {
	cache, _ := lru.New(opts.OpCacheSize) // it's ok to ignore this error for negative size
	batchOpts := batch.Options{
		BufferSize:   opts.BufferSize,
		BatchSize:    opts.BatchSize,
		BatchWorkers: opts.BatchWorkers,
	}
	writerOpts := BatchWriterOptions{
		WriteTimeout:        opts.WriteTimeout,
		RetryAttemptTimeout: opts.RetryAttemptTimeout,
		DbPath:              opts.DbPath,
	}
	var batchWriter batch.Writer
	if opts.ArchiveWriter {
		batchWriter = NewArchiveWriter(pool, metricsFactory, logger, jaegerLogger, writerOpts)
	} else {
		batchWriter = NewBatchWriter(pool, metricsFactory, logger, jaegerLogger, writerOpts)
	}
	bq := batch.NewQueue(batchOpts, metricsFactory.Namespace(metrics.NSOptions{Name: "spans"}), batchWriter)
	idx := indexer.NewIndexer(pool, metricsFactory, logger, jaegerLogger, indexer.Options{
		DbPath:              opts.DbPath,
		BufferSize:          opts.IndexerBufferSize,
		MaxTraces:           opts.IndexerMaxTraces,
		MaxTTL:              opts.IndexerTTL,
		WriteTimeout:        opts.WriteTimeout,
		RetryAttemptTimeout: opts.RetryAttemptTimeout,
		Batch:               batchOpts,
	})
	return &SpanWriter{
		opts:              opts,
		pool:              pool,
		logger:            logger,
		jaegerLogger:      jaegerLogger,
		spanBatch:         bq,
		indexer:           idx,
		nameCache:         cache,
		invalidateMetrics: newInvalidSpanMetrics(metricsFactory),
	}
}

// WriteSpan saves the span into YDB
func (s *SpanWriter) WriteSpan(ctx context.Context, span *model.Span) error {
	if s.opts.MaxSpanAge != time.Duration(0) && time.Now().Sub(span.StartTime) > s.opts.MaxSpanAge {
		s.invalidateMetrics.Inc(span.Process.ServiceName, span.OperationName)
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

	return s.saveServiceNameAndOperationName(ctx, span)
}

func (s *SpanWriter) saveServiceNameAndOperationName(ctx context.Context, span *model.Span) error {
	serviceName := span.GetProcess().GetServiceName()
	operationName := span.GetOperationName()
	kind, _ := span.GetSpanKind()
	if exists, _ := s.nameCache.ContainsOrAdd(serviceName, true); !exists {
		data := types.ListValue(types.StructValue(
			types.StructFieldValue("service_name", types.UTF8Value(serviceName)),
		))

		if s.opts.WriteTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, s.opts.WriteTimeout)
			defer cancel()
		}
		err := db.UpsertData(ctx, s.pool, s.opts.DbPath.FullTable("service_names"), data, s.opts.RetryAttemptTimeout)
		if err != nil {
			s.jaegerLogger.Error(
				"Failed to save service name",
				"service_name", serviceName,
				"error", err,
			)

			return err
		}
	}
	if operationName == "" {
		return nil
	}
	if exists, _ := s.nameCache.ContainsOrAdd(serviceName+"-"+operationName+"-"+kind, true); !exists {
		data := types.ListValue(types.StructValue(
			types.StructFieldValue("service_name", types.UTF8Value(serviceName)),
			types.StructFieldValue("operation_name", types.UTF8Value(operationName)),
			types.StructFieldValue("span_kind", types.UTF8Value(kind)),
		))
		if s.opts.WriteTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, s.opts.WriteTimeout)
			defer cancel()
		}
		err := db.UpsertData(ctx, s.pool, s.opts.DbPath.FullTable("operation_names_v2"), data, s.opts.RetryAttemptTimeout)
		if err != nil {
			s.jaegerLogger.Error(
				"Failed to save operation name",
				"operation_name", operationName,
				"error", err,
			)
			return err
		}
	}
	return nil
}

func (s *SpanWriter) Close() {
	s.spanBatch.Close()
	s.indexer.Close()
}
