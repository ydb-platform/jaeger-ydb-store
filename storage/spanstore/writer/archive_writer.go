package writer

import (
	"context"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
	"go.uber.org/zap"

	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/dbmodel"
	wmetrics "github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/writer/metrics"
)

const (
	tblArchive = "archive"
)

type ArchiveSpanWriter struct {
	metrics batchWriterMetrics
	pool    *table.SessionPool
	logger  *zap.Logger
	opts    BatchWriterOptions
}

func NewArchiveWriter(pool *table.SessionPool, factory metrics.Factory, logger *zap.Logger, opts BatchWriterOptions) *ArchiveSpanWriter {
	ns := factory.Namespace(metrics.NSOptions{Name: "archive"})

	return &ArchiveSpanWriter{
		pool:    pool,
		logger:  logger,
		opts:    opts,
		metrics: newBatchWriterMetrics(ns),
	}
}

func (w *ArchiveSpanWriter) WriteItems(items []interface{}) {
	spans := make([]*model.Span, 0, len(items))
	for _, item := range items {
		span := item.(*model.Span)
		spans = append(spans, span)
	}
	w.writeItems(spans)
}

func (w *ArchiveSpanWriter) writeItems(items []*model.Span) {
	spanRecords := make([]ydb.Value, 0, len(items))
	for _, span := range items {
		dbSpan, _ := dbmodel.FromDomain(span)
		spanRecords = append(spanRecords, dbSpan.StructValue())
	}

	ctx, ctxCancel := context.WithTimeout(context.Background(), w.opts.WriteTimeout)
	defer ctxCancel()
	tableName := w.opts.DbPath.FullTable(tblArchive)
	var err error

	if err = w.uploadRows(ctx, tableName, spanRecords, w.metrics.traces); err != nil {
		w.logger.Error("insertSpan error", zap.Error(err))
		return
	}
}

func (w *ArchiveSpanWriter) uploadRows(ctx context.Context, tableName string, rows []ydb.Value, metrics *wmetrics.WriteMetrics) error {
	ts := time.Now()
	data := ydb.ListValue(rows...)
	err := table.Retry(ctx, w.pool, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		return session.BulkUpsert(ctx, tableName, data)
	}))
	metrics.Emit(err, time.Since(ts), len(rows))
	return err
}
