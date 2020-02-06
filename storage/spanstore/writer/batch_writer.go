package writer

import (
	"context"
	"github.com/YandexClassifieds/jaeger-ydb-store/schema"
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/dbmodel"
	wmetrics "github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/writer/metrics"
	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"go.uber.org/zap"
	"time"
)

const (
	tblTraces = "traces"
)

type BatchSpanWriter struct {
	metrics batchWriterMetrics
	pool    *table.SessionPool
	logger  *zap.Logger
	opts    BatchWriterOptions
}

func NewBatchWriter(pool *table.SessionPool, factory metrics.Factory, logger *zap.Logger, opts BatchWriterOptions) *BatchSpanWriter {
	return &BatchSpanWriter{
		pool:    pool,
		logger:  logger,
		opts:    opts,
		metrics: newBatchWriterMetrics(factory),
	}
}

func (w *BatchSpanWriter) WriteItems(items []interface{}) {
	parts := map[schema.PartitionKey][]*model.Span{}
	for _, item := range items {
		span := item.(*model.Span)
		k := schema.PartitionFromTime(span.StartTime)
		parts[k] = append(parts[k], span)
	}
	for k, partial := range parts {
		w.writeItemsToPartition(k, partial)
	}
}

func (w *BatchSpanWriter) writeItemsToPartition(part schema.PartitionKey, items []*model.Span) {
	spanRecords := make([]ydb.Value, 0, len(items))
	for _, span := range items {
		dbSpan, _ := dbmodel.FromDomain(span)
		spanRecords = append(spanRecords, dbSpan.StructValue())
	}

	ctx, ctxCancel := context.WithTimeout(context.Background(), w.opts.WriteTimeout)
	defer ctxCancel()
	tableName := func(table string) string {
		return part.BuildFullTableName(w.opts.DbPath.String(), table)
	}
	var err error

	if err = w.uploadRows(ctx, tableName(tblTraces), spanRecords, w.metrics.traces); err != nil {
		w.logger.Error("insertSpan error", zap.Error(err))
		return
	}
}

func (w *BatchSpanWriter) uploadRows(ctx context.Context, tableName string, rows []ydb.Value, metrics *wmetrics.WriteMetrics) error {
	ts := time.Now()
	data := ydb.ListValue(rows...)
	err := table.Retry(ctx, w.pool, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		return session.BulkUpsert(ctx, tableName, data)
	}))
	metrics.Emit(err, time.Since(ts), len(rows))
	return err
}
