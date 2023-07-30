package writer

import (
	"context"
	"github.com/ydb-platform/jaeger-ydb-store/internal/db"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"

	"github.com/ydb-platform/jaeger-ydb-store/schema"
	"github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/dbmodel"
	wmetrics "github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/writer/metrics"
)

const (
	tblTraces = "traces"
)

type BatchSpanWriter struct {
	metrics      batchWriterMetrics
	pool         table.Client
	logger       *zap.Logger
	pluginLogger hclog.Logger
	opts         BatchWriterOptions
}

func NewBatchWriter(pool table.Client, factory metrics.Factory, logger *zap.Logger, opts BatchWriterOptions) *BatchSpanWriter {
	pluginLogger := hclog.New(&hclog.LoggerOptions{
		Name:       "BatchSpanWriter",
		JSONFormat: true,
		Color:      hclog.AutoColor,
	})

	return &BatchSpanWriter{
		pool:         pool,
		logger:       logger,
		pluginLogger: pluginLogger,
		opts:         opts,
		metrics:      newBatchWriterMetrics(factory),
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
	spanRecords := make([]types.Value, 0, len(items))
	for _, span := range items {
		dbSpan, _ := dbmodel.FromDomain(span)
		spanRecords = append(spanRecords, dbSpan.StructValue())
	}

	tableName := func(table string) string {
		return part.BuildFullTableName(w.opts.DbPath.String(), table)
	}
	var err error

	if err = w.uploadRows(tableName(tblTraces), spanRecords, w.metrics.traces); err != nil {
		w.logger.Error("insertSpan error", zap.Error(err))
		w.pluginLogger.Error(
			"Failed to save spans",
			"error", err,
		)
		return
	}
}

func (w *BatchSpanWriter) uploadRows(tableName string, rows []types.Value, metrics *wmetrics.WriteMetrics) error {
	ts := time.Now()

	data := types.ListValue(rows...)

	ctx := context.Background()
	if w.opts.WriteTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, w.opts.WriteTimeout)
		defer cancel()
	}
	err := db.UpsertData(ctx, w.pool, tableName, data, w.opts.WriteAttemptTimeout)
	metrics.Emit(err, time.Since(ts), len(rows))
	return err
}
