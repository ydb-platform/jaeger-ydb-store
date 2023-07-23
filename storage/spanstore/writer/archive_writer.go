package writer

import (
	"context"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"

	"github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/dbmodel"
	wmetrics "github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/writer/metrics"
)

const (
	tblArchive = "archive"
)

type ArchiveSpanWriter struct {
	metrics      batchWriterMetrics
	pool         table.Client
	logger       *zap.Logger
	pluginLogger hclog.Logger
	opts         BatchWriterOptions
}

func NewArchiveWriter(pool table.Client, factory metrics.Factory, logger *zap.Logger, opts BatchWriterOptions) *ArchiveSpanWriter {
	ns := factory.Namespace(metrics.NSOptions{Name: "archive"})
	pluginLogger := hclog.New(&hclog.LoggerOptions{
		Name:       "ArchiveWriter",
		JSONFormat: true,
		Color:      hclog.AutoColor,
	})

	return &ArchiveSpanWriter{
		pool:         pool,
		logger:       logger,
		pluginLogger: pluginLogger,
		opts:         opts,
		metrics:      newBatchWriterMetrics(ns),
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
	spanRecords := make([]types.Value, 0, len(items))
	for _, span := range items {
		dbSpan, _ := dbmodel.FromDomain(span)
		spanRecords = append(spanRecords, dbSpan.StructValue())
	}

	tableName := w.opts.DbPath.FullTable(tblArchive)
	var err error

	if err = w.uploadRows(tableName, spanRecords, w.metrics.traces); err != nil {
		w.logger.Error("insertSpan error", zap.Error(err))
		w.pluginLogger.Error(
			"Failed to save spans to archive storage",
			"error", err,
		)

		return
	}
}

func (w *ArchiveSpanWriter) uploadRows(tableName string, rows []types.Value, metrics *wmetrics.WriteMetrics) error {
	ts := time.Now()
	data := types.ListValue(rows...)
	err := w.pool.Do(
		context.Background(),
		func(ctx context.Context, session table.Session) (err error) {
			ctx, cancel := context.WithTimeout(ctx, w.opts.WriteTimeout)
			defer cancel()
			return session.BulkUpsert(ctx, tableName, data)
		},
		table.WithIdempotent(),
	)
	metrics.Emit(err, time.Since(ts), len(rows))
	return err
}
