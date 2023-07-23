package indexer

import (
	"context"
	"github.com/hashicorp/go-hclog"
	"math/rand"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"

	"github.com/ydb-platform/jaeger-ydb-store/schema"
	"github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/batch"
	"github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/dbmodel"
	"github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/indexer/index"
	wmetrics "github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/writer/metrics"
)

type indexWriter struct {
	pool         table.Client
	logger       *zap.Logger
	pluginLogger hclog.Logger
	metrics      indexerMetrics
	tableName    string
	opts         Options

	idxRand *rand.Rand
	batch   *batch.Queue
	*indexTTLMap
}

type indexData struct {
	idx      index.Indexable
	traceIds index.TraceIDList
}

type indexerMetrics interface {
	Emit(err error, latency time.Duration, count int)
}

func startIndexWriter(pool table.Client, mf metrics.Factory, logger *zap.Logger, tableName string, opts Options) *indexWriter {
	pluginLogger := hclog.New(&hclog.LoggerOptions{
		Name:       "index writer",
		JSONFormat: true,
		Color:      hclog.AutoColor,
	})
	w := &indexWriter{
		pool:         pool,
		logger:       logger,
		pluginLogger: pluginLogger,
		metrics:      wmetrics.NewWriteMetrics(mf, ""),
		tableName:    tableName,
		opts:         opts,
		idxRand:      newLockedRand(time.Now().UnixNano()),
	}
	w.indexTTLMap = newIndexMap(w.flush, opts.MaxTraces, opts.MaxTTL)
	w.batch = batch.NewQueue(opts.Batch, mf, w)
	w.batch.Init()
	return w
}

func (w *indexWriter) flush(idx index.Indexable, traceIds []model.TraceID) {
	err := w.batch.Add(indexData{
		idx:      idx,
		traceIds: traceIds,
	})
	switch {
	case err == batch.ErrOverflow:
	case err != nil:
		w.logger.Error("indexer batch error", zap.String("table", w.tableName), zap.Error(err))
		w.pluginLogger.Error(
			"indexer batch error",
			"table", w.tableName,
			"error", err,
		)
	}
}

func (w *indexWriter) WriteItems(items []interface{}) {
	parts := map[schema.PartitionKey][]indexData{}
	for _, item := range items {
		data := item.(indexData)
		k := schema.PartitionFromTime(data.idx.Timestamp())
		parts[k] = append(parts[k], data)
	}
	for k, partial := range parts {
		w.writePartition(k, partial)
	}
}

func (w *indexWriter) writePartition(part schema.PartitionKey, items []indexData) {
	fullTableName := tableName(w.opts.DbPath, part, w.tableName)
	brr := newBucketRR(dbmodel.NumIndexBuckets)
	rows := make([]types.Value, 0, len(items))
	for _, item := range items {
		brr.Next()
		// nolint: typecheck, nolintlint
		buf := item.traceIds.ToBytes()
		fields := item.idx.StructFields(brr.Next())
		fields = append(fields,
			types.StructFieldValue("uniq", types.Uint32Value(w.idxRand.Uint32())),
			types.StructFieldValue("trace_ids", types.BytesValue(buf)),
		)
		rows = append(rows, types.StructValue(fields...))
	}
	ts := time.Now()
	err := w.pool.Do(
		context.Background(),
		func(ctx context.Context, session table.Session) (err error) {
			ctx, cancel := context.WithTimeout(ctx, w.opts.WriteTimeout)
			defer cancel()
			return session.BulkUpsert(ctx, fullTableName, types.ListValue(rows...))
		},
		table.WithIdempotent(),
	)
	w.metrics.Emit(err, time.Since(ts), len(rows))
	if err != nil {
		w.logger.Error("indexer write fail", zap.String("table", w.tableName), zap.Error(err))
		w.pluginLogger.Error(
			"indexer write fail",
			"table", w.tableName,
			"error", err,
		)
	}
}

func tableName(dbPath schema.DbPath, part schema.PartitionKey, tableName string) string {
	return part.BuildFullTableName(dbPath.String(), tableName)
}
