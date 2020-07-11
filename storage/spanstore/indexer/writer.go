package indexer

import (
	"context"
	"go.uber.org/zap"
	"math/rand"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"

	"github.com/yandex-cloud/jaeger-ydb-store/schema"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/batch"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/dbmodel"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/indexer/index"
	wmetrics "github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/writer/metrics"
)

type indexWriter struct {
	pool      *table.SessionPool
	logger    *zap.Logger
	metrics   indexerMetrics
	tableName string
	opts      Options

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

func startIndexWriter(pool *table.SessionPool, mf metrics.Factory, logger *zap.Logger, tableName string, opts Options) *indexWriter {
	w := &indexWriter{
		pool:      pool,
		logger:    logger,
		metrics:   wmetrics.NewWriteMetrics(mf, ""),
		tableName: tableName,
		opts:      opts,
		idxRand:   newLockedRand(time.Now().UnixNano()),
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
	rows := make([]ydb.Value, 0, len(items))
	for _, item := range items {
		brr.Next()
		buf := item.traceIds.ToBytes()
		fields := item.idx.StructFields(brr.Next())
		fields = append(fields,
			ydb.StructFieldValue("uniq", ydb.Uint32Value(w.idxRand.Uint32())),
			ydb.StructFieldValue("trace_ids", ydb.StringValue(buf)),
		)
		rows = append(rows, ydb.StructValue(fields...))
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.opts.WriteTimeout)
	defer cancel()
	ts := time.Now()
	err := table.Retry(ctx, w.pool, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		return session.BulkUpsert(ctx, fullTableName, ydb.ListValue(rows...))
	}))
	w.metrics.Emit(err, time.Since(ts), len(rows))
	if err != nil {
		w.logger.Error("indexer write fail", zap.String("table", w.tableName), zap.Error(err))
	}
}

func tableName(dbPath schema.DbPath, part schema.PartitionKey, tableName string) string {
	return part.BuildFullTableName(dbPath.String(), tableName)
}
