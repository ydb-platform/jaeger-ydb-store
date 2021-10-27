package writer

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/yandex-cloud/jaeger-ydb-store/internal/testutil"
	"github.com/yandex-cloud/jaeger-ydb-store/schema"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/reader"
)

func TestArchiveSpanWriter_WriteSpan(t *testing.T) {
	var err error
	pool := testutil.YdbSessionPool(t)
	opts := SpanWriterOptions{
		BufferSize:        10,
		BatchWorkers:      1,
		BatchSize:         1,
		IndexerBufferSize: 10,
		IndexerMaxTraces:  1,
		IndexerTTL:        time.Second,
		DbPath:            schema.DbPath{Path: os.Getenv("YDB_PATH"), Folder: os.Getenv("YDB_FOLDER")},
		WriteTimeout:      time.Second,
		ArchiveWriter:     true,
		OpCacheSize:       256,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	dt := time.Date(2063, 4, 5, 0, 0, 0, 0, time.UTC)
	err = testutil.CreatePartitionTables(ctx, pool, schema.PartitionFromTime(dt))
	require.NoError(t, err)

	testTraceId := model.NewTraceID(1, 47)
	writer := NewSpanWriter(pool, metrics.NullFactory, testutil.Zap(), opts)
	span := &model.Span{
		TraceID:       testTraceId,
		SpanID:        model.NewSpanID(1),
		StartTime:     dt,
		OperationName: "salute a Vulcan",
		Process:       model.NewProcess("svc", nil),
		Tags: []model.KeyValue{
			model.Int64("foo", 42),
			model.String("bar", "baz"),
		},
	}
	err = writer.WriteSpan(context.Background(), span)
	if !assert.NoError(t, err) {
		return
	}
	<-time.After(time.Second * 5)

	ctx, cancel = context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	r := setUpArchiveReader(t)
	trace, err := r.GetTrace(ctx, testTraceId)
	if !assert.NoError(t, err) {
		return
	}
	assert.NotEmpty(t, trace)
	assert.Equal(t, "svc", span.Process.ServiceName)
	span = trace.FindSpanByID(model.NewSpanID(1))
	assert.NotEmpty(t, span)
}

func setUpArchiveReader(t *testing.T) *reader.SpanReader {
	return reader.NewSpanReader(
		testutil.YdbSessionPool(t),
		reader.SpanReaderOptions{
			DbPath:        schema.DbPath{Path: os.Getenv("YDB_PATH"), Folder: os.Getenv("YDB_FOLDER")},
			ReadTimeout:   time.Second * 10,
			QueryParallel: 10,
			ArchiveReader: true,
		},
		testutil.Zap(),
	)
}
