package reader

import (
	"context"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
	"github.com/uber/jaeger-lib/metrics"

	"github.com/yandex-cloud/jaeger-ydb-store/internal/testutil"
	"github.com/yandex-cloud/jaeger-ydb-store/schema"
	ydbWriter "github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/writer"
)

func TestArchiveSpanReader_GetTrace(t *testing.T) {
	addArchiveTestData(t)
	s := setUpArchiveReader(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	trace, err := s.GetTrace(ctx, model.NewTraceID(1, 42))
	if !assert.NoError(t, err) {
		return
	}

	assert.Len(t, trace.Spans, 2)
	assert.Equal(t, model.NewTraceID(1, 42), trace.Spans[0].TraceID)
	assert.Equal(t, model.SpanID(42), trace.Spans[0].SpanID)
}

var archiveOnce = new(sync.Once)

func addArchiveTestData(t *testing.T) {
	archiveOnce.Do(func() {
		addArchiveTestDataOnce(t)
	})
}

func addArchiveTestDataOnce(t *testing.T) {
	var err error
	opts := ydbWriter.SpanWriterOptions{
		BatchWorkers:      1,
		BatchSize:         1,
		IndexerBufferSize: 100,
		IndexerMaxTraces:  10,
		IndexerTTL:        time.Second,
		DbPath:            schema.DbPath{Path: os.Getenv("YDB_PATH"), Folder: os.Getenv("YDB_FOLDER")},
		WriteTimeout:      time.Second,
		ArchiveWriter:     true,
		OpCacheSize:       256,
	}
	writer := ydbWriter.NewSpanWriter(testutil.YdbSessionPool(t), metrics.NullFactory, testutil.Zap(), opts)

	spans := []*model.Span{
		{
			TraceID:       model.NewTraceID(1, 42),
			SpanID:        model.NewSpanID(42),
			StartTime:     time.Now(),
			Duration:      time.Second,
			Process:       &model.Process{ServiceName: "svc1"},
			OperationName: "this-stuff",
			Tags: []model.KeyValue{
				model.Int64("http.status_code", 200),
				model.String("some_tag", "some_value"),
			},
		},
		{
			TraceID:       model.NewTraceID(rand.Uint64(), rand.Uint64()),
			SpanID:        model.NewSpanID(1),
			StartTime:     time.Now(),
			Duration:      time.Second / 2,
			Process:       &model.Process{ServiceName: "svc2"},
			OperationName: "this-stuff",
			Tags: []model.KeyValue{
				model.Int64("http.status_code", 200),
				model.String("some_tag", "some_value"),
			},
		},
		{
			TraceID:       model.NewTraceID(1, 42),
			SpanID:        model.NewSpanID(43),
			StartTime:     time.Now(),
			Duration:      time.Second,
			Process:       &model.Process{ServiceName: "svc1"},
			OperationName: "that-stuff",
			Tags: []model.KeyValue{
				model.Int64("http.status_code", 404),
			},
		},
		{
			TraceID:       model.NewTraceID(2, 42),
			SpanID:        model.NewSpanID(1),
			StartTime:     time.Now().Add(time.Hour * 2),
			Duration:      time.Second*10 + time.Millisecond,
			Process:       &model.Process{ServiceName: "svc2"},
			OperationName: "that-stuff",
			Tags: []model.KeyValue{
				model.Int64("http.status_code", 504),
			},
		},
	}
	for _, span := range spans {
		err = writer.WriteSpan(context.Background(), span)
		if !assert.NoError(t, err) {
			return
		}
	}
	// wait for flush
	<-time.After(time.Second * 2)
}

func setUpArchiveReader(t *testing.T) *SpanReader {
	return NewSpanReader(
		testutil.YdbSessionPool(t),
		SpanReaderOptions{
			DbPath:        schema.DbPath{Path: os.Getenv("YDB_PATH"), Folder: os.Getenv("YDB_FOLDER")},
			ReadTimeout:   time.Second * 10,
			QueryParallel: 10,
			ArchiveReader: true,
		},
		testutil.Zap(),
	)
}
