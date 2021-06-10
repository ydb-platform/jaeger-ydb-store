package reader

import (
	"context"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/stretchr/testify/assert"
	"github.com/uber/jaeger-lib/metrics"

	"github.com/yandex-cloud/jaeger-ydb-store/internal/testutil"
	"github.com/yandex-cloud/jaeger-ydb-store/schema"
	ydbWriter "github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/writer"
)

func TestSpanReader_GetTrace(t *testing.T) {
	addTestData(t)
	s := setUpReader(t)
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

func TestSpanReader_FindTraces(t *testing.T) {
	addTestData(t)
	s := setUpReader(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err := s.FindTraces(ctx, &spanstore.TraceQueryParameters{
		ServiceName:  "svc1",
		StartTimeMin: time.Now(),
		StartTimeMax: time.Now(),
	})
	assert.NoError(t, err)
}

func TestSpanReader_FindTraceIDs(t *testing.T) {
	addTestData(t)
	s := setUpReader(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	t.Run("duration", func(t *testing.T) {
		traces, err := s.FindTraces(ctx, &spanstore.TraceQueryParameters{
			ServiceName:  "svc2",
			StartTimeMin: time.Now().Add(-time.Hour),
			StartTimeMax: time.Now().Add(time.Hour * 3),
			DurationMin:  time.Second * 9,
			DurationMax:  time.Second * 12,
		})
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Len(t, traces, 1) {
			return
		}
		assert.Equal(t, "http.status_code", traces[0].Spans[0].Tags[0].Key)
		assert.Equal(t, "504", traces[0].Spans[0].Tags[0].AsString())
	})
	t.Run("service_name", func(t *testing.T) {
		ids, err := s.FindTraceIDs(ctx, &spanstore.TraceQueryParameters{
			ServiceName:  "svc1",
			StartTimeMin: time.Now().Add(-time.Hour),
			StartTimeMax: time.Now().Add(time.Hour * 2),
		})
		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, ids, 1)
	})
	t.Run("service_and_operation", func(t *testing.T) {
		ids, err := s.FindTraceIDs(ctx, &spanstore.TraceQueryParameters{
			ServiceName:   "svc1",
			OperationName: "this-stuff",
			StartTimeMin:  time.Now().Add(-time.Hour),
			StartTimeMax:  time.Now().Add(time.Hour),
		})
		assert.Len(t, ids, 1)
		assert.NoError(t, err)
	})
	t.Run("tags", func(t *testing.T) {
		ids, err := s.FindTraceIDs(ctx, &spanstore.TraceQueryParameters{
			ServiceName:  "svc1",
			StartTimeMin: time.Now().Add(-time.Hour),
			StartTimeMax: time.Now().Add(time.Hour),
			Tags: map[string]string{
				"some_tag": "some_value",
			},
		})
		assert.Len(t, ids, 1)
		assert.NoError(t, err)
	})
}

func TestSpanReader_GetServices(t *testing.T) {
	addTestData(t)
	s := setUpReader(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	services, err := s.GetServices(ctx)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	assert.Contains(t, services, "svc1")
	assert.Contains(t, services, "svc2")
}

func TestSpanReader_GetOperations(t *testing.T) {
	addTestData(t)
	s := setUpReader(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	ops, err := s.GetOperations(ctx, spanstore.OperationQueryParameters{ServiceName: "svc1"})
	assert.NoError(t, err)
	if err != nil {
		return
	}
	assert.Len(t, ops, 2)

	opNames := make(map[string]bool)
	for _, op := range ops {
		opNames[op.Name] = true
	}
	assert.Len(t, opNames, 2)
	assert.Contains(t, opNames, "this-stuff")
	assert.Contains(t, opNames, "that-stuff")
}

var once = new(sync.Once)

func addTestData(t *testing.T) {
	once.Do(func() {
		addTestDataOnce(t)
	})
}

func addTestDataOnce(t *testing.T) {
	var err error
	opts := ydbWriter.SpanWriterOptions{
		BatchWorkers:      1,
		BatchSize:         1,
		IndexerBufferSize: 100,
		IndexerMaxTraces:  10,
		IndexerTTL:        time.Second,
		DbPath:            schema.DbPath{Path: os.Getenv("YDB_PATH"), Folder: os.Getenv("YDB_FOLDER")},
		WriteTimeout:      time.Second,
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

func setUpReader(t *testing.T) *SpanReader {
	return NewSpanReader(
		testutil.YdbSessionPool(t),
		SpanReaderOptions{
			DbPath:        schema.DbPath{Path: os.Getenv("YDB_PATH"), Folder: os.Getenv("YDB_FOLDER")},
			ReadTimeout:   time.Second * 10,
			QueryParallel: 10,
			OpLimit:       100,
		},
		testutil.Zap(),
	)
}
