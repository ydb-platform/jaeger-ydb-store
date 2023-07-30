package indexer

import (
	"errors"

	"github.com/hashicorp/go-hclog"

	"github.com/jaegertracing/jaeger/model"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"go.uber.org/zap"

	"github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/indexer/index"
)

const (
	tblTagIndex              = "idx_tag_v2"
	tblDurationIndex         = "idx_duration"
	tblServiceNameIndex      = "idx_service_name"
	tblServiceOperationIndex = "idx_service_op"
)

var ErrOverflow = errors.New("indexer buffer overflow")

type Indexer struct {
	opts         Options
	logger       *zap.Logger
	jaegerLogger hclog.Logger

	inputItems     chan *model.Span
	tagWriter      *indexWriter
	svcWriter      *indexWriter
	opWriter       *indexWriter
	durationWriter *indexWriter
	dropCounter    metrics.Counter
}

func StartIndexer(pool table.Client, mf metrics.Factory, logger *zap.Logger, jaegerLogger hclog.Logger, opts Options) *Indexer {
	indexer := &Indexer{
		logger:       logger,
		jaegerLogger: jaegerLogger,
		opts:         opts,

		inputItems:  make(chan *model.Span, opts.BufferSize),
		dropCounter: mf.Counter(metrics.Options{Name: "indexer_dropped"}),
	}
	indexer.tagWriter = startIndexWriter(pool, mf.Namespace(metrics.NSOptions{Name: "tag_index"}), logger, jaegerLogger, tblTagIndex, opts)
	indexer.svcWriter = startIndexWriter(pool, mf.Namespace(metrics.NSOptions{Name: "service_name_index"}), logger, jaegerLogger, tblServiceNameIndex, opts)
	indexer.opWriter = startIndexWriter(pool, mf.Namespace(metrics.NSOptions{Name: "service_operation_index"}), logger, jaegerLogger, tblServiceOperationIndex, opts)
	indexer.durationWriter = startIndexWriter(pool, mf.Namespace(metrics.NSOptions{Name: "duration_index"}), logger, jaegerLogger, tblDurationIndex, opts)

	go indexer.spanProcessor()

	return indexer
}

func (w *Indexer) Add(span *model.Span) error {
	select {
	case w.inputItems <- span:
		return nil
	default:
		w.dropCounter.Inc(1)
		return ErrOverflow
	}
}

func (w *Indexer) spanProcessor() {
	for span := range w.inputItems {
		for _, tag := range span.GetTags() {
			w.processTag(tag, span)
		}
		if spanProcess := span.GetProcess(); spanProcess != nil {
			for _, tag := range spanProcess.GetTags() {
				w.processTag(tag, span)
			}
		}
		w.svcWriter.Add(index.NewServiceNameIndex(span), span.TraceID)
		w.opWriter.Add(index.NewServiceOperationIndex(span), span.TraceID)
		if span.OperationName != "" {
			w.durationWriter.Add(index.NewDurationIndex(span, span.OperationName), span.TraceID)
		}
		w.durationWriter.Add(index.NewDurationIndex(span, ""), span.TraceID)
	}
}

func (w *Indexer) processTag(kv model.KeyValue, span *model.Span) {
	if shouldIndexTag(kv) {
		w.tagWriter.Add(index.NewTagIndex(span, kv), span.TraceID)
	}
}
