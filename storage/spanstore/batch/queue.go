package batch

import (
	"context"
	"errors"
	"time"

	"github.com/uber/jaeger-lib/metrics"
)

const (
	defaultBufferSize = 2000
)

var ErrOverflow = errors.New("writer buffer overflow")

// Queue represents queue of message batches
type Queue struct {
	opts        Options
	inFlight    chan *batch
	itemBuffer  chan interface{}
	writer      Writer
	dropCounter metrics.Counter
}

type Writer interface {
	WriteItems(ctx context.Context, _ []interface{})
}

type Options struct {
	BufferSize   int
	BatchSize    int
	BatchWorkers int
}

func NewQueue(ctx context.Context, opts Options, mf metrics.Factory, writer Writer) *Queue {
	if opts.BufferSize <= 0 {
		opts.BufferSize = defaultBufferSize
	}

	q := &Queue{
		opts:        opts,
		inFlight:    make(chan *batch, 10),
		itemBuffer:  make(chan interface{}, opts.BufferSize),
		writer:      writer,
		dropCounter: mf.Counter(metrics.Options{Name: "dropped"}),
	}

	go q.inputProcessor(ctx)
	for i := 0; i < q.opts.BatchWorkers; i++ {
		go q.batchProcessor(ctx)
	}

	return q
}

func (w *Queue) Add(item interface{}) error {
	select {
	case w.itemBuffer <- item:
		return nil
	default:
		w.dropCounter.Inc(1)
		return ErrOverflow
	}
}

func (w *Queue) inputProcessor(ctx context.Context) {
	batch := newBatch(w.opts.BatchSize)
	flushTimer := time.NewTimer(time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-w.itemBuffer:
			batch.Append(item)
			if batch.Len() >= w.opts.BatchSize {
				w.inFlight <- batch
				batch = newBatch(w.opts.BatchSize)
			}
		case <-flushTimer.C:
			flushTimer.Reset(time.Second)
			if batch.Len() > 0 {
				w.inFlight <- batch
				batch = newBatch(w.opts.BatchSize)
			}
		}
	}
}

func (w *Queue) batchProcessor(ctx context.Context) {
	for b := range w.inFlight {
		w.writer.WriteItems(ctx, b.items)
	}
}
