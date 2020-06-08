package reader

import (
	"context"
	"sort"
	"sync"

	"github.com/yandex-cloud/jaeger-ydb-store/schema"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/dbmodel"
)

type bucketOperation func(ctx context.Context, bucket uint8)

func runBucketOperation(ctx context.Context, numBuckets uint8, callbackFunc bucketOperation) {
	wg := new(sync.WaitGroup)
	wg.Add(int(numBuckets))
	for i := uint8(0); i < numBuckets; i++ {
		go func(ctx context.Context, bucket uint8) {
			defer wg.Done()
			callbackFunc(ctx, bucket)
		}(ctx, i)
	}
	wg.Wait()
}

type partitionOperation func(ctx context.Context, key schema.PartitionKey)

func runPartitionOperation(ctx context.Context, parts []schema.PartitionKey, opFunc partitionOperation) {
	wg := new(sync.WaitGroup)
	wg.Add(len(parts))
	for _, part := range parts {
		go func(ctx context.Context, part schema.PartitionKey) {
			defer wg.Done()
			opFunc(ctx, part)
		}(ctx, part)
	}
	wg.Wait()
}

type sharedResult struct {
	Rows  []dbmodel.IndexResult
	Error error

	mx        *sync.Mutex
	cancelCtx context.CancelFunc
}

func newSharedResult(cancelFunc context.CancelFunc) *sharedResult {
	return &sharedResult{
		mx:        new(sync.Mutex),
		cancelCtx: cancelFunc,
		Rows:      make([]dbmodel.IndexResult, 0),
	}
}

func (r *sharedResult) AddRows(rows []dbmodel.IndexResult, err error) {
	r.mx.Lock()
	defer r.mx.Unlock()
	if err != nil {
		if r.Error == nil {
			r.Error = err
		}
		r.Rows = nil
		r.cancelCtx()
		return
	}
	for _, row := range rows {
		r.Rows = append(r.Rows, row)
	}
}

func (r *sharedResult) ProcessRows() (*dbmodel.UniqueTraceIDs, error) {
	if r.Error != nil {
		return nil, r.Error
	}
	sort.Slice(r.Rows, func(i, j int) bool {
		return r.Rows[i].RevTs < r.Rows[j].RevTs
	})
	ids := dbmodel.NewUniqueTraceIDs()
	for _, row := range r.Rows {
		for _, id := range row.Ids {
			ids.Add(id)
		}
	}
	return ids, nil
}
