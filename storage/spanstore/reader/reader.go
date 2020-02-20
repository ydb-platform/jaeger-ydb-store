package reader

import (
	"context"
	"errors"
	"fmt"
	"github.com/YandexClassifieds/jaeger-ydb-store/schema"
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/dbmodel"
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/queries"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/opentracing/opentracing-go"
	ottag "github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
	"time"
)

const (
	defaultNumTraces = 100
	// limitMultiple exists because many spans that are returned from indices can have the same trace, limitMultiple increases
	// the number of responses from the index, so we can respect the user's limit value they provided.
	limitMultiple = 3

	resultLimit = 1000

	partsCacheKey = "parts"
	partsTtl      = time.Minute
)

var (
	// ErrServiceNameNotSet occurs when attempting to query with an empty service name
	ErrServiceNameNotSet = status.Error(codes.InvalidArgument, "Service name must be set")

	// ErrStartTimeMinGreaterThanMax occurs when start time min is above start time max
	ErrStartTimeMinGreaterThanMax = status.Error(codes.InvalidArgument, "Start Time Minimum is above Maximum")

	// ErrDurationMinGreaterThanMax occurs when duration min is above duration max
	ErrDurationMinGreaterThanMax = status.Error(codes.InvalidArgument, "Duration Minimum is above Maximum")

	// ErrMalformedRequestObject occurs when a request object is nil
	ErrMalformedRequestObject = status.Error(codes.InvalidArgument, "Malformed request object")

	// ErrDurationAndTagQueryNotSupported occurs when duration and tags are both set
	ErrDurationAndTagQueryNotSupported = status.Error(codes.InvalidArgument, "Cannot query for duration and tags simultaneously")

	// ErrStartAndEndTimeNotSet occurs when start time and end time are not set
	ErrStartAndEndTimeNotSet = status.Error(codes.InvalidArgument, "Start and End Time must be set")

	ErrTraceNotFound = status.Error(codes.NotFound, "trace not found")

	ErrEmptyPartitionList = errors.New("partition list is empty")

	ErrNoPartitions = errors.New("no partitions to query")

	txc = table.TxControl(
		table.BeginTx(table.WithSerializableReadWrite()),
		table.CommitTx(),
	)
)

// SpanReader can query for and load traces from YDB.
type SpanReader struct {
	pool        *table.SessionPool
	dbPath      schema.DbPath
	logger      *zap.Logger
	readTimeout time.Duration
	cache       *ttlCache
}

type SpanReaderOptions struct {
	DbPath      schema.DbPath
	ReadTimeout time.Duration
}

// NewSpanReader returns a new SpanReader.
func NewSpanReader(pool *table.SessionPool, opts SpanReaderOptions, logger *zap.Logger) *SpanReader {
	return &SpanReader{
		pool:        pool,
		dbPath:      opts.DbPath,
		readTimeout: opts.ReadTimeout,
		logger:      logger,
		cache:       newTtlCache(),
	}
}

// GetServices returns all services traced by Jaeger
func (s *SpanReader) GetServices(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.readTimeout)
	defer cancel()
	result := make([]string, 0)
	err := table.Retry(ctx, s.pool, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		stmt, err := session.Prepare(ctx, queries.BuildQuery("query-services", s.dbPath))
		if err != nil {
			return err
		}
		_, res, err := stmt.Execute(ctx, txc, nil)
		if err != nil {
			return err
		}
		res.NextSet()
		for res.NextRow() {
			res.NextItem()
			result = append(result, res.OUTF8())
		}
		if err = res.Err(); err != nil {
			return err
		}
		return nil
	}))
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetOperations returns all operations for a specific service traced by Jaeger
func (s *SpanReader) GetOperations(ctx context.Context, service string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.readTimeout)
	defer cancel()
	result := make([]string, 0)
	err := table.Retry(ctx, s.pool, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		stmt, err := session.Prepare(ctx, queries.BuildQuery("query-operations", s.dbPath))
		if err != nil {
			return err
		}
		_, res, err := stmt.Execute(ctx, txc, table.NewQueryParameters(
			table.ValueParam("$service_name", ydb.UTF8Value(service)),
		))
		if err != nil {
			return err
		}

		res.NextSet()
		for res.NextRow() {
			res.NextItem()
			result = append(result, res.OUTF8())
		}
		if res.Err() != nil {
			return res.Err()
		}
		return nil
	}))
	if err != nil {
		return nil, err
	}
	return result, nil
}

// FindTraces retrieves traces that match the traceQuery
func (s *SpanReader) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FindTraces")
	defer span.Finish()

	uniqueTraceIDs, err := s.FindTraceIDs(ctx, query)
	if err != nil {
		return nil, err
	}
	var retMe []*model.Trace

	parts := schema.MakePartitionList(query.StartTimeMin, query.StartTimeMax)
	availableParts, err := s.getPartitionList(ctx)
	if err != nil {
		return nil, err
	}
	parts = schema.IntersectPartList(parts, availableParts)
	if len(parts) == 0 {
		return nil, ErrNoPartitions
	}

	numThreads := 16
	queryC := make(chan model.TraceID)
	mx := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	wg.Add(numThreads)
	childSpan, ctx := opentracing.StartSpanFromContext(ctx, "readTraces")
	defer childSpan.Finish()
	for i := 0; i < numThreads; i++ {
		go func() {
			defer wg.Done()
			for traceID := range queryC {
				if jTrace, err := s.readTraceFromPartitions(ctx, parts, traceID); err != nil {
					s.logger.Error("Failure to read trace", zap.String("trace_id", traceID.String()), zap.Error(err))
				} else {
					mx.Lock()
					retMe = append(retMe, jTrace)
					mx.Unlock()
				}
			}
		}()
	}
	for _, traceID := range uniqueTraceIDs {
		queryC <- traceID
	}
	close(queryC)
	wg.Wait()

	return retMe, nil
}

// FindTraceIDs retrieve traceIDs that match the traceQuery
func (s *SpanReader) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "FindTraceIDs")
	defer span.Finish()
	ctx, cancel := context.WithTimeout(ctx, s.readTimeout)
	defer cancel()

	if err := validateQuery(query); err != nil {
		return nil, err
	}
	if query.NumTraces == 0 {
		query.NumTraces = defaultNumTraces
	}

	dbTraceIDs, err := s.findTraceIDs(ctx, query)
	if err != nil {
		return nil, err
	}

	var traceIDs []model.TraceID
	for _, t := range dbTraceIDs.AsList() {
		if len(traceIDs) >= query.NumTraces {
			break
		}
		traceIDs = append(traceIDs, t.ToDomain())
	}
	return traceIDs, nil
}

// GetTrace takes a traceID and returns a Trace associated with that traceID
func (s *SpanReader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	ctx, cancel := context.WithTimeout(ctx, s.readTimeout)
	defer cancel()
	span, ctx := opentracing.StartSpanFromContext(ctx, "GetTrace")
	defer span.Finish()
	span.LogFields(otlog.String("event", "searching"), otlog.Object("trace_id", traceID))
	return s.readTrace(ctx, traceID)
}

func (s *SpanReader) readTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	span, ctx := startSpanForQuery(ctx, "readTrace")
	defer span.Finish()
	span.LogFields(otlog.String("event", "searching"), otlog.Object("trace_id", traceID))

	parts, err := s.getPartitionList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch partitions list: %w", err)
	}

	trace, err := s.readTraceFromPartitions(ctx, parts, traceID)
	logErrorToSpan(span, err)
	return trace, err
}

func (s *SpanReader) queryPartitionList(ctx context.Context) ([]schema.PartitionKey, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "queryPartitions")
	defer span.Finish()
	var result []schema.PartitionKey
	err := table.Retry(ctx, s.pool, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		stmt, err := session.Prepare(ctx, schema.BuildQuery(s.dbPath, schema.QueryActiveParts))
		if err != nil {
			return err
		}
		_, res, err := stmt.Execute(ctx, txc, nil)
		if err != nil {
			return err
		}

		result = make([]schema.PartitionKey, 0, res.RowCount())
		part := schema.PartitionKey{}
		for res.NextSet() {
			for res.NextRow() {
				if err = part.Scan(res); err != nil {
					return err
				}
				result = append(result, part)
			}
		}
		return nil
	}))
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, ErrEmptyPartitionList
	}
	return result, nil
}

func (s *SpanReader) getPartitionList(ctx context.Context) ([]schema.PartitionKey, error) {
	if data, ok := s.cache.Get(partsCacheKey); ok {
		return data.([]schema.PartitionKey), nil
	}
	data, err := s.queryPartitionList(ctx)
	if err != nil {
		return nil, err
	}
	s.cache.Set(partsCacheKey, data, partsTtl)
	return data, nil
}

func (s *SpanReader) readTraceFromPartitions(ctx context.Context, parts []schema.PartitionKey, traceID model.TraceID) (*model.Trace, error) {
	mx := new(sync.Mutex)
	result := &model.Trace{}
	var resultErr error
	runPartitionOperation(ctx, parts, func(ctx context.Context, key schema.PartitionKey) {
		spans, err := s.spansFromPartition(ctx, key, traceID)
		mx.Lock()
		defer mx.Unlock()
		if err != nil {
			if resultErr == nil {
				resultErr = err
			}
			return
		}
		result.Spans = append(result.Spans, spans...)
	})
	if resultErr != nil {
		return nil, resultErr
	}
	if len(result.Spans) == 0 {
		return nil, ErrTraceNotFound
	}
	return result, nil
}

func (s *SpanReader) spansFromPartition(ctx context.Context, part schema.PartitionKey, traceID model.TraceID) ([]*model.Span, error) {
	var result []*model.Span
	span, ctx := opentracing.StartSpanFromContext(ctx, "spansFromPartition")
	defer span.Finish()
	err := table.Retry(ctx, s.pool, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		stmt, err := session.Prepare(ctx, queries.BuildPartitionQuery("querySpanCount", s.dbPath, part))
		if err != nil {
			return err
		}
		_, res, err := stmt.Execute(ctx, txc, table.NewQueryParameters(
			table.ValueParam("$trace_id_high", ydb.Uint64Value(traceID.High)),
			table.ValueParam("$trace_id_low", ydb.Uint64Value(traceID.Low)),
		))
		if err != nil {
			return err
		}

		var numSpans int
		if res.NextSet() && res.NextRow() && res.NextItem() {
			numSpans = int(res.Uint64())
			if err := res.Err(); err != nil {
				return fmt.Errorf("failed to read spancount result: %w", err)
			}
		}
		if numSpans == 0 {
			return nil
		}
		result = make([]*model.Span, 0, numSpans)

		dbSpan := dbmodel.Span{}
		var span *model.Span
		for i := 0; i < numSpans/1000+1; i++ {
			offset := i * resultLimit
			stmt, err := session.Prepare(ctx, queries.BuildPartitionQuery("queryByTraceID", s.dbPath, part))
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, txc, table.NewQueryParameters(
				table.ValueParam("$trace_id_high", ydb.Uint64Value(traceID.High)),
				table.ValueParam("$trace_id_low", ydb.Uint64Value(traceID.Low)),
				table.ValueParam("$limit", ydb.Uint64Value(uint64(resultLimit))),
				table.ValueParam("$offset", ydb.Uint64Value(uint64(offset))),
			))
			if err != nil {
				return err
			}
			for res.NextSet() {
				for res.NextRow() {
					if err = dbSpan.Scan(res); err != nil {
						return fmt.Errorf("span.Scan failed: %w", err)
					}
					if span, err = dbmodel.ToDomain(&dbSpan); err != nil {
						return err
					}
					result = append(result, span)
				}
			}

			if err = res.Err(); err != nil {
				return fmt.Errorf("failed to read spans: %w", err)
			}
		}
		return nil
	}))
	if err != nil {
		logErrorToSpan(span, err)
		return nil, err
	}
	return result, nil
}

func (s *SpanReader) findTraceIDs(ctx context.Context, traceQuery *spanstore.TraceQueryParameters) (*dbmodel.UniqueTraceIDs, error) {
	if traceQuery.DurationMin != 0 || traceQuery.DurationMax != 0 {
		return s.queryByDuration(ctx, traceQuery)
	}
	if len(traceQuery.Tags) > 0 {
		return s.queryByTagsAndLogs(ctx, traceQuery)
	}
	if traceQuery.OperationName != "" {
		traceIds, err := s.queryByServiceNameAndOperation(ctx, traceQuery)
		if err != nil {
			return nil, err
		}
		return traceIds, nil
	}
	return s.queryByService(ctx, traceQuery)
}

func (s *SpanReader) queryByTagsAndLogs(ctx context.Context, tq *spanstore.TraceQueryParameters) (*dbmodel.UniqueTraceIDs, error) {
	span, ctx := startSpanForQuery(ctx, "queryByTagsAndLogs")
	defer span.Finish()

	results := make([]*dbmodel.UniqueTraceIDs, 0, len(tq.Tags))
	parts := schema.MakePartitionList(tq.StartTimeMin, tq.StartTimeMax)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for k, v := range tq.Tags {
		childSpan, ctx := opentracing.StartSpanFromContext(ctx, "queryByTag")
		childSpan.LogFields(otlog.String("tag.key", k), otlog.String("tag.value", v))

		result := newSharedResult(cancel)
		runBucketOperation(ctx, dbmodel.NumIndexBuckets, func(ctx context.Context, bucket uint8) {
			hash := dbmodel.HashTagIndex(tq.ServiceName, tq.OperationName, k, v, bucket)
			span, ctx := opentracing.StartSpanFromContext(ctx, "queryBucket", opentracing.Tags{"bucket": bucket, "hash": hash})
			defer span.Finish()
			values := []table.ParameterOption{
				table.ValueParam("$hash", ydb.Uint64Value(hash)),
				table.ValueParam("$time_min", ydb.Int64Value(tq.StartTimeMin.UnixNano())),
				table.ValueParam("$time_max", ydb.Int64Value(tq.StartTimeMax.UnixNano())),
			}
			ids, err := s.queryParallel(ctx, parts, "queryByTag", tq, values...)
			result.AddRows(ids, err)
		})
		if ids, err := result.ProcessRows(); err == nil {
			results = append(results, ids)
		} else {
			return nil, err
		}
		childSpan.Finish()
	}
	return dbmodel.IntersectTraceIDs(results), nil
}

func (s *SpanReader) queryByDuration(ctx context.Context, tq *spanstore.TraceQueryParameters) (*dbmodel.UniqueTraceIDs, error) {
	span, ctx := startSpanForQuery(ctx, "queryByDuration")
	defer span.Finish()

	minDurationNano := tq.DurationMin.Nanoseconds()
	maxDurationNano := (time.Hour * 24).Nanoseconds()
	if tq.DurationMax != 0 {
		maxDurationNano = tq.DurationMax.Nanoseconds()
	}
	parts := schema.MakePartitionList(tq.StartTimeMin, tq.StartTimeMax)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := newSharedResult(cancel)
	runBucketOperation(ctx, dbmodel.NumIndexBuckets, func(ctx context.Context, bucket uint8) {
		hash := dbmodel.HashBucketData(bucket, tq.ServiceName, tq.OperationName)
		values := []table.ParameterOption{
			table.ValueParam("$hash", ydb.Uint64Value(hash)),
			table.ValueParam("$duration_min", ydb.Int64Value(minDurationNano)),
			table.ValueParam("$duration_max", ydb.Int64Value(maxDurationNano)),
		}
		ids, err := s.queryParallel(ctx, parts, "queryByDuration", tq, values...)
		result.AddRows(ids, err)
	})
	if ids, err := result.ProcessRows(); err == nil {
		return trimResults(ids, tq.NumTraces), nil
	} else {
		return nil, err
	}
}

func (s *SpanReader) queryByServiceNameAndOperation(ctx context.Context, tq *spanstore.TraceQueryParameters) (*dbmodel.UniqueTraceIDs, error) {
	span, ctx := startSpanForQuery(ctx, "queryByServiceNameAndOperation")
	defer span.Finish()
	values := []table.ParameterOption{
		table.ValueParam("$idx_hash", ydb.Uint64Value(dbmodel.HashData(tq.ServiceName, tq.OperationName))),
	}
	parts := schema.MakePartitionList(tq.StartTimeMin, tq.StartTimeMax)
	ctx, cancel := context.WithCancel(ctx)
	sr := newSharedResult(cancel)
	sr.AddRows(s.queryParallel(ctx, parts, "queryByServiceAndOperationName", tq, values...))
	return sr.ProcessRows()
}

func (s *SpanReader) queryByService(ctx context.Context, tq *spanstore.TraceQueryParameters) (*dbmodel.UniqueTraceIDs, error) {
	span, ctx := startSpanForQuery(ctx, "queryByService")
	defer span.Finish()

	parts := schema.MakePartitionList(tq.StartTimeMin, tq.StartTimeMax)
	ctx, cancel := context.WithCancel(ctx)
	sr := newSharedResult(cancel)
	runBucketOperation(ctx, dbmodel.NumIndexBuckets, func(ctx context.Context, bucket uint8) {
		hashParam := table.ValueParam("$idx_hash", ydb.Uint64Value(dbmodel.HashBucketData(bucket, tq.ServiceName)))
		sr.AddRows(s.queryParallel(ctx, parts, "queryByServiceName", tq, hashParam))
	})
	return sr.ProcessRows()
}

func (s *SpanReader) queryParallel(ctx context.Context, parts []schema.PartitionKey, queryName string, tq *spanstore.TraceQueryParameters, values ...table.ParameterOption) ([]dbmodel.IndexResult, error) {
	availableParts, err := s.getPartitionList(ctx)
	if err != nil {
		return nil, err
	}
	parts = schema.IntersectPartList(parts, availableParts)
	if len(parts) == 0 {
		return nil, ErrNoPartitions
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := newSharedResult(cancel)
	runPartitionOperation(ctx, parts, func(ctx context.Context, part schema.PartitionKey) {
		result.AddRows(s.queryInPartition(ctx, queryName, part, tq, values...))
	})
	return result.Rows, result.Error
}

func (s *SpanReader) queryInPartition(ctx context.Context, queryName string, part schema.PartitionKey, tq *spanstore.TraceQueryParameters, values ...table.ParameterOption) ([]dbmodel.IndexResult, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "queryPartition", opentracing.Tag{Key: "partition", Value: part.Suffix()})
	defer span.Finish()

	limit := tq.NumTraces * limitMultiple
	query := queries.BuildPartitionQuery(queryName, s.dbPath, part)
	timeStart, timeEnd := part.TimeSpan()
	if tq.StartTimeMin.Sub(timeStart) > 0 {
		timeStart = tq.StartTimeMin
	}
	if tq.StartTimeMax.Sub(timeEnd) < 0 {
		timeEnd = tq.StartTimeMax
	}

	values = append(values,
		table.ValueParam("$time_min", ydb.Int64Value(timeStart.UnixNano())),
		table.ValueParam("$time_max", ydb.Int64Value(timeEnd.UnixNano())),
		table.ValueParam("$limit", ydb.Uint64Value(uint64(limit))),
	)
	return s.execQuery(ctx, span, query, values...)
}

func (s *SpanReader) execQuery(ctx context.Context, span opentracing.Span, query string, values ...table.ParameterOption) ([]dbmodel.IndexResult, error) {
	var result []dbmodel.IndexResult
	err := table.Retry(ctx, s.pool, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		var (
			err  error
			stmt *table.Statement
		)
		if stmt, err = session.Prepare(ctx, query); err != nil {
			return err
		}

		_, res, err := stmt.Execute(ctx, txc, table.NewQueryParameters(values...))
		if err != nil {
			return err
		}
		res.NextSet()
		result = make([]dbmodel.IndexResult, 0, res.RowCount())
		for res.NextRow() {
			qr := dbmodel.IndexResult{}
			if err := qr.Scan(res); err != nil {
				return err
			}
			result = append(result, qr)
		}
		if res.Err() != nil {
			return res.Err()
		}
		return nil
	}))
	if err != nil {
		span.LogFields(otlog.String("query", query))
		s.logger.Error("Failed to exec query", zap.Error(err), zap.String("query", query))
		return nil, err
	}
	return result, nil
}

func validateQuery(p *spanstore.TraceQueryParameters) error {
	if p == nil {
		return ErrMalformedRequestObject
	}
	if p.ServiceName == "" && len(p.Tags) > 0 {
		return ErrServiceNameNotSet
	}
	if p.StartTimeMin.IsZero() || p.StartTimeMax.IsZero() {
		return ErrStartAndEndTimeNotSet
	}
	if !p.StartTimeMin.IsZero() && !p.StartTimeMax.IsZero() && p.StartTimeMax.Before(p.StartTimeMin) {
		return ErrStartTimeMinGreaterThanMax
	}
	if p.DurationMin != 0 && p.DurationMax != 0 && p.DurationMin > p.DurationMax {
		return ErrDurationMinGreaterThanMax
	}
	if (p.DurationMin != 0 || p.DurationMax != 0) && len(p.Tags) > 0 {
		return ErrDurationAndTagQueryNotSupported
	}
	return nil
}

func startSpanForQuery(ctx context.Context, name string) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, name)
	ottag.DBType.Set(span, "ydb")
	ottag.Component.Set(span, "yql")
	return span, ctx
}

func logErrorToSpan(span opentracing.Span, err error) {
	if err == nil {
		return
	}
	ottag.Error.Set(span, true)
	span.LogFields(otlog.Error(err))
}

func trimResults(ids *dbmodel.UniqueTraceIDs, limit int) *dbmodel.UniqueTraceIDs {
	results := dbmodel.NewUniqueTraceIDs()
	for _, traceID := range ids.AsList() {
		results.Add(traceID)
		if results.Len() == limit {
			break
		}
	}
	return results
}
