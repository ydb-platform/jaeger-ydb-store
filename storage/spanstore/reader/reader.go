package reader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/opentracing/opentracing-go"
	ottag "github.com/opentracing/opentracing-go/ext"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yandex-cloud/jaeger-ydb-store/schema"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/dbmodel"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/queries"
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
		table.BeginTx(table.WithOnlineReadOnly(table.WithInconsistentReads())),
		table.CommitTx(),
	)
)

// SpanReader can query for and load traces from YDB.
var _ spanstore.Reader = (*SpanReader)(nil)

type SpanReader struct {
	pool   table.Client
	opts   SpanReaderOptions
	logger *zap.Logger
	cache  *ttlCache
}

type SpanReaderOptions struct {
	DbPath        schema.DbPath
	ReadTimeout   time.Duration
	OpLimit       uint64 // max number of operations to fetch from operation_names index
	QueryParallel int
	ArchiveReader bool
}

// NewSpanReader returns a new SpanReader.
func NewSpanReader(pool table.Client, opts SpanReaderOptions, logger *zap.Logger) *SpanReader {
	return &SpanReader{
		pool:   pool,
		opts:   opts,
		logger: logger,
		cache:  newTtlCache(),
	}
}

// GetServices returns all services traced by Jaeger
func (s *SpanReader) GetServices(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.ReadTimeout)
	defer cancel()
	result := make([]string, 0)
	err := s.pool.Do(ctx, func(ctx context.Context, session table.Session) error {
		_, res, err := session.Execute(
			ctx,
			txc,
			queries.BuildQuery("query-services", s.opts.DbPath),
			nil,
			options.WithQueryCachePolicy(options.WithQueryCachePolicyKeepInCache()),
		)
		if err != nil {
			return err
		}
		defer res.Close()

		for res.NextResultSet(ctx) {
			for res.NextRow() {
				var v string
				if err := res.ScanWithDefaults(&v); err != nil {
					return fmt.Errorf("scan fail: %w", err)
				}
				result = append(result, v)
			}
		}
		if err = res.Err(); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// GetOperations returns all operations for a specific service traced by Jaeger
func (s *SpanReader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.ReadTimeout)
	defer cancel()

	var prepQuery string
	var queryParameters *table.QueryParameters
	if len(query.SpanKind) > 0 {
		prepQuery = queries.BuildQuery("query-operations-with-kind", s.opts.DbPath)
		queryParameters = table.NewQueryParameters(
			table.ValueParam("$service_name", types.UTF8Value(query.ServiceName)),
			table.ValueParam("$span_kind", types.UTF8Value(query.SpanKind)),
			table.ValueParam("$limit", types.Uint64Value(s.opts.OpLimit)),
		)
	} else {
		prepQuery = queries.BuildQuery("query-operations", s.opts.DbPath)
		queryParameters = table.NewQueryParameters(
			table.ValueParam("$service_name", types.UTF8Value(query.ServiceName)),
			table.ValueParam("$limit", types.Uint64Value(s.opts.OpLimit)),
		)
	}

	result := make([]spanstore.Operation, 0)
	err := s.pool.Do(ctx, func(ctx context.Context, session table.Session) error {
		res, err := session.StreamExecuteScanQuery(ctx, prepQuery, queryParameters)
		if err != nil {
			return err
		}
		defer res.Close()

		for res.NextResultSet(ctx) {
			for res.NextRow() {
				v := spanstore.Operation{
					SpanKind: query.SpanKind,
				}
				if err := res.ScanWithDefaults(&v.Name); err != nil {
					return fmt.Errorf("scan failed: %w", err)
				}

				result = append(result, v)
			}
		}
		if res.Err() != nil {
			return res.Err()
		}
		return nil
	})
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

	queryC := make(chan model.TraceID)
	mx := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	wg.Add(s.opts.QueryParallel)
	childSpan, ctx := opentracing.StartSpanFromContext(ctx, "readTraces")
	defer childSpan.Finish()
	for i := 0; i < s.opts.QueryParallel; i++ {
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
	ctx, cancel := context.WithTimeout(ctx, s.opts.ReadTimeout)
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
	ctx, cancel := context.WithTimeout(ctx, s.opts.ReadTimeout)
	defer cancel()
	operationName := "GetTrace"
	if s.opts.ArchiveReader {
		operationName = "GetArchiveTrace"
	}
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName)
	defer span.Finish()
	span.LogFields(otlog.String("event", "searching"), otlog.Object("trace_id", traceID))
	return s.readTrace(ctx, traceID)
}

func (s *SpanReader) readTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	span, ctx := startSpanForQuery(ctx, "readTrace")
	defer span.Finish()
	span.LogFields(otlog.String("event", "searching"), otlog.Object("trace_id", traceID))

	if s.opts.ArchiveReader {
		trace, err := s.readArchiveTrace(ctx, traceID)
		logErrorToSpan(span, err)
		return trace, err
	}

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
	err := s.pool.Do(ctx, func(ctx context.Context, session table.Session) error {
		_, res, err := session.Execute(
			ctx,
			txc,
			schema.BuildQuery(s.opts.DbPath, schema.QueryActiveParts),
			nil,
			options.WithQueryCachePolicy(options.WithQueryCachePolicyKeepInCache()),
		)
		if err != nil {
			return err
		}
		defer res.Close()

		result = make([]schema.PartitionKey, 0, res.TotalRowCount())
		for res.NextResultSet(ctx, "part_date", "part_num", "is_active") {
			for res.NextRow() {
				part := schema.PartitionKey{}
				if err = res.ScanWithDefaults(&part.Date, &part.Num, &part.IsActive); err != nil {
					return err
				}
				result = append(result, part)
			}
		}
		return nil
	})
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
		spans, err := s.spansFromPartition(ctx, traceID, key)
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

func (s *SpanReader) readArchiveTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	spans, err := s.spansFromPartition(ctx, traceID, schema.PartitionKey{})
	if err != nil {
		return nil, err
	}
	if len(spans) == 0 {
		return nil, ErrTraceNotFound
	}

	return &model.Trace{
		Spans: spans,
	}, nil
}

func (s *SpanReader) spansFromPartition(ctx context.Context, traceID model.TraceID, part schema.PartitionKey) ([]*model.Span, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "spansFromPartition")

	var numSpans uint64
	var query string
	if s.opts.ArchiveReader {
		query = queries.BuildQuery("querySpanCount", s.opts.DbPath)
	} else {
		query = queries.BuildPartitionQuery("querySpanCount", s.opts.DbPath, part)
	}
	err := s.pool.Do(ctx, func(ctx context.Context, session table.Session) error {
		_, res, err := session.Execute(
			ctx,
			txc,
			query,
			table.NewQueryParameters(
				table.ValueParam("$trace_id_high", types.Uint64Value(traceID.High)),
				table.ValueParam("$trace_id_low", types.Uint64Value(traceID.Low)),
			),
			options.WithQueryCachePolicy(options.WithQueryCachePolicyKeepInCache()),
		)
		if err != nil {
			return err
		}
		defer res.Close()

		if res.NextResultSet(ctx) && res.NextRow() {
			if err := res.Scan(&numSpans); err != nil {
				return fmt.Errorf("failed to read spancount result: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if numSpans == 0 {
		return nil, nil
	}

	if s.opts.ArchiveReader {
		query = queries.BuildQuery("queryByTraceID", s.opts.DbPath)
	} else {
		query = queries.BuildPartitionQuery("queryByTraceID", s.opts.DbPath, part)
	}

	var result []*model.Span
	err = s.pool.Do(ctx, func(ctx context.Context, session table.Session) error {
		result = make([]*model.Span, 0, numSpans)
		dbSpan := dbmodel.Span{}
		var span *model.Span
		for i := uint64(0); i < numSpans/1000+1; i++ {
			offset := i * resultLimit
			_, res, err := session.Execute(
				ctx,
				txc,
				query,
				table.NewQueryParameters(
					table.ValueParam("$trace_id_high", types.Uint64Value(traceID.High)),
					table.ValueParam("$trace_id_low", types.Uint64Value(traceID.Low)),
					table.ValueParam("$limit", types.Uint64Value(uint64(resultLimit))),
					table.ValueParam("$offset", types.Uint64Value(offset)),
				),
			)
			if err != nil {
				return err
			}
			defer res.Close()

			for res.NextResultSet(ctx, "trace_id_low", "trace_id_high", "span_id", "operation_name", "flags", "start_time", "duration", "extra") {
				for res.NextRow() {
					err = res.ScanWithDefaults(
						&dbSpan.TraceIDLow,
						&dbSpan.TraceIDHigh,
						&dbSpan.SpanID,
						&dbSpan.OperationName,
						&dbSpan.Flags,
						&dbSpan.StartTime,
						&dbSpan.Duration,
						&dbSpan.Extra,
					)
					if err != nil {
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
	})
	logErrorToSpan(span, err) // will skip if err == nil
	return result, err
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
			hash := dbmodel.HashTagIndex(tq.ServiceName, k, v, bucket)
			span, ctx := opentracing.StartSpanFromContext(ctx, "queryBucket", opentracing.Tags{"bucket": bucket, "hash": hash})
			defer span.Finish()
			values := []table.ParameterOption{
				table.ValueParam("$hash", types.Uint64Value(hash)),
			}
			queryName := "queryByTag"
			if tq.OperationName != "" {
				values = append(values, table.ValueParam("$op_hash", types.Uint64Value(dbmodel.HashData(tq.OperationName))))
				queryName = "queryByTagAndOperation"
			}
			ids, err := s.queryParallel(ctx, parts, queryName, tq, values...)
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
			table.ValueParam("$hash", types.Uint64Value(hash)),
			table.ValueParam("$duration_min", types.Int64Value(minDurationNano)),
			table.ValueParam("$duration_max", types.Int64Value(maxDurationNano)),
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
		table.ValueParam("$hash", types.Uint64Value(dbmodel.HashData(tq.ServiceName, tq.OperationName))),
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
		hashParam := table.ValueParam("$hash", types.Uint64Value(dbmodel.HashBucketData(bucket, tq.ServiceName)))
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
	query := queries.BuildPartitionQuery(queryName, s.opts.DbPath, part)
	timeStart, timeEnd := part.TimeSpan()
	if tq.StartTimeMin.Sub(timeStart) > 0 {
		timeStart = tq.StartTimeMin
	}
	if tq.StartTimeMax.Sub(timeEnd) < 0 {
		timeEnd = tq.StartTimeMax
	}

	values = append(values,
		table.ValueParam("$time_min", types.Int64Value(timeStart.UnixNano())),
		table.ValueParam("$time_max", types.Int64Value(timeEnd.UnixNano())),
		table.ValueParam("$limit", types.Uint64Value(uint64(limit))),
	)
	return s.execQuery(ctx, span, query, values...)
}

func (s *SpanReader) execQuery(ctx context.Context, span opentracing.Span, query string, values ...table.ParameterOption) ([]dbmodel.IndexResult, error) {
	var result []dbmodel.IndexResult
	err := s.pool.Do(ctx, func(ctx context.Context, session table.Session) error {
		_, res, err := session.Execute(
			ctx,
			txc,
			query,
			table.NewQueryParameters(values...),
		)
		if err != nil {
			return err
		}
		defer res.Close()
		result = make([]dbmodel.IndexResult, 0, res.TotalRowCount())

		for res.NextResultSet(ctx, "trace_ids", "rev_start_time") {
			for res.NextRow() {
				qr := dbmodel.IndexResult{}
				if err := res.ScanWithDefaults(&qr.Ids, &qr.RevTs); err != nil {
					return fmt.Errorf("scan failed: %w", err)
				}
				result = append(result, qr)
			}
		}
		if res.Err() != nil {
			return res.Err()
		}
		return nil
	})
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
