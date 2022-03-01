package plugin

import (
	"context"
	ydbZap "github.com/ydb-platform/ydb-go-sdk-zap"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"time"

	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	jgrProm "github.com/uber/jaeger-lib/metrics/prometheus"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"go.uber.org/zap"

	"github.com/yandex-cloud/jaeger-ydb-store/internal/db"
	"github.com/yandex-cloud/jaeger-ydb-store/schema"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/config"
	ydbDepStore "github.com/yandex-cloud/jaeger-ydb-store/storage/dependencystore"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/reader"
	"github.com/yandex-cloud/jaeger-ydb-store/storage/spanstore/writer"
)

type YdbStorage struct {
	metricsFactory  metrics.Factory
	metricsRegistry *prometheus.Registry
	logger          *zap.Logger
	ydbPool         table.Client
	opts            config.Options

	writer        *writer.SpanWriter
	reader        *reader.SpanReader
	archiveWriter *writer.SpanWriter
	archiveReader *reader.SpanReader
}

func NewYdbStorage() *YdbStorage {
	registry := prometheus.NewRegistry()
	return &YdbStorage{
		metricsRegistry: registry,
		metricsFactory:  jgrProm.New(jgrProm.WithRegisterer(registry)).Namespace(metrics.NSOptions{Name: "jaeger_ydb"}),
	}
}

// InitFromViper pops settings from flags/env
func (p *YdbStorage) InitFromViper(v *viper.Viper) {
	v.SetDefault(db.KeyYdbConnectTimeout, time.Second*10)
	v.SetDefault(db.KeyYdbWriterBufferSize, 1000)
	v.SetDefault(db.KeyYdbWriterBatchSize, 100)
	v.SetDefault(db.KeyYdbWriterBatchWorkers, 10)
	v.SetDefault(db.KeyYdbWriterSvcOpCacheSize, 256)
	v.SetDefault(db.KeyYdbIndexerBufferSize, 1000)
	v.SetDefault(db.KeyYdbIndexerMaxTraces, 100)
	v.SetDefault(db.KeyYdbIndexerMaxTTL, time.Second*5)
	v.SetDefault(db.KeyYdbPoolSize, 100)
	v.SetDefault(db.KeyYdbQueryCacheSize, 50)
	v.SetDefault(db.KeyYdbWriteTimeout, time.Second)
	v.SetDefault(db.KeyYdbReadTimeout, time.Second*10)
	v.SetDefault(db.KeyYdbReadQueryParallel, 16)
	v.SetDefault(db.KeyYdbReadOpLimit, 5000)
	v.SetDefault(db.KeyYdbReadSvcLimit, 1000)
	// Zero stands for "unbound" interval so any span age is good.
	v.SetDefault(db.KeyYdbWriterMaxSpanAge, time.Duration(0))
	p.opts = config.Options{
		DbAddress: v.GetString(db.KeyYdbAddress),
		DbPath: schema.DbPath{
			Path:   v.GetString(db.KeyYdbPath),
			Folder: v.GetString(db.KeyYdbFolder),
		},
		PoolSize:            v.GetInt(db.KeyYdbPoolSize),
		QueryCacheSize:      v.GetInt(db.KeyYdbQueryCacheSize),
		ConnectTimeout:      v.GetDuration(db.KeyYdbConnectTimeout),
		BufferSize:          v.GetInt(db.KeyYdbWriterBufferSize),
		BatchSize:           v.GetInt(db.KeyYdbWriterBatchSize),
		BatchWorkers:        v.GetInt(db.KeyYdbWriterBatchWorkers),
		WriteSvcOpCacheSize: v.GetInt(db.KeyYdbWriterSvcOpCacheSize),
		IndexerBufferSize:   v.GetInt(db.KeyYdbIndexerBufferSize),
		IndexerMaxTraces:    v.GetInt(db.KeyYdbIndexerMaxTraces),
		IndexerMaxTTL:       v.GetDuration(db.KeyYdbIndexerMaxTTL),
		WriteTimeout:        v.GetDuration(db.KeyYdbWriteTimeout),
		ReadTimeout:         v.GetDuration(db.KeyYdbReadTimeout),
		ReadQueryParallel:   v.GetInt(db.KeyYdbReadQueryParallel),
		ReadOpLimit:         v.GetUint64(db.KeyYdbReadOpLimit),
		ReadSvcLimit:        v.GetUint64(db.KeyYdbReadSvcLimit),
		WriteMaxSpanAge:     v.GetDuration(db.KeyYdbWriterMaxSpanAge),
	}
	var err error
	cfg := zap.NewProductionConfig()
	if logPath := v.GetString("plugin_log_path"); logPath != "" {
		cfg.ErrorOutputPaths = []string{logPath}
		cfg.OutputPaths = []string{logPath}
	}
	p.logger, err = cfg.Build()
	if err != nil {
		panic(err)
	}
	p.initDB(
		v,
		ydbZap.WithTraces(
			p.logger,
			trace.DetailsAll,
		),
	)
	p.initWriters()
	p.initReaders()
}

func (p *YdbStorage) Registry() *prometheus.Registry {
	return p.metricsRegistry
}

func (p *YdbStorage) SpanReader() spanstore.Reader {
	return p.reader
}

func (p *YdbStorage) SpanWriter() spanstore.Writer {
	return p.writer
}

func (p *YdbStorage) ArchiveSpanReader() spanstore.Reader {
	return p.archiveReader
}

func (p *YdbStorage) ArchiveSpanWriter() spanstore.Writer {
	return p.archiveWriter
}

func (*YdbStorage) DependencyReader() dependencystore.Reader {
	return ydbDepStore.DependencyStore{}
}

func (p *YdbStorage) initDB(v *viper.Viper, opts ...ydb.Option) {
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.ConnectTimeout)
	defer cancel()

	opts = append(
		opts,
		ydb.WithEndpoint(p.opts.DbAddress),
		ydb.WithDatabase(p.opts.DbPath.Path),
		ydb.WithSessionPoolSizeLimit(p.opts.PoolSize),
		ydb.WithSessionPoolKeepAliveTimeout(time.Second),
		ydb.WithTraceTable(tableClientMetrics(p.metricsFactory)),
	)

	conn, err := db.DialFromViper(ctx, v, opts...)
	if err != nil {
		p.logger.Fatal("db init failed", zap.Error(err))
	}

	p.ydbPool = conn.Table()
}

func (p *YdbStorage) initWriters() {
	opts := writer.SpanWriterOptions{
		BufferSize:        p.opts.BufferSize,
		BatchSize:         p.opts.BatchSize,
		BatchWorkers:      p.opts.BatchWorkers,
		IndexerBufferSize: p.opts.IndexerBufferSize,
		IndexerMaxTraces:  p.opts.IndexerMaxTraces,
		IndexerTTL:        p.opts.IndexerMaxTTL,
		DbPath:            p.opts.DbPath,
		WriteTimeout:      p.opts.WriteTimeout,
		OpCacheSize:       p.opts.WriteSvcOpCacheSize,
		MaxSpanAge:        p.opts.WriteMaxSpanAge,
	}
	ns := p.metricsFactory.Namespace(metrics.NSOptions{Name: "writer"})
	p.writer = writer.NewSpanWriter(p.ydbPool, ns, p.logger, opts)

	opts.ArchiveWriter = true
	p.archiveWriter = writer.NewSpanWriter(p.ydbPool, ns, p.logger, opts)
}

func (p *YdbStorage) initReaders() {
	opts := reader.SpanReaderOptions{
		DbPath:        p.opts.DbPath,
		ReadTimeout:   p.opts.ReadTimeout,
		QueryParallel: p.opts.ReadQueryParallel,
		OpLimit:       p.opts.ReadOpLimit,
		SvcLimit:      p.opts.ReadSvcLimit,
	}
	p.reader = reader.NewSpanReader(p.ydbPool, opts, p.logger)

	opts.ArchiveReader = true
	p.archiveReader = reader.NewSpanReader(p.ydbPool, opts, p.logger)
}
