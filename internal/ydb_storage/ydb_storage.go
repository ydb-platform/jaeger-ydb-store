package ydb_storage

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/ydb-platform/jaeger-ydb-store/schema"

	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	jgrProm "github.com/uber/jaeger-lib/metrics/prometheus"
	"github.com/ydb-platform/jaeger-ydb-store/internal/db"
	"github.com/ydb-platform/jaeger-ydb-store/storage/config"
	ydbDepStore "github.com/ydb-platform/jaeger-ydb-store/storage/dependencystore"
	"github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/reader"
	"github.com/ydb-platform/jaeger-ydb-store/storage/spanstore/writer"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type ydbStorage struct {
	metricsFactory  metrics.Factory
	metricsRegistry *prometheus.Registry
	logger          hclog.Logger
	ydbPool         table.Client
	opts            config.Options

	writer        spanstore.Writer
	reader        spanstore.Reader
	archiveWriter spanstore.Writer
	archiveReader spanstore.Reader
}

func setDefaultYDBOptions(v *viper.Viper) {
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
}

func getYDBOptions(v *viper.Viper) config.Options {
	opts := config.Options{
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

	return opts
}

func NewYdbStorage(v *viper.Viper, logger hclog.Logger) (*ydbStorage, error) {
	registry := prometheus.NewRegistry()
	p := &ydbStorage{
		metricsRegistry: registry,
		metricsFactory:  jgrProm.New(jgrProm.WithRegisterer(registry)).Namespace(metrics.NSOptions{Name: "jaeger_ydb"}),
	}

	setDefaultYDBOptions(v)
	p.opts = getYDBOptions(v)

	p.logger = logger

	ctx, cancel := context.WithTimeout(context.Background(), p.opts.ConnectTimeout)
	defer cancel()
	conn, err := db.ConnectToYDB(
		ctx,
		v,
		sugar.DSN(p.opts.DbAddress, p.opts.DbPath.Path, true),
		ydb.WithSessionPoolSizeLimit(p.opts.PoolSize),
		ydb.WithSessionPoolKeepAliveTimeout(time.Second),
		ydb.WithTraceTable(tableClientMetrics(p.metricsFactory)),
	)
	if err != nil {
		return nil, fmt.Errorf("NewYdbStorage(): %w", err)
	}
	p.ydbPool = conn.Table()

	p.writer = p.newWriter()
	p.archiveWriter = p.newArchiveWriter()

	p.reader = p.newReader()
	p.archiveReader = p.newArchiveReader()

	return p, nil
}

func (p *ydbStorage) newWriter() *writer.SpanWriter {
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
	return writer.NewSpanWriter(p.ydbPool, ns, p.logger, opts)
}

func (p *ydbStorage) newArchiveWriter() *writer.SpanWriter {
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

		ArchiveWriter: true,
	}
	ns := p.metricsFactory.Namespace(metrics.NSOptions{Name: "writer"})
	return writer.NewSpanWriter(p.ydbPool, ns, p.logger, opts)
}

func (p *ydbStorage) newReader() *reader.SpanReader {
	opts := reader.SpanReaderOptions{
		DbPath:        p.opts.DbPath,
		ReadTimeout:   p.opts.ReadTimeout,
		QueryParallel: p.opts.ReadQueryParallel,
		OpLimit:       p.opts.ReadOpLimit,
		SvcLimit:      p.opts.ReadSvcLimit,
	}
	return reader.NewSpanReader(p.ydbPool, opts, p.logger)
}

func (p *ydbStorage) newArchiveReader() *reader.SpanReader {
	opts := reader.SpanReaderOptions{
		DbPath:        p.opts.DbPath,
		ReadTimeout:   p.opts.ReadTimeout,
		QueryParallel: p.opts.ReadQueryParallel,
		OpLimit:       p.opts.ReadOpLimit,
		SvcLimit:      p.opts.ReadSvcLimit,
		ArchiveReader: true,
	}
	return reader.NewSpanReader(p.ydbPool, opts, p.logger)
}

func (p *ydbStorage) Registry() *prometheus.Registry {
	return p.metricsRegistry
}

func (p *ydbStorage) SpanReader() spanstore.Reader {
	return p.reader
}

func (p *ydbStorage) SpanWriter() spanstore.Writer {
	return p.writer
}

func (p *ydbStorage) ArchiveSpanReader() spanstore.Reader {
	return p.archiveReader
}

func (p *ydbStorage) ArchiveSpanWriter() spanstore.Writer {
	return p.archiveWriter
}

func (p *ydbStorage) DependencyReader() dependencystore.Reader {
	return ydbDepStore.DependencyStore{}
}
