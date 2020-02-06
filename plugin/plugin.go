package plugin

import (
	"context"
	"github.com/YandexClassifieds/jaeger-ydb-store/internal/db"
	"time"

	"github.com/YandexClassifieds/jaeger-ydb-store/schema"
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/config"
	ydbDepStore "github.com/YandexClassifieds/jaeger-ydb-store/storage/dependencystore"
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/reader"
	"github.com/YandexClassifieds/jaeger-ydb-store/storage/spanstore/writer"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	jgrProm "github.com/uber/jaeger-lib/metrics/prometheus"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"go.uber.org/zap"
)

const (
	keyYdbAddress          = "ydb.address"
	keyYdbPath             = "ydb.path"
	keyYdbFolder           = "ydb.folder"
	keyYdbConnectTimeout   = "ydb.connect-timeout"
	keyYdbWriteTimeout     = "ydb.write-timeout"
	keyYdbReadTimeout      = "ydb.read-timeout"
	keyYdbPoolSize         = "ydb.pool-size"
	keyYdbQueryCacheSize   = "ydb.query-cache-size"
	keyWriterBufferSize    = "ydb.writer.buffer-size"
	keyWriterBatchSize     = "ydb.writer.batch-size"
	keyWriterBatchWorkers  = "ydb.writer.batch-workers"
	keyIndexerBufferSize   = "ydb.indexer.buffer-size"
	keyIndexerMaxTraces    = "ydb.indexer.max-traces"
	keyIndexerMaxTTL       = "ydb.indexer.max-ttl"
)

type YdbStorage struct {
	metricsFactory  metrics.Factory
	metricsRegistry *prometheus.Registry
	logger          *zap.Logger
	ydbPool         *table.SessionPool
	opts            config.Options

	writer *writer.SpanWriter
	reader *reader.SpanReader
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
	v.SetDefault(keyYdbConnectTimeout, time.Second*10)
	v.SetDefault(keyWriterBufferSize, 1000)
	v.SetDefault(keyWriterBatchSize, 100)
	v.SetDefault(keyWriterBatchWorkers, 10)
	v.SetDefault(keyIndexerBufferSize, 1000)
	v.SetDefault(keyIndexerMaxTraces, 100)
	v.SetDefault(keyIndexerMaxTTL, time.Second*5)
	v.SetDefault(keyYdbPoolSize, 100)
	v.SetDefault(keyYdbQueryCacheSize, 50)
	v.SetDefault(keyYdbWriteTimeout, time.Second)
	v.SetDefault(keyYdbReadTimeout, time.Second*10)
	p.opts = config.Options{
		DbAddress:         v.GetString(keyYdbAddress),
		DbPath:            schema.DbPath{Path: v.GetString(keyYdbPath), Folder: v.GetString(keyYdbFolder)},
		PoolSize:          v.GetInt(keyYdbPoolSize),
		QueryCacheSize:    v.GetInt(keyYdbQueryCacheSize),
		ConnectTimeout:    v.GetDuration(keyYdbConnectTimeout),
		BufferSize:        v.GetInt(keyWriterBufferSize),
		BatchSize:         v.GetInt(keyWriterBatchSize),
		BatchWorkers:      v.GetInt(keyWriterBatchWorkers),
		IndexerBufferSize: v.GetInt(keyIndexerBufferSize),
		IndexerMaxTraces:  v.GetInt(keyIndexerMaxTraces),
		IndexerMaxTTL:     v.GetDuration(keyIndexerMaxTTL),
		WriteTimeout:      v.GetDuration(keyYdbWriteTimeout),
		ReadTimeout:       v.GetDuration(keyYdbReadTimeout),
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
	p.initDB(v)
	p.initWriter()
	p.initReader()
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

func (*YdbStorage) DependencyReader() dependencystore.Reader {
	return ydbDepStore.DependencyStore{}
}

func (p *YdbStorage) initDB(v *viper.Viper) {
	dialer, err := db.DialerFromViper(v)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.opts.ConnectTimeout)
	defer cancel()

	driver, err := dialer.Dial(ctx, p.opts.DbAddress)
	if err != nil {
		p.logger.Fatal("db init failed", zap.Error(err))
	}
	tc := &table.Client{
		Driver:            driver,
		MaxQueryCacheSize: p.opts.QueryCacheSize,
		Trace:             tableClientMetrics(p.metricsFactory),
	}
	p.ydbPool = &table.SessionPool{
		SizeLimit:          p.opts.PoolSize,
		KeepAliveBatchSize: -1,
		KeepAliveTimeout:   time.Second,
		Builder:            tc,
	}
}

func (p *YdbStorage) initWriter() {
	opts := writer.SpanWriterOptions{
		BufferSize:        p.opts.BufferSize,
		BatchSize:         p.opts.BatchSize,
		BatchWorkers:      p.opts.BatchWorkers,
		IndexerBufferSize: p.opts.IndexerBufferSize,
		IndexerMaxTraces:  p.opts.IndexerMaxTraces,
		IndexerTTL:        p.opts.IndexerMaxTTL,
		DbPath:            p.opts.DbPath,
		WriteTimeout:      p.opts.WriteTimeout,
	}
	ns := p.metricsFactory.Namespace(metrics.NSOptions{Name: "writer"})
	p.writer = writer.NewSpanWriter(p.ydbPool, ns, p.logger, opts)
}

func (p *YdbStorage) initReader() {
	opts := reader.SpanReaderOptions{
		DbPath:      p.opts.DbPath,
		ReadTimeout: p.opts.ReadTimeout,
	}
	p.reader = reader.NewSpanReader(p.ydbPool, opts, p.logger)
}
