package main

import (
	"fmt"
	"io"
	stdLog "log"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"

	"github.com/ydb-platform/jaeger-ydb-store/internal/log"
	"github.com/ydb-platform/jaeger-ydb-store/internal/ydb_storage"

	"github.com/hashicorp/go-hclog"
	jaegerGrpc "github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	jaegerCfg "github.com/uber/jaeger-client-go/config"

	localViper "github.com/ydb-platform/jaeger-ydb-store/internal/viper"
)

func init() {
	viper.SetDefault("plugin_http_listen_address", ":15000")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.AutomaticEnv()
}

type storagePluginWithRegistry interface {
	shared.StoragePlugin
	shared.ArchiveStoragePlugin
	Registry() *prometheus.Registry
}

func main() {
	localViper.ConfigureViperFromFlag(viper.GetViper())

	logger, err := log.NewLogger(viper.GetViper())
	if err != nil {
		stdLog.Fatal(err)
	}

	var ydbPlugin storagePluginWithRegistry
	ydbPlugin, err = ydb_storage.NewYdbStorage(viper.GetViper(), logger)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	go serveHttp(ydbPlugin.Registry(), logger)

	closer, err := initTracer()
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	defer func() {
		_ = closer.Close()
	}()

	logger.Warn("starting plugin")
	jaegerGrpc.Serve(&shared.PluginServices{
		Store:        ydbPlugin,
		ArchiveStore: ydbPlugin,
	})
	logger.Warn("stopped")
}

func serveHttp(gatherer prometheus.Gatherer, logger hclog.Logger) {
	mux := http.NewServeMux()
	logger.Warn("serving metrics", "addr", viper.GetString("plugin_http_listen_address"))
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("/ping", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})

	if viper.GetBool("ENABLE_PPROF") {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	err := http.ListenAndServe(viper.GetString("plugin_http_listen_address"), mux)
	if err != nil {
		logger.Error("failed to start http listener", "err", err)
		os.Exit(1)
	}
}

func initTracer() (_ io.Closer, err error) {
	cfg, err := jaegerCfg.FromEnv()
	if err != nil {
		return nil, fmt.Errorf("initTracer: %w", err)
	}

	closer, err := cfg.InitGlobalTracer("jaeger-ydb-query")
	if err != nil {
		return nil, fmt.Errorf("initTracer: %w", err)
	}
	return closer, nil
}
