package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"

	hclog "github.com/hashicorp/go-hclog"
	jaegerGrpc "github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	jaegerCfg "github.com/uber/jaeger-client-go/config"

	localViper "github.com/ydb-platform/jaeger-ydb-store/internal/viper"
	"github.com/ydb-platform/jaeger-ydb-store/plugin"
)

func init() {
	viper.SetDefault("plugin_http_listen_address", ":15000")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.AutomaticEnv()
}

func newJaegerLogger() hclog.Logger {
	pluginLogger := hclog.New(&hclog.LoggerOptions{
		Name:       "ydb-store-plugin",
		JSONFormat: true,
	})

	return pluginLogger
}

func main() {
	localViper.ConfigureViperFromFlag(viper.GetViper())

	jaegerLogger := newJaegerLogger()

	ydbPlugin, err := plugin.NewYdbStorage(viper.GetViper(), jaegerLogger)
	if err != nil {
		jaegerLogger.Error(err.Error())
		os.Exit(1)
	}
	go serveHttp(ydbPlugin.Registry(), jaegerLogger)

	closer, err := initTracer()
	if err != nil {
		jaegerLogger.Error(err.Error())
		os.Exit(1)
	}
	defer func() {
		_ = closer.Close()
	}()

	jaegerLogger.Warn("starting plugin")
	jaegerGrpc.Serve(&shared.PluginServices{
		Store:        ydbPlugin,
		ArchiveStore: ydbPlugin,
	})
	jaegerLogger.Warn("stopped")
}

func serveHttp(gatherer prometheus.Gatherer, jaegerLogger hclog.Logger) {
	mux := http.NewServeMux()
	jaegerLogger.Warn("serving metrics", "addr", viper.GetString("plugin_http_listen_address"))
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
		jaegerLogger.Error("failed to start http listener", "err", err)
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
