package main

import (
	"context"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"

	"github.com/hashicorp/go-hclog"
	jaegerGrpc "github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ydbPlugin, err := plugin.NewYdbStorage(ctx, viper.GetViper(), jaegerLogger)
	if err != nil {
		jaegerLogger.Error(err.Error())
		os.Exit(1)
	}
	defer ydbPlugin.Close()

	go serveHttp(ydbPlugin.Registry(), jaegerLogger)

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
