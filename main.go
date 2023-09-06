package main

import (
	"context"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"

	"github.com/hashicorp/go-hclog"
	hcplugin "github.com/hashicorp/go-plugin"
	"github.com/jaegertracing/jaeger/pkg/jtracer"
	jaegerGrpc "github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

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

	tracer, err := jtracer.New("jaeger-ydb-store")
	if err != nil {
		jaegerLogger.Error(err.Error())
		os.Exit(1)
	}
	defer func() {
		_ = tracer.Close(context.Background())
	}()

	jaegerLogger.Warn("starting plugin")

	service := &shared.PluginServices{
		Store:        ydbPlugin,
		ArchiveStore: ydbPlugin,
	}

	jaegerGrpc.ServeWithGRPCServer(service, func(options []grpc.ServerOption) *grpc.Server {
		return hcplugin.DefaultGRPCServer([]grpc.ServerOption{
			grpc.UnaryInterceptor(otelgrpc.UnaryServerInterceptor(otelgrpc.WithTracerProvider(tracer.OTEL))),
			grpc.StreamInterceptor(otelgrpc.StreamServerInterceptor(otelgrpc.WithTracerProvider(tracer.OTEL))),
		})
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
