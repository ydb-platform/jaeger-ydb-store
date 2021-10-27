module github.com/yandex-cloud/jaeger-ydb-store

go 1.16

require (
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/hashicorp/go-hclog v0.16.2
	github.com/hashicorp/golang-lru v0.5.4
	github.com/jaegertracing/jaeger v1.25.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/prometheus/client_golang v1.11.0
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.7.0
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible
	github.com/ydb-platform/ydb-go-sdk/v3 v3.2.0-alpha
	github.com/ydb-platform/ydb-go-yc v0.1.1-0.20211027133733-7473895f506b
	go.uber.org/zap v1.18.1
	google.golang.org/grpc v1.39.0
)
