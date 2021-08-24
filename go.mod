module github.com/yandex-cloud/jaeger-ydb-store

go 1.16

require (
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/gogo/protobuf v1.2.1
	github.com/golang/protobuf v1.5.2
	github.com/hashicorp/go-hclog v0.14.0
	github.com/hashicorp/golang-lru v0.5.3
	github.com/jaegertracing/jaeger v1.20.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/prometheus/client_golang v1.5.1
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.6.2
	github.com/stretchr/testify v1.7.0
	github.com/uber/jaeger-client-go v2.23.1+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible
	github.com/yandex-cloud/ydb-go-sdk/v2 v2.3.3
	go.uber.org/zap v1.16.0
	google.golang.org/grpc v1.39.0
)
