ARG jaeger_version=1.48.0
ARG golang_version=1.20
ARG alpine_version=3.10

FROM jaegertracing/jaeger-collector:${jaeger_version} as base-collector

FROM jaegertracing/jaeger-query:${jaeger_version} as base-query

FROM golang:${golang_version} as builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY . .
RUN CGO_ENABLED=0 go build -ldflags='-w -s' -o /ydb-plugin .
RUN CGO_ENABLED=0 go build -ldflags='-w -s' -o /ydb-schema ./cmd/schema

FROM alpine:${alpine_version} AS watcher
ENV YDB_CA_FILE="/ydb-ca.pem"
RUN apk add --no-cache ca-certificates && \
    wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O /ydb-ca.pem
COPY  --from=builder /ydb-schema /
ENTRYPOINT ["/ydb-schema"]

FROM alpine:${alpine_version} AS shared
ENV SPAN_STORAGE_TYPE="grpc-plugin"
ENV GRPC_STORAGE_PLUGIN_BINARY="/ydb-plugin"
ENV YDB_CA_FILE="/ydb-ca.pem"
RUN apk add --no-cache ca-certificates && \
    wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O /ydb-ca.pem
COPY --from=builder /ydb-plugin /

FROM shared AS collector
COPY --from=base-collector /go/bin/collector-linux /jaeger-collector
EXPOSE 9411
EXPOSE 14250
EXPOSE 14268
EXPOSE 14269
EXPOSE 4317
EXPOSE 4318
ENTRYPOINT ["/jaeger-collector"]

FROM shared AS query
COPY --from=base-query /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base-query /go/bin/query-linux /jaeger-query
EXPOSE 16685
EXPOSE 16686
EXPOSE 16687
ENTRYPOINT ["/jaeger-query"]
