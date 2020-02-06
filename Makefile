DOCKER_NAMESPACE ?= registry.vertis.yandex.net
DOCKER_TAG ?= dev

none:
	$(error no target specified)

image-collector:
	docker build -t $(DOCKER_NAMESPACE)/jaeger-ydb-collector:$(DOCKER_TAG) --target collector .

image-query:
	docker build -t $(DOCKER_NAMESPACE)/jaeger-ydb-query:$(DOCKER_TAG) --target query .

image-watcher:
	docker build -t $(DOCKER_NAMESPACE)/jaeger-ydb-watcher:$(DOCKER_TAG) --target watcher .

images: image-watcher image-collector image-query

push-images:
	docker push $(DOCKER_NAMESPACE)/jaeger-ydb-collector:$(DOCKER_TAG)
	docker push $(DOCKER_NAMESPACE)/jaeger-ydb-query:$(DOCKER_TAG)
	docker push $(DOCKER_NAMESPACE)/jaeger-ydb-watcher:$(DOCKER_TAG)

generate:
	go generate ./...

PROTOC := "protoc"
PROTO_INCLUDES := \
	-I storage/spanstore/dbmodel/proto \
	-I vendor/github.com/gogo/googleapis \
	-I vendor/github.com/gogo/protobuf
PROTO_GOGO_MAPPINGS := $(shell echo \
		Mgoogle/protobuf/descriptor.proto=github.com/gogo/protobuf/types, \
		Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types, \
		Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types, \
		Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types, \
		Mgoogle/api/annotations.proto=github.com/gogo/googleapis/google/api, \
		Mmodel.proto=github.com/jaegertracing/jaeger/model \
	| sed 's/ //g')

proto:
	$(PROTOC) \
		$(PROTO_INCLUDES) \
		--gofast_out=plugins=grpc,$(PROTO_GOGO_MAPPINGS):storage/spanstore/dbmodel \
		storage/spanstore/dbmodel/proto/spandata.proto

.PHONY: image-collector image-query images
