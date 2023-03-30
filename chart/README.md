# YDB storage plugin for Jaeger

## Introduction
This chart adds all components required to run [Jaeger](https://github.com/jaegertracing/jaeger) using [Yandex.Database](https://cloud.yandex.ru/services/ydb) backend storage. Chart will deploy jaeger-agent as a DaemonSet and deploy the jaeger-collector, jaeger-query and schema-watcher components as Deployments.

## Configuration
### Storage
You should create dedicated Yandex Database as described in [YDB documentation](https://cloud.yandex.ru/docs/ydb/quickstart/create-db) before installing this chart. After creating database you will get YDB Endpoint and database name needed to create Jaeger store:
```qoute
endpoint: grpcs://lb.etns9ff54e1j4d7.ydb.mdb.yandexcloud.net:2135/?database=/ru-central1/afg8slkos03mal53s/etns9ff54e1j4d7
``` 

### Parameters
This is necessary parameters, all other options described in [YDB storage plugin for Jaeger documentation](https://github.com/ydb-platform/jaeger-ydb-store#environment-variables) and can be overriden using `ydb.env.{ENV_VARIABLE}`

Name | Type | Default | Description
--- | --- | --- | ---
`ydb.endpoint` | `string` | | db endpoint host:port to connect to
`ydb.useMetaAuth` | `bool` | `false` | use metadata to authorize requests [documentation](https://cloud.yandex.com/docs/compute/operations/vm-connect/auth-inside-vm#auth-inside-vm)
`ydb.saId` | `string` | | service account id for Yandex.Cloud authorization [documentation on service accounts](https://cloud.yandex.com/docs/iam/concepts/users/service-accounts)
`ydb.saKeyId` | `string` | | service account key id for Yandex.Cloud authorization
`saPrivateKey` | `string` | | service account private key for Yandex.Cloud authorization	
`ydb.database` | `string` | | database name
`ydb.folder` | `string` | `jaeger` | folder for tables to store data in


## Installing the Chart
Add the Jaeger Tracing Helm repository:
```bash
$ helm repo add jaeger-ydb-store https://charts.ydb.tech/
$ helm repo update
```
To install a release named jaeger:
```bash
$ helm install jaeger jaeger-ydb-store/jaeger-ydb-store \
  --set ydb.endpoint={YDB_ENDPOINT}}:2135 \
  --set ydb.database={YDB_DATABASE}} \
  --set ydb.folder={YDB_FOLDER}} \
  --set ydb.useMetaAuth=true
```

or using Service Account Key:
```bash
$ helm install jaeger jaeger-ydb-store/jaeger-ydb-store \
  --set ydb.endpoint={YDB_ENDPOINT}}:2135 \
  --set ydb.database={YDB_DATABASE}} \
  --set ydb.folder={YDB_FOLDER}} \
  --set ydb.saId={SA_ID}} \
  --set ydb.saKeyId={SA_KEY_ID}} \
  --set ydb.saPrivateKey="$(cat ~/jaeger-over-ydb-sa-key.pem)"
```

**Use either Metadata or Service Account Key authorization**

Alternatively, a YAML file that specifies the values for the parameters can be provided while installing the chart. For example,
```bash
$ helm install jaeger jaeger-ydb-store/jaeger-ydb-store --values values.yaml
```
You can get default values.yaml from [chart repository](https://github.com/ydb-platform/jaeger-ydb-store/chart/values.yaml).

By default, the chart deploys the following:

 - Jaeger Agent DaemonSet
 - Jaeger Collector Deployment
 - Jaeger Query (UI) Deployment
 - Schema-Watcher Deployment (creates new tables for spans/indexes and removes old ones)

 You can use Jaeger Agent as a sidecar for you service and not to deploy as DaemonSet by setting `--set agent.enable=false`




