{{/*
Expand the name of the chart.
*/}}
{{- define "jaeger-ydb-store.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "jaeger-ydb-store.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "jaeger-ydb-store.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "jaeger-ydb-store.labels" -}}
helm.sh/chart: {{ include "jaeger-ydb-store.chart" . }}
{{ include "jaeger-ydb-store.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "jaeger-ydb-store.selectorLabels" -}}
app.kubernetes.io/name: {{ include "jaeger-ydb-store.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Watcher selector labels
*/}}
{{- define "jaeger-ydb-store.watcherSelectorLabels" -}}
jaeger-ydb-store.component: watcher
{{- end }}

{{/*
Collector selector labels
*/}}
{{- define "jaeger-ydb-store.collectorSelectorLabels" -}}
jaeger-ydb-store.component: collector
{{- end }}

{{/*
Query selector labels
*/}}
{{- define "jaeger-ydb-store.querySelectorLabels" -}}
jaeger-ydb-store.component: query
{{- end }}

{{/*
Agent selector labels
*/}}
{{- define "jaeger-ydb-store.agentSelectorLabels" -}}
jaeger-ydb-store.component: agent
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "jaeger-ydb-store.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "jaeger-ydb-store.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
