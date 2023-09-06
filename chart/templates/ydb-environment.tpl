{{- define "jaeger-ydb-store.ydb.env" -}}
{{- range $key, $value :=.Values.ydb.env }}
- name: {{ $key }}
  value: {{ $value | quote }}
{{- end }}
{{- if .Values.ydb.endpoint }}
- name: YDB_ADDRESS
  value: {{ .Values.ydb.endpoint | quote }}
{{- end }}
{{- if .Values.ydb.database }}
- name: YDB_PATH
  value: {{ .Values.ydb.database | quote }}
{{- end }}
{{- if .Values.ydb.anonymous }}
- name: YDB_ANONYMOUS
  value: {{ .Values.ydb.anonymous | quote }}
{{- end }}
{{- if .Values.ydb.token }}
- name: YDB_TOKEN
  value: {{ .Values.ydb.token | quote }}
{{- end }}
{{- if .Values.ydb.useMetaAuth }}
- name: YDB_SA_META_AUTH
  value: {{ .Values.ydb.useMetaAuth | quote }}
{{- end }}
{{- if .Values.ydb.saKeyJson }}
- name: YDB_SA_KEY_JSON
  value: {{ .Values.ydb.saKeyJson | quote }}
{{- end }}
{{- if .Values.ydb.saId }}
- name: YDB_SA_ID
  value: {{ .Values.ydb.saId | quote }}
{{- end }}
{{- if .Values.ydb.saKeyId }}
- name: YDB_SA_KEY_ID
  value: {{ .Values.ydb.saKeyId | quote }}
{{- end }}
{{- if .Values.ydb.saPrivateKey }}
- name: YDB_SA_PRIVATE_KEY_FILE
  value: /opt/secrets/ydb-sa-key.pem
{{- end }}
{{- if .Values.ydb.secureConnection }}
- name: YDB_SECURE_CONNECTION
  value: {{ .Values.ydb.secureConnection | quote }}
{{- end }}
- name: YDB_FOLDER
  value: {{ default "jaeger" .Values.ydb.folder | quote }}
{{- end -}}
