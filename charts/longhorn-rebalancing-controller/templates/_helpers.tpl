{{- define "longhorn-rebalancing-controller.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "longhorn-rebalancing-controller.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "longhorn-rebalancing-controller.labels" -}}
app: {{ include "longhorn-rebalancing-controller.fullname" . }}
env: production
category: storage
app.kubernetes.io/name: {{ include "longhorn-rebalancing-controller.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}

{{- define "longhorn-rebalancing-controller.serviceAccountName" -}}
{{ include "longhorn-rebalancing-controller.fullname" . }}
{{- end }}
