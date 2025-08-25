{{- define "rindexer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rindexer.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" $name .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "rindexer.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "rindexer.labels" -}}
app.kubernetes.io/name: {{ include "rindexer.name" . }}
helm.sh/chart: {{ include "rindexer.chart" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.podLabels }}
{{ toYaml . | indent 4 }}
{{- end }}
{{- end -}}

{{- define "rindexer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rindexer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
