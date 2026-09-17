{{- define "connect.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "connect.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 40 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "connect.name" .) | trunc 40 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- define "connect.selectorLabels" -}}
app.kubernetes.io/name: {{ include "connect.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "connect.labels" -}}
{{ include "connect.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
{{- end -}}
{{- define "connect.serviceAccount" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "connect.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
{{- define "connect.serverSecret" -}}
{{- if .Values.tls.plugin.existingServerSecret -}}
{{- .Values.tls.plugin.existingServerSecret -}}
{{- else if .Values.tls.certManager.enabled -}}
{{- include "connect.fullname" . -}}-server-tls
{{- else -}}
{{- fail "tls.plugin.existingServerSecret is required when cert-manager is disabled" -}}
{{- end -}}
{{- end -}}
{{- define "connect.clientSecret" -}}
{{- if .Values.tls.plugin.existingClientSecret -}}
{{- .Values.tls.plugin.existingClientSecret -}}
{{- else if .Values.tls.certManager.enabled -}}
{{- include "connect.fullname" . -}}-client-tls
{{- else -}}
{{- fail "tls.plugin.existingClientSecret is required when cert-manager is disabled" -}}
{{- end -}}
{{- end -}}
{{- define "connect.clientCASecret" -}}
{{- default (include "connect.clientSecret" .) .Values.tls.plugin.clientCASecret -}}
{{- end -}}
{{- define "connect.applicationSecret" -}}
{{- if .Values.tls.application.existingSecret -}}
{{- .Values.tls.application.existingSecret -}}
{{- else if .Values.tls.certManager.enabled -}}
{{- include "connect.fullname" . -}}-application-tls
{{- else -}}
{{- fail "tls.application.existingSecret is required when cert-manager is disabled" -}}
{{- end -}}
{{- end -}}
{{- define "connect.issuerRef" -}}
{{- if .Values.tls.certManager.createIssuer -}}
name: {{ include "connect.fullname" . }}-ca
kind: Issuer
group: cert-manager.io
{{- else -}}
name: {{ required "tls.certManager.issuerRef.name is required when createIssuer=false" .Values.tls.certManager.issuerRef.name }}
kind: {{ .Values.tls.certManager.issuerRef.kind }}
group: {{ .Values.tls.certManager.issuerRef.group }}
{{- end -}}
{{- end -}}
