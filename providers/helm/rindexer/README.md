
# Rindexer Helm Chart

## Description

A Helm chart for deploying the `rindexer` application. This chart supports integration with PostgreSQL and allows the injection of environment variables via an external Kubernetes Secret.

## Prerequisites

- Kubernetes 1.16+
- Helm 3.0+
- PersistentVolume provisioner support in the underlying infrastructure

## Installing the Chart

To install the chart with the release name `my-release`:

```bash
helm install my-release ./rindexer
```

The command deploys the `rindexer` application on the Kubernetes cluster using the default configuration.

## Uninstalling the Chart

To uninstall/delete the `my-release` deployment:

```bash
helm uninstall my-release
```

This command removes all the Kubernetes components associated with the chart and deletes the release.

## Configuration

The following table lists the configurable parameters of the `rindexer` chart and their default values.

| Parameter                                | Description                                      | Default                       |
|------------------------------------------|--------------------------------------------------|-------------------------------|
| `replicaCount`                           | Number of replicas to deploy                     | `1`                           |
| `image.repository`                       | Image repository                                 | `ghcr.io/joshstevens19/rindexer` |
| `image.tag`                              | Image tag                                        | `latest`                      |
| `image.pullPolicy`                       | Image pull policy                                | `IfNotPresent`                |
| `service.type`                           | Kubernetes service type                          | `ClusterIP`                   |
| `service.port`                           | Kubernetes service port                          | `3001`                        |
| `ingress.enabled`                        | Enable Ingress resource                          | `false`                       |
| `ingress.annotations`                    | Ingress annotations                              | `{}`                          |
| `ingress.hosts`                          | Ingress accepted hostnames                       | `[]`
| `postgresql.enabled`                     | Enable/disable PostgreSQL integration            | `false`                       |
| `externalSecret`                         | Specify an external secret for additional environment variables | `""`                          |
| `command`                                | Custom command to run in the container           | `[]` (inherits default command) |

### Security Context Parameters

| Parameter                                | Description                                      | Default                       |
|------------------------------------------|--------------------------------------------------|-------------------------------|
| `securityContext.enabled`                | Enable/disable the security context              | `true`                        |
| `securityContext.runAsUser`              | User ID to run the container as                  | `1000`                        |
| `securityContext.runAsGroup`             | Group ID to run the container as                 | `3000`                        |
| `securityContext.fsGroup`                | File system group ID                             | `2000`                        |
| `securityContext.allowPrivilegeEscalation` | Allow privilege escalation in the container     | `false`                       |
| `securityContext.runAsNonRoot`           | Ensure the container runs as a non-root user     | `true`                        |
| `securityContext.dropCapabilities`       | Capabilities to drop in the container            | `['ALL']`                     |

## PostgreSQL Integration

If PostgreSQL is enabled, the chart will configure the application to connect to it using the specified credentials.

### Configuring External PostgreSQL

To configure the Helm chart to use an external PostgreSQL database, you need to:

1. Set `postgresql.enabled` to `true` in the `values.yaml` file.
2. Create a Kubernetes Secret that contains the PostgreSQL credentials, including `POSTGRES_PASSWORD` and `DATABASE_URL`.
3. Specify the name of this secret in the `externalSecret` parameter in `values.yaml`.

Example configuration in `values.yaml`:

```yaml
postgresql:
  enabled: true

externalSecret: "my-postgres-secret"
```

Your Kubernetes Secret (`my-postgres-secret`) should include the following keys:

- `POSTGRES_PASSWORD`: The password for the PostgreSQL user.
- `DATABASE_URL`: The connection URL for the PostgreSQL database.

Example command to create the Kubernetes Secret:

```bash
kubectl create secret generic my-postgres-secret   --from-literal=POSTGRES_PASSWORD=mysecretpassword   --from-literal=DATABASE_URL=postgres://user:mysecretpassword@host:5432/dbname
```

This setup ensures that the application can connect to an external PostgreSQL database using the provided credentials.

### Using helm values

If you prefer to manage PostgreSQL credentials directly within the Helm chart, you can extend the `values.yaml` to include fields like `username`, `password`, `host`, `port`, and `database`.

Example configuration in `values.yaml`:

```yaml
postgresql:
  enabled: true
  auth:
    username: "rindexer"
    password: "yourpassword"
    database: "rindexerdb"
    host: "localhost"
    port: "5432"

externalSecret: ""
```

The corresponding `secret.yaml`:

```yaml
{{- if .Values.postgresql.enabled }}
{{- if not .Values.externalSecret }}
apiVersion: v1
kind: Secret
metadata:
  name: {{ include "rindexer.fullname" . }}-postgresql
  labels:
    {{- include "rindexer.labels" . | nindent 4 }}
type: Opaque
data:
  postgresql-password: {{ .Values.postgresql.auth.password | b64enc | quote }}
  database-url: {{ printf "postgres://%s:%s@%s:%s/%s" .Values.postgresql.auth.username .Values.postgresql.auth.password .Values.postgresql.auth.host .Values.postgresql.auth.port .Values.postgresql.auth.database | b64enc | quote }}
{{- end }}
{{- end }}
```

## Ingress Configurations

### NGINX Ingress Controller

```yaml
ingress:
  enabled: true
  annotations:
    kubernetes.io/ingress.class: "nginx"
    nginx.ingress.kubernetes.io/rewrite-target: /
  hosts:
    - host: rindexer.local
      paths:
        - path: /
          pathType: Prefix
```

### AWS ALB Ingress Controller

```yaml
ingress:
  enabled: true
  annotations:
    kubernetes.io/ingress.class: "alb"
    alb.ingress.kubernetes.io/scheme: internet-facing
    alb.ingress.kubernetes.io/ssl-redirect: "true"
    alb.ingress.kubernetes.io/certificate-arn: "arn:aws:acm:us-east-1:123456789012:certificate/your-certificate-arn"
    alb.ingress.kubernetes.io/listen-ports: '[{"HTTPS": 443}]'
    alb.ingress.kubernetes.io/target-type: "ip"
    alb.ingress.kubernetes.io/backend-protocol: "HTTP"
  hosts:
    - host: rindexer.example.com
      paths:
        - path: /
          pathType: Prefix
```
