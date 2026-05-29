# Configuration reference

All keys live in the `connect.*` namespace.

> **Tip.** Anywhere a value can hold a secret (`oauth2.client.secret`,
> `basic.password`, reporter `sasl.jaas.config`) you should externalize it
> through a `ConfigProvider` — see [`SECURITY.md`](SECURITY.md).

## Request

| Key | Type | Default | Description |
|---|---|---|---|
| `connect.http.endpoint` | string | **required** | Target URL. Templated. |
| `connect.http.method` | string | `POST` | One of `GET`, `POST`, `PUT`, `PATCH`, `DELETE`. |
| `connect.http.request.content` | string | `""` | Request body template. Ignored for `GET`; usually empty for `DELETE`. |
| `connect.http.request.headers` | string | `""` | Comma-separated `Name:Value` pairs. Both name and value are templated. |
| `connect.http.connect.timeout.ms` | long | `10000` | TCP connect timeout. |
| `connect.http.request.timeout.ms` | long | `30000` | Per-attempt total timeout. |

## Retries

| Key | Type | Default | Description |
|---|---|---|---|
| `connect.http.retry.mode` | string | `exponential` | `none` disables retries. |
| `connect.http.retries.max.retries` | int | `3` | Max retries per record (after the first attempt). |
| `connect.http.retries.on.status.codes` | list | `408,429,500,502,503,504` | Status codes that trigger a retry. |
| `connect.http.retries.initial.delay.ms` | long | `500` | First backoff. |
| `connect.http.retries.max.delay.ms` | long | `30000` | Backoff cap. |

Backoff formula: `min(initialDelay * 2^attempt, maxDelay)`.

## Error handling

| Key | Type | Default | Description |
|---|---|---|---|
| `connect.http.error.threshold` | int | `1000` | Failed sends above which the task **fails** (Connect will restart it per its own policy). |
| `errors.tolerance` | string | inherited | Standard Connect key. Set to `all` to send records to a DLQ instead of failing. |
| `errors.deadletterqueue.topic.name` | string | inherited | Standard Connect DLQ topic. |
| `errors.deadletterqueue.topic.replication.factor` | int | inherited | Replication factor of an auto-created DLQ topic. |
| `errors.deadletterqueue.context.headers.enable` | bool | inherited | Adds `__connect.errors.*` headers describing the cause. |

## Authentication

| Key | Type | Default | Description |
|---|---|---|---|
| `connect.http.authentication.type` | string | `none` | `none` · `basic` · `oauth2`. |
| `connect.http.authentication.basic.user` | string | `""` | Used when `type=basic`. |
| `connect.http.authentication.basic.password` | password | `""` | Used when `type=basic`. |
| `connect.http.authentication.oauth2.token.url` | string | `""` | OAuth2 token endpoint. |
| `connect.http.authentication.oauth2.client.id` | string | `""` | Client id. |
| `connect.http.authentication.oauth2.client.secret` | password | `""` | Client secret. |
| `connect.http.authentication.oauth2.client.scope` | string | `""` | Optional `scope`. |
| `connect.http.authentication.oauth2.token.property` | string | `access_token` | JSON property holding the bearer token in the OAuth2 response. |
| `connect.http.authentication.oauth2.client.headers` | string | `Content-Type:application/x-www-form-urlencoded` | Headers attached to the token request. |

OAuth2 specifics:

- Grant type is **client_credentials**.
- The bearer token is cached and refreshed when within 30 s of `expires_in`.
- On HTTP `401` the cache is invalidated; the next attempt fetches a fresh token.

## Response reporting

Both reporters can be enabled independently. They are **fire-and-forget**: any
send failure is logged at `WARN` but never propagates to the task.

| Key (success / error) | Type | Default | Description |
|---|---|---|---|
| `connect.reporting.success.config.enabled` / `…error.config.enabled` | bool | `false` | Master switch per reporter. |
| `…bootstrap.servers` | string | `""` | Reporter producer bootstrap servers. |
| `…topic` | string | `""` | Destination topic. |
| `…security.protocol` | string | `PLAINTEXT` | Standard Kafka client property. |
| `…sasl.mechanism` | string | `""` | Standard Kafka client property. |
| `…sasl.jaas.config` | password | `""` | Standard Kafka client property. |

Each reported record carries the following headers:

| Header | Value |
|---|---|
| `http.status.code` | e.g. `201` |
| `http.method` | the HTTP method actually sent |
| `http.url` | the fully-rendered URL |
| `input.topic` | the source topic |
| `input.partition` | the source partition |
| `input.offset` | the source offset |

Key passthrough: the reported record uses the source record's key (UTF-8 bytes).
Value: the HTTP response body (raw bytes).

