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

The default value mode is compatible with the existing HTTP ACK topic shape:
the reported record value is the rendered HTTP request body, while the HTTP
response body and status are exposed in headers. This lets downstream consumers
correlate the exact payload sent to the HTTP endpoint with the endpoint
acknowledgement.

| Key (success / error) | Type | Default | Description |
|---|---|---|---|
| `connect.reporting.success.config.enabled` / `…error.config.enabled` | bool | `false` | Master switch per reporter. |
| `…bootstrap.servers` | string | `""` | Reporter producer bootstrap servers. |
| `…topic` | string | `""` | Destination topic. |
| `…security.protocol` | string | `PLAINTEXT` | Standard Kafka client property. |
| `…sasl.mechanism` | string | `""` | Standard Kafka client property. |
| `…sasl.jaas.config` | password | `""` | Standard Kafka client property. |

### Report content

| Key | Type | Default | Description |
|---|---|---|---|
| `connect.reporting.value.mode` | string | `request_body` | `request_body` keeps the value as the rendered HTTP request body. `response_only` uses the raw response body. `envelope` emits a JSON document containing selected input/request/response sections. |
| `connect.reporting.include.input.metadata` | bool | `true` | Include source topic, partition, offset and timestamp in headers and envelope. |
| `connect.reporting.include.input.key` | bool | `true` | Include the post-SMT source key as `input_key`. The header is present with a null value when the input key is null. |
| `connect.reporting.include.input.payload` | bool | `true` | Include the post-SMT source value as `input_payload`. |
| `connect.reporting.include.transformed.input.payload` | bool | `true` | Documents/enforces that reported input payload is the transformed record seen by the task. |
| `connect.reporting.include.request.body` | bool | `false` | Include the rendered HTTP request body. |
| `connect.reporting.include.request.headers` | bool | `false` | Include rendered HTTP request headers in `envelope` mode. |
| `connect.reporting.include.response.content` | bool | `true` | Include the response body as `response_content` and in `envelope` mode. |
| `connect.reporting.include.response.headers` | bool | `false` | Include response headers as JSON in Kafka header `response_headers` and in `envelope` mode. |
| `connect.reporting.include.http.metadata` | bool | `true` | Include `http_status_code`, `http_method` and `http_url` headers. |
| `connect.reporting.redaction.enabled` | bool | `false` | Mask configured sensitive fields in reported text. |
| `connect.reporting.redaction.fields` | list | `iban,taxNumber,accountNumber,Authorization,client_secret` | Field/header names to mask when redaction is enabled. |
| `connect.reporting.max.payload.bytes` | int | `-1` | Max reported input/request payload characters; `-1` disables truncation. |
| `connect.reporting.max.response.bytes` | int | `-1` | Max reported response characters; `-1` disables truncation. |

Each reported record carries the following headers:

| Header | Value |
|---|---|
| `input_topic` | the source topic |
| `input_partition` | the source partition |
| `input_offset` | the source offset |
| `input_timestamp` | the source timestamp, when available |
| `input_key` | the source key, present with null value when the source key is null |
| `input_payload` | the source value, after Kafka Connect SMTs |
| `response_content` | the HTTP response body |
| `response_headers` | the HTTP response headers as a JSON object (`name -> array of values`), when `include.response.headers=true` |
| `response_status_code` | the HTTP status code |
| `response_status` / `status_code` | compatibility aliases for the HTTP status code |
| `http_status_code` | e.g. `201`, when `include.http.metadata=true` |
| `http_method` | the HTTP method actually sent, when `include.http.metadata=true` |
| `http_url` | the fully-rendered URL, when `include.http.metadata=true` |
| `request_body` | optional, when `include.request.body=true` |

Key passthrough: the reported record uses the source record's key (UTF-8 bytes);
a null source key remains a null reported key.
In the default `request_body` mode, the value is the rendered HTTP request body
(raw bytes). In `response_only` mode, the value is the HTTP response body (raw
bytes).

When `connect.reporting.value.mode=envelope`, the value is a JSON document:

```json
{
  "input": {
    "input_topic": "orders",
    "input_partition": 2,
    "input_offset": 53,
    "input_timestamp": 1710000000000,
    "input_key": "key",
    "input_payload": "{...}"
  },
  "request": {
    "method": "POST",
    "url": "https://api.example.com/v1/orders",
    "body": "{...}"
  },
  "response": {
    "response_status": 200,
    "response_status_code": 200,
    "status_code": 200,
    "response_headers": {
      "Content-Type": ["application/json"]
    },
    "response_content": "{...}"
  }
}
```

## Single Message Transforms

Standard Kafka Connect SMTs are supported. SMTs run in the Connect runtime
before the sink task receives records, so the connector templates and reporting
operate on the post-SMT record.

