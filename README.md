# Etech Kafka Connect HTTP Sink

[![Build](https://img.shields.io/badge/build-maven-blue)]() [![Kafka Connect](https://img.shields.io/badge/kafka--connect-3.x-orange)]() [![License](https://img.shields.io/badge/license-Apache--2.0-green)]() [![Java](https://img.shields.io/badge/java-11%2B-red)]()

A small, dependency-free **HTTP sink connector for Apache Kafka Connect**.
Open-source (Apache 2.0). One ~30 KB jar, no runtime dependencies beyond what
Kafka Connect already ships, full coverage of REST verbs including `DELETE`,
and a Mustache-style templating language for the URL, body and headers.

```
┌──────────────┐       ┌────────────────────────┐       ┌────────────────┐
│ Kafka topic  │  ──►  │   EtechHttpSinkTask    │  ──►  │ HTTP endpoint  │
│ Avro / JSON  │       │ template + auth +      │       │ (any REST API) │
│ raw bytes    │       │ retry + reporting      │       │                │
└──────────────┘       └───────────┬────────────┘       └────────────────┘
                                   │
                                   ▼ (optional, fire-and-forget)
                         ┌─────────────────────┐
                         │   reporter topics   │  success / error
                         └─────────────────────┘
```

## Features

| | |
|---|---|
| **HTTP methods**     | `GET` · `POST` · `PUT` · `PATCH` · `DELETE` |
| **Payload shapes**   | Schema-ful (`Struct` from Avro / JsonSchema) · Schemaless (`Map` from `JsonConverter` `schemas.enable=false`) · Raw `String` / `byte[]` |
| **Templating**       | `{{key}}` · `{{value}}` · `{{value.<dotted.path>}}` · `{{header.<name>}}` · `{{topic}} / {{partition}} / {{offset}} / {{timestamp}}` |
| **Authentication**   | `none` · `basic` · `oauth2` (client_credentials, cached + auto-refresh on 401 / expiry) |
| **Retries**          | Exponential backoff on configurable status codes **and** transport `IOException` |
| **Response reporting** | 2xx → success topic · non-2xx → error topic, request-body ACK value by default, optional `response_headers` JSON, optional envelope, Kafka input metadata, request/response capture and redaction |
| **Dead-letter queue** | Delegated to the Connect runtime (`errors.deadletterqueue.*`) |
| **Secrets**          | Full `${file:…}` `ConfigProvider` support (works with `FileConfigProvider`, `DirectoryConfigProvider`, KV-vault providers) |
| **Footprint**        | ~30 KB jar · no extra runtime deps · loaded under standard `plugin.path` |

## Quick start

### Build

```bash
./mvnw -q -DskipTests package
```

Produces:

- `target/etech-kafka-connect-http-1.3.0.jar` — the connector
- `target/etech-kafka-connect-http-1.3.0.zip` — Confluent-Hub-style component
  with the layout
  ```
  etech-kafka-connect-http-1.3.0/
    lib/etech-kafka-connect-http-1.3.0.jar
    manifest.json
    README.md
  ```

### Install

| Target | Action |
|---|---|
| **Confluent Platform / cp-kafka-connect** | `unzip etech-kafka-connect-http-1.3.0.zip -d /usr/share/confluent-hub-components/` and restart the worker. |
| **Apache Kafka Connect** | Drop the jar under any directory listed in `plugin.path`. |
| **Docker compose** | Bind-mount the jar at `/opt/connectors/etech-kafka-connect-http/lib/etech-kafka-connect-http-1.3.0.jar` and add that directory to `CONNECT_PLUGIN_PATH`. |

### Register a connector

```bash
curl -X PUT http://localhost:8083/connectors/my-connector/config \
     -H "Content-Type: application/json" \
     --data @my-connector.json
```

A minimal connector:

```json
{
  "connector.class": "fr.etech.kafka.connect.http.EtechHttpSinkConnector",
  "tasks.max": "1",
  "topics": "my.topic",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",

  "connect.http.endpoint": "https://api.example.com/v1/things/{{key}}",
  "connect.http.method":   "POST",
  "connect.http.request.content": "{{value}}",
  "connect.http.request.headers": "Content-Type:application/json"
}
```

See [`docs/RECIPES.md`](docs/RECIPES.md) for more shapes (DELETE, OAuth2, Basic,
schemaless JSON, …).

### ACK-compatible reporting

By default, response reporting keeps the reported record value equal to the
rendered HTTP request body. The HTTP response body and status are carried in
headers, so downstream BPP-style ACK consumers can correlate the payload sent to
the endpoint with the endpoint acknowledgement.

```json
{
  "connect.reporting.value.mode": "request_body",
  "connect.reporting.include.input.metadata": "true",
  "connect.reporting.include.input.key": "true",
  "connect.reporting.include.input.payload": "true",
  "connect.reporting.include.response.content": "true"
}
```

Default ACK headers include `input_topic`, `input_partition`, `input_offset`,
`input_timestamp`, `input_key`, `input_payload`, `response_content` and
`response_status_code`. Optional HTTP metadata uses underscore names such as
`http_status_code`, `http_method` and `http_url`; the connector does not emit
old `http.*` / `input.*` dotted aliases.

To include HTTP response headers in reports, opt in explicitly (default `false`):

```json
{
  "connect.reporting.include.response.headers": "true",
  "connect.reporting.response.headers.filter.regex": "x-.*"
}
```

When enabled, the reporter adds a Kafka header named `response_headers` containing
JSON (`header-name -> array of values`). In `envelope` mode, the same object is
also available under `response.response_headers`.

You can keep only selected response headers with either an exact-name allowlist,
a regex, or both (`exact OR regex`, case-insensitive):

```json
{
  "connect.reporting.include.response.headers": "true",
  "connect.reporting.response.headers.filter.names": "x-request-id,x-correlation-id,traceparent",
  "connect.reporting.response.headers.filter.regex": "x-.*"
}
```

For incident analysis, use `envelope` mode and opt in to input/request capture
with redaction:

```json
{
  "connect.reporting.value.mode": "envelope",
  "connect.reporting.include.input.payload": "true",
  "connect.reporting.include.request.body": "true",
  "connect.reporting.include.response.content": "true",
  "connect.reporting.redaction.enabled": "true",
  "connect.reporting.redaction.fields": "iban,taxNumber,accountNumber,Authorization,client_secret"
}
```

### Single Message Transforms

The connector supports standard Kafka Connect SMTs. SMTs are executed by the
Kafka Connect runtime before `EtechHttpSinkTask.put(records)`, so templating and
reporting see the transformed record that is actually used for the HTTP call.

## Documentation

| | |
|---|---|
| [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) | Every configuration key, its type, default and meaning. |
| [`docs/TEMPLATING.md`](docs/TEMPLATING.md) | Placeholder syntax, schema-ful vs schemaless payloads, worked examples. |
| [`docs/SECURITY.md`](docs/SECURITY.md) | Authentication modes, secret externalization, TLS notes. |
| [`docs/RECIPES.md`](docs/RECIPES.md) | Copy/paste-ready connector JSONs. |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Component diagram, per-record lifecycle, retry semantics. |
| [`CHANGELOG.md`](CHANGELOG.md) | Release history. |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | How to build, test and contribute. |

## License

Apache License 2.0 — see [`LICENSE`](LICENSE).
Free, open-source. Pull requests welcome.

