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
| **Response reporting** | 2xx → success topic · non-2xx → error topic, HTTP metadata exposed as record headers |
| **Dead-letter queue** | Delegated to the Connect runtime (`errors.deadletterqueue.*`) |
| **Secrets**          | Full `${file:…}` `ConfigProvider` support (works with `FileConfigProvider`, `DirectoryConfigProvider`, KV-vault providers) |
| **Footprint**        | ~30 KB jar · no extra runtime deps · loaded under standard `plugin.path` |

## Quick start

### Build

```bash
./mvnw -q -DskipTests package
```

Produces:

- `target/etech-kafka-connect-http-1.0.0.jar` — the connector
- `target/etech-kafka-connect-http-1.0.0.zip` — Confluent-Hub-style component
  with the layout
  ```
  etech-kafka-connect-http-1.0.0/
    lib/etech-kafka-connect-http-1.0.0.jar
    manifest.json
    README.md
  ```

### Install

| Target | Action |
|---|---|
| **Confluent Platform / cp-kafka-connect** | `unzip etech-kafka-connect-http-1.0.0.zip -d /usr/share/confluent-hub-components/` and restart the worker. |
| **Apache Kafka Connect** | Drop the jar under any directory listed in `plugin.path`. |
| **Docker compose** | Bind-mount the jar at `/opt/connectors/etech-kafka-connect-http/lib/etech-kafka-connect-http-1.0.0.jar` and add that directory to `CONNECT_PLUGIN_PATH`. |

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

