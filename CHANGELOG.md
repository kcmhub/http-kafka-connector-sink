# Changelog

All notable changes to this project will be documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] — 2026-06-09

### Added
- Configurable response reporting with `request_body`, `response_only` and
  `envelope` value modes.
- ACK-compatible default reporting: the report value remains the rendered HTTP
  request body, with the HTTP response body/status carried in headers.
- Optional report headers/fields for `input_topic`, `input_partition`,
  `input_offset`, `input_timestamp`, `input_key`, `input_payload`,
  `response_status`, `status_code` and `response_content`.
- Optional HTTP metadata headers using underscore names:
  `http_status_code`, `http_method` and `http_url`.
- Optional request body capture in reports.
- Redaction and max-size controls for sensitive input/request/response content.
- Unit tests covering ACK value compatibility, envelope reporting, redaction and
  post-SMT input payload reporting.

### Documentation
- Documented standard Kafka Connect SMT support and recipes for `ExtractField`,
  `ReplaceField`, `HeaderFrom`, `HoistField` and `RegexRouter`.
- Documented offset behavior: the connector leaves offset commits to the Kafka
  Connect runtime and does not override `preCommit(...)`.

## [1.0.0] — 2024-05-29

### Added
- First public release of the Etech HTTP sink connector for Apache Kafka Connect.
- Supports all REST verbs: `GET`, `POST`, `PUT`, `PATCH`, `DELETE`.
- Schema-ful (`Struct`) **and** schemaless (`Map`, raw `String` / `byte[]`)
  payload handling with dotted-path field lookup.
- OAuth2 `client_credentials` authenticator with token caching and
  `401`-triggered refresh.
- HTTP Basic and No-Auth authenticators.
- Exponential-backoff retry policy on configurable status codes and transport
  `IOException`.
- Fire-and-forget success / error response reporting to Kafka topics, with HTTP
  metadata pushed as record headers.
- Native integration with the Kafka Connect runtime DLQ
  (`errors.deadletterqueue.topic.name`).
- `${file:…}` / `${dir:…}` `ConfigProvider` support for secret externalization.

### Documentation
- `README.md`, `docs/CONFIGURATION.md`, `docs/TEMPLATING.md`,
  `docs/SECURITY.md`, `docs/RECIPES.md`, `docs/ARCHITECTURE.md`,
  `CONTRIBUTING.md`, `LICENSE`.

