# Changelog

All notable changes to this project will be documented in this file. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.0] — 2026-06-14

### Fixed
- **Task no longer fails on a single non-retryable HTTP response.** Previously,
  after the response was published to the error reporting topic, the task
  re-threw `ConnectException` from `put()`, which crashed the worker and
  blocked all subsequent records (Kafka Connect's `errors.tolerance=all`
  does **not** cover exceptions from `SinkTask.put()` — only converter and
  SMT failures). The new default is to swallow the exception once the
  record has been reported, keeping the task healthy.

### Added
- New configuration `connect.http.behavior.on.error` controlling the
  behavior of terminal record failures (non-retryable status codes or
  exhausted retries):
  - `report_and_continue` (**new default**): publish the failure to the
    error reporting topic and keep processing. Recommended for production
    when `connect.reporting.error.config.enabled=true`.
  - `fail`: re-throw and let the task fail. Preserves the previous
    behavior; use this for strict pipelines where any HTTP failure must
    halt processing.
- `connect.http.error.threshold` is still honored as a circuit breaker
  in both modes — the task fails loud once the cumulative failure count
  exceeds the threshold, regardless of `behavior.on.error`.

### Changed
- **Default behavior change** (`behavior.on.error` defaults to
  `report_and_continue`): operators relying on the previous fail-fast
  semantics must explicitly set `connect.http.behavior.on.error=fail`.

## [1.3.0] — 2026-06-10

### Added
- Response header allowlist filtering with exact names and/or regex via
  `connect.reporting.response.headers.filter.names` and
  `connect.reporting.response.headers.filter.regex`.
- Case-insensitive response header filtering (`exact OR regex`) applied to both
  Kafka header `response_headers` and envelope field `response.response_headers`.
- Dedicated tests for exact/regex filtering and invalid regex validation.

## [1.2.0] — 2026-06-10

### Added
- Optional HTTP response headers reporting via
  `connect.reporting.include.response.headers` (default `false`).
- New Kafka report header `response_headers` containing JSON
  (`header-name -> array of values`) when enabled.
- Envelope support for response headers in `response.response_headers`.
- Redaction support for sensitive HTTP response header names using
  `connect.reporting.redaction.fields` when redaction is enabled.
- Dedicated unit tests covering default-disabled behavior, header JSON rendering
  and redaction.

## [1.1.1] — 2026-06-10

### Fixed
- Response reporting now renders schemaful Kafka Connect `Struct` values as
  JSON instead of `Struct{...}` text.
- For ACK compatibility, `Struct` or `Map` values containing a `payload` field
  report that field as `input_payload`, so downstream consumers receive the
  original JSON request body.

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

