## What's new in v1.3.0

### Added

- **Response headers filtering** — when `connect.reporting.include.response.headers=true`, you can now keep only the headers you need instead of reporting all of them.

Two new config keys:

| Key | Type | Default | Description |
|---|---|---|---|
| `connect.reporting.response.headers.filter.names` | list | `""` | Exact-name allowlist, case-insensitive. Empty = no filter. |
| `connect.reporting.response.headers.filter.regex` | string | `""` | Regex allowlist, case-insensitive. Empty = no filter. |

Filter logic is **exact OR regex**. Both filters empty = all headers reported (fully backward-compatible).

### Example — keep only tracing headers

```json
{
  "connect.reporting.include.response.headers": "true",
  "connect.reporting.response.headers.filter.regex": "x-.*"
}
```

Kafka header `response_headers` will contain only:

```json
{
  "x-request-id": ["580e4cfd-6e20-411e-bdca-de561d100d8c"],
  "x-correlation-id": ["PLX-OD-3740c09f-fb66-43d4-a52b-8a47acfeab87"],
  "x-api-version": ["2026-06-10"]
}
```

### Connector config example

If you enable response-header filtering in a deployment, add for example:

```json
"connect.reporting.response.headers.filter.names": "x-request-id,x-correlation-id,traceparent",
"connect.reporting.response.headers.filter.regex": "x-.*"
```

### Tests

13 tests — 0 failure — BUILD SUCCESS

---

Full changelog: https://github.com/kcmhub/http-kafka-connector-sink/blob/master/CHANGELOG.md
