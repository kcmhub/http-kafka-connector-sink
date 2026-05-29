# Architecture

## Component diagram

```
                                    ┌──────────────────────────────────────────────┐
                                    │           Kafka Connect worker JVM           │
                                    │                                              │
   Kafka source topic               │   ┌────────────────────────────────────┐     │
   (records consumed by             │   │        EtechHttpSinkTask           │     │
    the Connect runtime)            │   │                                    │     │
        │                           │   │  put(records)                      │     │
        ▼                           │   │    └─► for each record:            │     │
   ┌─────────┐    SinkRecord        │   │         render templates           │     │
   │ runtime │ ─────────────────────┼──►│         attach Authorization       │     │
   └─────────┘                      │   │         send via HttpClient        │     │
        ▲                           │   │         classify status & retry    │     │
        │  commit offsets / DLQ     │   │         publish report (optional)  │     │
        │                           │   │                                    │     │
        │   ┌───────────────────────┼───┤                                    │     │
        │   │  errors.tolerance=all │   └─────────┬──────────────────────────┘     │
        │   │  + DLQ topic          │             │                                │
        │   └───────────────────────┘             │                                │
        └─────────────────────────────────────────┘                                │
                                                                                  │
                            ┌─────────────────────────────────────────────────────┘
                            │
                            ▼
     ┌──────────────────────────────────────────────┐         ┌─────────────────┐
     │           Authenticator (cached)             │         │   HTTP target   │
     │   ┌────────────┐  ┌────────────┐  ┌────────┐ │ ──────► │  (any REST API) │
     │   │   NoAuth   │  │  BasicAuth │  │ OAuth2 │ │ <────── │                 │
     │   └────────────┘  └────────────┘  └────────┘ │         └─────────────────┘
     └──────────────────────────────────────────────┘
                            │
                            ▼ (success | error)
                  ┌───────────────────────────┐
                  │  KafkaResponseReporter    │  fire-and-forget producer
                  │  topic + http headers     │
                  └───────────────────────────┘
```

## Per-record lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                Task#put(record)                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │ render URL, body, hdrs  │
                          └────────────┬────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │ authenticator.header()  │
                          └────────────┬────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │       send request      │  ◄────────────────────┐
                          └────────────┬────────────┘                       │
                                       │                                    │
                ┌──────────────────────┼────────────────────────┐           │
                ▼ 2xx                  ▼ retryable (5xx, 408,   │ 401       │
       success reporter ─► return      │ 429, IOException)      │ invalidate│
                                       │                        ▼ token     │
                                       ▼                  ┌───────────┐     │
                          ┌─────────────────────────┐     │ backoff & │ ────┘
                          │ retries < max?          │     │  retry    │
                          └─────┬───────────────────┘     └───────────┘
                                │ no
                                ▼
                       error reporter
                                │
                                ▼
                  ┌────────────────────────────────────┐
                  │  errors.tolerance=all → Connect    │
                  │  routes to DLQ                     │
                  │  errors.tolerance=none → task fails│
                  └────────────────────────────────────┘
```

## Retry semantics

| Trigger | Action |
|---|---|
| HTTP status ∈ `retries.on.status.codes` | Retry up to `max.retries` with exponential backoff. |
| HTTP status `401` | Invalidate cached OAuth2 token, then apply the normal retry rule (401 must also be in the status list to be retried). |
| `java.io.IOException` (DNS, RST, timeout, …) | Always retry up to `max.retries`. |
| Any other non-2xx status | No retry → error reporter + DLQ. |

Backoff formula: `min(initial.delay.ms * 2^attempt, max.delay.ms)`.

## Error threshold

A monotonic per-task counter is incremented on each **terminal** failure
(`response not 2xx after all retries`, or transport error after all retries).
When the counter exceeds `connect.http.error.threshold` the task throws a
`ConnectException`, letting the runtime restart it per its standard policy.
Below the threshold, failures are propagated to the runtime — which means a
configured `errors.deadletterqueue.topic.name` will receive them.

## Threading & lifecycle

| Phase | What happens |
|---|---|
| `start(props)` | Build immutable config, instantiate `HttpClient`, `Authenticator`, retry policy and (optionally) the two reporters. |
| `put(records)` | Each record is processed sequentially on the task thread (one in-flight HTTP request at a time per task). Parallelism comes from `tasks.max`. |
| `flush(offsets)` | No-op — every send is synchronous, so when `put` returns the corresponding records are durably delivered (success-reported, error-reported, or DLQ'd by Connect). |
| `stop()` | Closes both reporter producers and the authenticator. The HTTP client follows the JVM's normal GC. |

## Performance notes

- Single in-flight request per task — favours **correctness over throughput**.
- Scale horizontally with `tasks.max` and matching topic partition count.
- The connection pool defaults to the JDK `HttpClient` defaults (HTTP/2 when
  the server allows it). One TLS session is reused for the lifetime of the
  task.
- Reporter producers use `linger.ms=5` and `acks=1` — they are intentionally
  cheap to keep the hot path fast and **must not** block on a slow broker.

