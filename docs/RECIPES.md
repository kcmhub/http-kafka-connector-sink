# Recipes

Copy-paste-ready connector JSONs for the most common shapes. All examples
assume a `FileConfigProvider` named `file` is registered on the worker.

---

## 1. Schema-ful POST (Avro Struct)

Forward each record as the body of a `POST`, with the URL templated from a
field of the record's value.

```json
{
  "connector.class": "fr.etech.kafka.connect.http.EtechHttpSinkConnector",
  "tasks.max": "1",
  "topics": "orders.created",

  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter.use.latest.version": "true",

  "connect.http.endpoint": "https://api.example.com/v1/orders",
  "connect.http.method":   "POST",
  "connect.http.request.content": "{{value.payload}}",
  "connect.http.request.headers": "Content-Type:application/json,Idempotency-Key:{{header.Idempotency-Key}}",

  "connect.http.authentication.type": "oauth2",
  "connect.http.authentication.oauth2.token.url":     "${file:/etc/secrets/api.properties:oauth2.endpoint}",
  "connect.http.authentication.oauth2.client.id":     "${file:/etc/secrets/api.properties:client.id}",
  "connect.http.authentication.oauth2.client.secret": "${file:/etc/secrets/api.properties:client.secret}",
  "connect.http.authentication.oauth2.client.scope":  "any"
}
```

---

## 2. Schemaless POST (raw JSON map)

Use when the producer emits JSON without a schema and the worker is configured
with `JsonConverter` + `schemas.enable=false`. **No connector change** — the
dotted-path syntax just walks the `Map` instead of a `Struct`.

```json
{
  "connector.class": "fr.etech.kafka.connect.http.EtechHttpSinkConnector",
  "tasks.max": "1",
  "topics": "user.events",

  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",

  "connect.http.endpoint": "https://api.example.com/v1/users/{{value.user.id}}/events",
  "connect.http.method":   "POST",
  "connect.http.request.content": "{{value}}",
  "connect.http.request.headers": "Content-Type:application/json,X-Tenant:{{value.tenant}}",

  "connect.http.authentication.type": "none"
}
```

---

## 3. DELETE with templated path

```json
{
  "connector.class": "fr.etech.kafka.connect.http.EtechHttpSinkConnector",
  "tasks.max": "1",
  "topics": "orders.tombstones",

  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter.use.latest.version": "true",

  "connect.http.endpoint": "https://api.example.com/v1/orders/{{key}}",
  "connect.http.method":   "DELETE",
  "connect.http.request.headers": "X-Trace-Id:{{header.X-Trace-Id}},Idempotency-Key:{{header.Idempotency-Key}}",

  "connect.http.authentication.type": "oauth2",
  "connect.http.authentication.oauth2.token.url":     "${file:/etc/secrets/api.properties:oauth2.endpoint}",
  "connect.http.authentication.oauth2.client.id":     "${file:/etc/secrets/api.properties:client.id}",
  "connect.http.authentication.oauth2.client.secret": "${file:/etc/secrets/api.properties:client.secret}",
  "connect.http.authentication.oauth2.client.scope":  "any",

  "connect.reporting.success.config.enabled": "true",
  "connect.reporting.success.config.bootstrap.servers": "kafka:29092",
  "connect.reporting.success.config.topic": "orders.tombstones.ack.success",

  "connect.reporting.error.config.enabled": "true",
  "connect.reporting.error.config.bootstrap.servers": "kafka:29092",
  "connect.reporting.error.config.topic": "orders.tombstones.ack.error",

  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "orders.tombstones.dlq",
  "errors.deadletterqueue.topic.replication.factor": "1"
}
```

---

## 4. PUT with header-driven path + custom API version

```json
{
  "connector.class": "fr.etech.kafka.connect.http.EtechHttpSinkConnector",
  "tasks.max": "1",
  "topics": "accounts.updates",

  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081",

  "connect.http.endpoint": "https://api.example.com/v1/accounts/{{header.X-Account-Number}}",
  "connect.http.method":   "PUT",
  "connect.http.request.content": "{{value.payload}}",
  "connect.http.request.headers": "Content-Type:application/json,X-Trace-Id:{{header.X-Trace-Id}},X-API-Version:2025-08-12"
}
```

---

## 5. Basic auth (legacy on-prem REST)

```json
{
  "connector.class": "fr.etech.kafka.connect.http.EtechHttpSinkConnector",
  "tasks.max": "1",
  "topics": "legacy.events",

  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",

  "connect.http.endpoint": "https://legacy.internal/api/v3/events",
  "connect.http.method":   "POST",
  "connect.http.request.content": "{{value}}",
  "connect.http.request.headers": "Content-Type:application/json",

  "connect.http.authentication.type": "basic",
  "connect.http.authentication.basic.user":     "${file:/etc/secrets/legacy.properties:user}",
  "connect.http.authentication.basic.password": "${file:/etc/secrets/legacy.properties:password}",

  "connect.http.retry.mode": "exponential",
  "connect.http.retries.max.retries": "5",
  "connect.http.retries.on.status.codes": "408,429,500,502,503,504",
  "connect.http.retries.initial.delay.ms": "1000",
  "connect.http.retries.max.delay.ms": "60000"
}
```

---

## 6. Fire-and-forget GET (rare; useful for webhooks / pings)

```json
{
  "connector.class": "fr.etech.kafka.connect.http.EtechHttpSinkConnector",
  "tasks.max": "1",
  "topics": "ping.requests",

  "value.converter": "org.apache.kafka.connect.storage.StringConverter",

  "connect.http.endpoint": "https://hooks.example.com/ping?id={{key}}",
  "connect.http.method":   "GET",
  "connect.http.authentication.type": "none"
}
```

