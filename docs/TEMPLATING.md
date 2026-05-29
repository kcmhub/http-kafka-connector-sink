# Templating

The connector renders `{{...}}` placeholders inside three configuration values:

| Where | Key |
|---|---|
| URL | `connect.http.endpoint` |
| Request body | `connect.http.request.content` |
| Request headers | `connect.http.request.headers` (both name and value) |

## Expressions

| Expression | Resolves to |
|---|---|
| `{{key}}` | Record key, stringified (`null` ⇒ empty string). |
| `{{value}}` | Record value, stringified. |
| `{{value.<dotted.path>}}` | Field lookup on a structured value. See below. |
| `{{key.<dotted.path>}}` | Same, applied to the key (rare). |
| `{{header.<name>}}` | The *last* header with the given key, stringified. Missing ⇒ empty string. |
| `{{topic}}` `{{partition}}` `{{offset}}` `{{timestamp}}` | Source record metadata. |

Unknown expressions and missing values render as the **empty string**. This is
deliberate: an optional header should not crash the task.

## Schema-ful payloads (Avro, JSON Schema)

The converter produces an `org.apache.kafka.connect.data.Struct`. Dotted paths
walk through nested structs:

```json
"connect.http.endpoint": "https://api.example.com/v1/orders/{{value.order.id}}",
"connect.http.request.content": "{{value.payload}}"
```

Worker config:

```properties
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry:8081
```

## Schemaless payloads (raw JSON)

The converter produces a `java.util.Map`. The very same dotted-path syntax
applies — there is **no connector change**:

```json
"connect.http.endpoint": "https://api.example.com/v1/users/{{value.user.id}}",
"connect.http.request.content": "{{value}}"
```

Worker config:

```properties
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
```

> When `{{value}}` is rendered against a `Map`, the result is its `toString()`
> representation (e.g. `{user={id=42, name=Alice}}`). To forward a JSON
> document *as JSON*, your producer should either (a) emit it as schemaless
> JSON consumed by `JsonConverter`, or (b) emit it as raw bytes through
> `ByteArrayConverter`. In both cases `{{value}}` renders the document
> verbatim.

## Raw scalar payloads

| Converter | `r.value()` Java type | `{{value}}` renders |
|---|---|---|
| `StringConverter` | `String` | The string verbatim. |
| `ByteArrayConverter` | `byte[]` | UTF-8 decoded. |
| `IntegerConverter` / `LongConverter` / … | the boxed number | `Number.toString()`. |

Dotted paths against scalars resolve to empty. That's intended — there's
nothing to walk into.

## Headers

`connect.http.request.headers` is a CSV of `Name:Value` pairs. Both sides are
templated.

```json
"connect.http.request.headers":
  "Content-Type:application/json,X-Trace-Id:{{header.X-Trace-Id}},X-Tenant:{{value.tenant}}"
```

If a header value templates to the empty string (e.g. the source header is
absent), that header is **silently skipped** — avoids sending stray `X-Foo:`
empty headers.

## Worked example — Avro Struct

Source value (Avro):

```json
{
  "orderNumber": "O-001",
  "payload": "{\"items\":[{\"sku\":\"X\",\"qty\":2}]}",
  "tenant": "EU"
}
```

Connector:

```json
"connect.http.endpoint": "https://api.example.com/v1/orders/{{value.orderNumber}}",
"connect.http.method":   "POST",
"connect.http.request.content": "{{value.payload}}",
"connect.http.request.headers": "X-Tenant:{{value.tenant}}"
```

Outgoing request:

```http
POST /v1/orders/O-001
X-Tenant: EU
Content-Type: application/json

{"items":[{"sku":"X","qty":2}]}
```

## Worked example — Schemaless JSON

Source value (`Map` produced by `JsonConverter` `schemas.enable=false`):

```json
{ "user": { "id": 42, "email": "alice@example.com" }, "action": "DELETE" }
```

Connector:

```json
"connect.http.method": "DELETE",
"connect.http.endpoint": "https://api.example.com/v1/users/{{value.user.id}}",
"connect.http.request.headers": "X-Action:{{value.action}}"
```

Outgoing request:

```http
DELETE /v1/users/42
X-Action: DELETE
```

