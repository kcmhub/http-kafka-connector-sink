# Security

## Authentication modes

### None

```json
"connect.http.authentication.type": "none"
```

No `Authorization` header is sent. Useful for internal services behind mTLS or
network policy.

### HTTP Basic

```json
"connect.http.authentication.type": "basic",
"connect.http.authentication.basic.user":     "${file:/etc/secrets/api.properties:user}",
"connect.http.authentication.basic.password": "${file:/etc/secrets/api.properties:password}"
```

Username and password are concatenated as `user:password`, Base64-encoded and
sent as `Authorization: Basic …`. The password field is typed `PASSWORD`, so
it is masked in logs and the Connect REST API.

### OAuth2 client_credentials

```json
"connect.http.authentication.type": "oauth2",
"connect.http.authentication.oauth2.token.url":     "${file:/etc/secrets/api.properties:oauth2.endpoint}",
"connect.http.authentication.oauth2.client.id":     "${file:/etc/secrets/api.properties:client.id}",
"connect.http.authentication.oauth2.client.secret": "${file:/etc/secrets/api.properties:client.secret}",
"connect.http.authentication.oauth2.client.scope":  "any"
```

Lifecycle:

1. On first request, the connector POSTs
   `client_id=…&client_secret=…&grant_type=client_credentials[&scope=…]`
   to the token URL.
2. The response JSON is parsed; the bearer token is read from the property
   named by `connect.http.authentication.oauth2.token.property` (default
   `access_token`) and `expires_in` (seconds) is honoured.
3. The token is cached and reused until **30 s before** the advertised expiry.
4. On HTTP `401` the cache is invalidated; the very next attempt refreshes it.

Token requests honour `connect.http.connect.timeout.ms` for the TCP handshake
and impose a hard 15-second total timeout.

## Secret externalization

**Never hard-code secrets in connector JSON.** Three production-grade options:

### a) `FileConfigProvider` (recommended, works everywhere)

Worker config:

```properties
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

Mount your secrets file read-only into the worker container, then reference
each property via:

```
${file:/absolute/path/to/secrets.properties:propertyName}
```

### b) `DirectoryConfigProvider`

Same idea but each property lives in its own file (one file per secret) —
matches the layout produced by Kubernetes secret mounts.

```properties
config.providers=dir
config.providers.dir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider
```

```
${dir:/etc/secrets:client.secret}
```

### c) Cloud secret managers

Bring your own `ConfigProvider` implementation (Azure Key Vault, AWS Secrets
Manager, HashiCorp Vault) and register it on the worker. The connector itself
needs **no change** — it sees only the resolved value.

## TLS

The connector uses the standard JDK `HttpClient`, which honours the JVM trust
store. To trust a private CA add it via:

```bash
keytool -import -alias my-ca -file ca.pem \
  -keystore $JAVA_HOME/lib/security/cacerts -storepass changeit -noprompt
```

Or set system properties on the worker:

```
KAFKA_OPTS=-Djavax.net.ssl.trustStore=/etc/ssl/custom-truststore.jks \
           -Djavax.net.ssl.trustStorePassword=…
```

## Logging

The connector uses SLF4J. The authentication code logs **the URL only** when
refreshing a token, never the body. Connector configs are logged by the Connect
runtime in INFO with `PASSWORD`-typed keys redacted to `[hidden]`.

## What the reporter publishes

By default, the reporter publishes the rendered HTTP request body as the report
value and the HTTP response body as the `response_content` header. If your API
returns sensitive material in error responses (PII, secrets, etc.) either
disable the error reporter, disable `connect.reporting.include.response.content`
or restrict ACLs on the reporter topic.

The connector can include the post-SMT input payload and rendered request body
in headers/envelopes. On sensitive topics, explicitly disable payload/request
capture unless it is needed for investigation:

```json
{
  "connect.reporting.include.input.payload": "false",
  "connect.reporting.include.request.body": "false"
}
```

If payload/request capture is enabled, enable redaction and set explicit fields:

```json
{
  "connect.reporting.redaction.enabled": "true",
  "connect.reporting.redaction.fields": "iban,taxNumber,accountNumber,Authorization,client_secret",
  "connect.reporting.max.payload.bytes": "20000",
  "connect.reporting.max.response.bytes": "20000"
}
```

Redaction is a safety net, not a replacement for topic ACLs. Treat ACK topics as
potentially sensitive when they contain response bodies, input payloads or
rendered request bodies.

