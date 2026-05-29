# Contributing

Thanks for considering a contribution! This project is small, focused and
intends to stay that way.

## Build

```bash
./mvnw -q -DskipTests package
```

Artefacts land in `target/`:

- `etech-kafka-connect-http-<version>.jar`
- `etech-kafka-connect-http-<version>.zip` — Confluent-Hub-style component

## Project layout

```
etech-kafka-connect-http/
├── pom.xml
├── manifest.json                          # Confluent-Hub descriptor
├── README.md
├── CHANGELOG.md
├── CONTRIBUTING.md
├── LICENSE
├── docs/                                  # operator-facing documentation
│   ├── CONFIGURATION.md
│   ├── TEMPLATING.md
│   ├── SECURITY.md
│   ├── RECIPES.md
│   └── ARCHITECTURE.md
└── src/
    ├── assembly/plugin.xml                # produces the .zip
    └── main/java/fr/etech/kafka/connect/http/
        ├── EtechHttpSinkConnector.java
        ├── EtechHttpSinkTask.java         # hot path: render → auth → send → report
        ├── EtechHttpSinkConfig.java       # all config keys, types and defaults
        ├── auth/
        │   ├── Authenticator.java
        │   ├── NoAuthAuthenticator.java
        │   ├── BasicAuthAuthenticator.java
        │   └── OAuth2ClientCredentialsAuthenticator.java
        ├── template/RecordTemplateRenderer.java
        ├── retry/RetryPolicy.java
        └── reporting/KafkaResponseReporter.java
```

## Code style

- Java 11 — we rely on `java.net.http.HttpClient` from the standard library.
- 2-space indent, 110-column soft limit.
- All public types carry javadoc explaining intent (not signature).
- `final` everywhere it makes sense (classes, fields, parameters).
- **No external runtime dependencies** — anything pulled in must be
  `provided` and already shipped by `cp-kafka-connect`. This keeps the jar at
  ~30 KB and avoids classloader headaches.

## Pull requests

1. Open an issue first to discuss anything non-trivial.
2. Keep PRs focused on a single concern.
3. Update `docs/` and `CHANGELOG.md` in the same PR if the change is
   user-visible.
4. Add or update unit tests when feasible.

## Releasing

1. Update `<version>` in `pom.xml` and `manifest.json`.
2. Move the `## [Unreleased]` block in `CHANGELOG.md` under a new version + date.
3. Tag: `git tag -a v<x.y.z> -m "Release <x.y.z>"`.
4. Build & publish the `.zip` to your artefact repository.

## License

Apache 2.0. By contributing, you agree your contribution will be released under
the same license.

