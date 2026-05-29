package fr.etech.kafka.connect.http;

import fr.etech.kafka.connect.http.auth.Authenticator;
import fr.etech.kafka.connect.http.auth.BasicAuthAuthenticator;
import fr.etech.kafka.connect.http.auth.NoAuthAuthenticator;
import fr.etech.kafka.connect.http.auth.OAuth2ClientCredentialsAuthenticator;
import fr.etech.kafka.connect.http.reporting.KafkaResponseReporter;
import fr.etech.kafka.connect.http.retry.RetryPolicy;
import fr.etech.kafka.connect.http.template.RecordTemplateRenderer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task that turns each {@link SinkRecord} into one HTTP call.
 *
 * <p>Flow per record:
 * <ol>
 *   <li>Render URL, body and headers from {@code {{...}}} templates.</li>
 *   <li>Inject {@code Authorization} via the configured {@link Authenticator}.</li>
 *   <li>Send the request through a shared {@link HttpClient}. Retry on configured
 *       status codes / network IOExceptions with exponential backoff.</li>
 *   <li>2xx → publish to the success reporter (if enabled). Non-2xx → publish to
 *       the error reporter and either (a) tolerate (when {@code errors.tolerance=all})
 *       or (b) throw, letting the Connect runtime route the record to its DLQ.</li>
 * </ol>
 */
public final class EtechHttpSinkTask extends SinkTask {

  private static final Logger LOG = LoggerFactory.getLogger(EtechHttpSinkTask.class);

  private EtechHttpSinkConfig cfg;
  private HttpClient http;
  private Authenticator auth;
  private RetryPolicy retry;
  private KafkaResponseReporter successReporter;   // nullable
  private KafkaResponseReporter errorReporter;     // nullable
  private final AtomicLong errorCount = new AtomicLong();
  private String connectorName;

  @Override public String version() { return "1.0.0"; }

  @Override public void start(Map<String, String> props) {
    this.cfg = new EtechHttpSinkConfig(props);
    this.connectorName = props.getOrDefault("name", "etech-http-sink");
    this.http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(cfg.connectTimeoutMs()))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
    this.auth = buildAuthenticator(cfg);
    this.retry = new RetryPolicy(cfg);
    if (cfg.successEnabled() && !cfg.successTopic().isEmpty()) {
      this.successReporter = new KafkaResponseReporter(cfg.successReporterProps(),
          cfg.successTopic(), connectorName + "-success-reporter");
    }
    if (cfg.errorEnabled() && !cfg.errorTopic().isEmpty()) {
      this.errorReporter = new KafkaResponseReporter(cfg.errorReporterProps(),
          cfg.errorTopic(), connectorName + "-error-reporter");
    }
    LOG.info("Started Etech HTTP sink `{}` -> {} {}", connectorName, cfg.method(), cfg.endpoint());
  }

  @Override public void put(Collection<SinkRecord> records) {
    for (SinkRecord r : records) {
      try {
        sendWithRetry(r);
      } catch (RetriableException re) {
        throw re;     // Connect will rewind and retry the batch
      } catch (Exception e) {
        handleTerminalFailure(r, e);
      }
    }
  }

  private void sendWithRetry(SinkRecord record) throws Exception {
    String url = RecordTemplateRenderer.render(cfg.endpoint(), record);
    String body = RecordTemplateRenderer.render(cfg.requestBodyTemplate(), record);
    Map<String, String> headers = RecordTemplateRenderer.renderHeadersCsv(cfg.requestHeadersTpl(), record);

    int attempt = 0;
    Exception lastException = null;
    while (true) {
      try {
        HttpResponse<String> resp = doSend(url, body, headers);
        int code = resp.statusCode();

        if (code / 100 == 2) {
          if (successReporter != null) {
            successReporter.publish(record, cfg.method(), url, code, resp.body());
          }
          LOG.debug("HTTP {} {} -> {}", cfg.method(), url, code);
          return;
        }
        if (code == 401) {
          auth.invalidate();    // force refresh before next attempt
        }
        if (retry.isRetryableStatus(code) && retry.decide(attempt) == RetryPolicy.Decision.RETRY) {
          long backoff = retry.backoffMs(attempt);
          LOG.warn("HTTP {} {} -> {} (attempt {}/{}), backing off {}ms",
              cfg.method(), url, code, attempt + 1, cfg.maxRetries(), backoff);
          sleepInterruptibly(backoff);
          attempt++;
          continue;
        }
        // Non-retryable, or retries exhausted.
        if (errorReporter != null) {
          errorReporter.publish(record, cfg.method(), url, code, resp.body());
        }
        throw new ConnectException("HTTP " + cfg.method() + " " + url
            + " failed with status " + code + ": " + truncate(resp.body(), 500));
      } catch (java.io.IOException ioe) {
        lastException = ioe;
        if (retry.decide(attempt) == RetryPolicy.Decision.RETRY) {
          long backoff = retry.backoffMs(attempt);
          LOG.warn("HTTP {} {} I/O error (attempt {}/{}): {} — backing off {}ms",
              cfg.method(), url, attempt + 1, cfg.maxRetries(), ioe.getMessage(), backoff);
          sleepInterruptibly(backoff);
          attempt++;
          continue;
        }
        throw new RetriableException("HTTP " + cfg.method() + " " + url
            + " failed after " + (attempt + 1) + " attempt(s)", ioe);
      }
    }
  }

  private HttpResponse<String> doSend(String url, String body, Map<String, String> headers) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
        .timeout(Duration.ofMillis(cfg.requestTimeoutMs()));

    String authz = auth.authorizationHeader();
    if (authz != null) b.header("Authorization", authz);
    headers.forEach(b::header);

    switch (cfg.method()) {
      case "GET":    b.GET(); break;
      case "DELETE": b.method("DELETE", body == null || body.isEmpty()
                                 ? HttpRequest.BodyPublishers.noBody()
                                 : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
                     break;
      case "POST":   b.POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : body, StandardCharsets.UTF_8)); break;
      case "PUT":    b.PUT(HttpRequest.BodyPublishers.ofString(body == null ? "" : body, StandardCharsets.UTF_8)); break;
      case "PATCH":  b.method("PATCH", HttpRequest.BodyPublishers.ofString(body == null ? "" : body, StandardCharsets.UTF_8)); break;
      default: throw new ConnectException("Unsupported HTTP method: " + cfg.method());
    }
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }

  private void handleTerminalFailure(SinkRecord record, Exception e) {
    long count = errorCount.incrementAndGet();
    LOG.error("Terminal failure on record {}-{}@{}: {}",
        record.topic(), record.kafkaPartition(), record.kafkaOffset(), e.getMessage());
    if (count > cfg.errorThreshold()) {
      throw new ConnectException("Error threshold (" + cfg.errorThreshold()
          + ") exceeded for " + connectorName, e);
    }
    // Below threshold: let the Connect runtime's tolerant error handling kick in
    // (errors.tolerance=all → DLQ via errors.deadletterqueue.topic.name).
    if (e instanceof ConnectException) throw (ConnectException) e;
    throw new ConnectException(e);
  }

  private static void sleepInterruptibly(long ms) {
    try { Thread.sleep(ms); }
    catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw new ConnectException(ie); }
  }

  private static String truncate(String s, int max) {
    if (s == null) return "";
    return s.length() <= max ? s : s.substring(0, max) + "…";
  }

  private static Authenticator buildAuthenticator(EtechHttpSinkConfig cfg) {
    switch (cfg.authType()) {
      case BASIC:  return new BasicAuthAuthenticator(cfg.basicUser(),
                       cfg.basicPassword() == null ? "" : cfg.basicPassword().value());
      case OAUTH2: return new OAuth2ClientCredentialsAuthenticator(
                       cfg.oauth2TokenUrl(), cfg.oauth2ClientId(),
                       cfg.oauth2ClientSecret() == null ? "" : cfg.oauth2ClientSecret().value(),
                       cfg.oauth2Scope(), cfg.oauth2TokenProperty(),
                       cfg.oauth2ClientHeaders(), Duration.ofMillis(cfg.connectTimeoutMs()));
      case NONE:
      default:     return new NoAuthAuthenticator();
    }
  }

  @Override public void stop() {
    if (successReporter != null) successReporter.close();
    if (errorReporter != null) errorReporter.close();
    if (auth != null) try { auth.close(); } catch (Exception ignore) { /* nop */ }
    LOG.info("Stopped Etech HTTP sink `{}`", connectorName);
  }
}

