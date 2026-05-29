package fr.etech.kafka.connect.http.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth2 {@code client_credentials} grant — token fetched once and cached until
 * {@code expires_in} seconds before expiry. Thread-safe: a single token is shared
 * by all callers of the same instance.
 *
 * <p>Token request body is URL-encoded:
 * {@code client_id=...&client_secret=...&grant_type=client_credentials[&scope=...]}.
 */
public final class OAuth2ClientCredentialsAuthenticator implements Authenticator {

  private static final Logger LOG = LoggerFactory.getLogger(OAuth2ClientCredentialsAuthenticator.class);
  private static final ObjectMapper JSON = new ObjectMapper();
  /** Refresh this many seconds before expiry to avoid clock-skew races. */
  private static final long REFRESH_SAFETY_WINDOW_S = 30L;

  private final String tokenUrl;
  private final String clientId;
  private final String clientSecret;
  private final String scope;
  private final String tokenProperty;
  private final Map<String, String> tokenRequestHeaders;
  private final HttpClient http;

  private final ReentrantLock lock = new ReentrantLock();
  private volatile String cachedHeader;
  private volatile long expiresAtEpochMs;

  public OAuth2ClientCredentialsAuthenticator(String tokenUrl, String clientId, String clientSecret,
                                              String scope, String tokenProperty,
                                              String headersCsv, Duration connectTimeout) {
    this.tokenUrl = tokenUrl;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.scope = scope;
    this.tokenProperty = (tokenProperty == null || tokenProperty.isEmpty()) ? "access_token" : tokenProperty;
    this.tokenRequestHeaders = parseHeadersCsv(headersCsv);
    this.http = HttpClient.newBuilder().connectTimeout(connectTimeout).build();
  }

  @Override
  public String authorizationHeader() {
    if (cachedHeader != null && System.currentTimeMillis() < expiresAtEpochMs) {
      return cachedHeader;
    }
    lock.lock();
    try {
      if (cachedHeader == null || System.currentTimeMillis() >= expiresAtEpochMs) {
        refresh();
      }
      return cachedHeader;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void invalidate() {
    lock.lock();
    try {
      cachedHeader = null;
      expiresAtEpochMs = 0L;
    } finally {
      lock.unlock();
    }
  }

  private void refresh() {
    String body = "client_id=" + enc(clientId)
        + "&client_secret=" + enc(clientSecret)
        + "&grant_type=client_credentials"
        + (scope == null || scope.isEmpty() ? "" : "&scope=" + enc(scope));

    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(tokenUrl))
        .timeout(Duration.ofSeconds(15))
        .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
    tokenRequestHeaders.forEach(b::header);

    try {
      HttpResponse<String> resp = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() / 100 != 2) {
        throw new IllegalStateException("OAuth2 token endpoint " + tokenUrl
            + " returned HTTP " + resp.statusCode() + ": " + resp.body());
      }
      JsonNode json = JSON.readTree(resp.body());
      JsonNode tok = json.get(tokenProperty);
      if (tok == null || tok.asText().isEmpty()) {
        throw new IllegalStateException("OAuth2 response missing `" + tokenProperty + "`: " + resp.body());
      }
      long expiresIn = json.path("expires_in").asLong(3600L); // default 1h
      this.cachedHeader = "Bearer " + tok.asText();
      this.expiresAtEpochMs = System.currentTimeMillis() + Math.max(0L, (expiresIn - REFRESH_SAFETY_WINDOW_S) * 1_000L);
      LOG.info("OAuth2 token refreshed, valid for ~{}s", expiresIn);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to fetch OAuth2 token from " + tokenUrl, e);
    }
  }

  private static String enc(String s) { return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8); }

  private static Map<String, String> parseHeadersCsv(String csv) {
    Map<String, String> out = new LinkedHashMap<>();
    if (csv == null || csv.isBlank()) return out;
    for (String entry : csv.split(",")) {
      int i = entry.indexOf(':');
      if (i <= 0) continue;
      out.put(entry.substring(0, i).trim(), entry.substring(i + 1).trim());
    }
    return out;
  }
}

