package fr.etech.kafka.connect.http.reporting;

import fr.etech.kafka.connect.http.EtechHttpSinkConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public final class ReportingOptions {

  public enum ValueMode {
    REQUEST_BODY,
    RESPONSE_ONLY,
    ENVELOPE
  }

  private final ValueMode valueMode;
  private final boolean includeInputMetadata;
  private final boolean includeInputKey;
  private final boolean includeInputPayload;
  private final boolean includeRequestBody;
  private final boolean includeRequestHeaders;
  private final boolean includeResponseContent;
  private final boolean includeResponseHeaders;
  private final boolean includeHttpMetadata;
  private final boolean redactionEnabled;
  private final List<String> redactionFields;
  private final List<String> responseHeadersFilterNames;
  private final Pattern responseHeadersFilterRegex;
  private final int maxPayloadBytes;
  private final int maxResponseBytes;

  public ReportingOptions(
      ValueMode valueMode,
      boolean includeInputMetadata,
      boolean includeInputKey,
      boolean includeInputPayload,
      boolean includeRequestBody,
      boolean includeRequestHeaders,
      boolean includeResponseContent,
      boolean includeResponseHeaders,
      boolean includeHttpMetadata,
      boolean redactionEnabled,
      List<String> redactionFields,
      List<String> responseHeadersFilterNames,
      Pattern responseHeadersFilterRegex,
      int maxPayloadBytes,
      int maxResponseBytes) {
    this.valueMode = valueMode;
    this.includeInputMetadata = includeInputMetadata;
    this.includeInputKey = includeInputKey;
    this.includeInputPayload = includeInputPayload;
    this.includeRequestBody = includeRequestBody;
    this.includeRequestHeaders = includeRequestHeaders;
    this.includeResponseContent = includeResponseContent;
    this.includeResponseHeaders = includeResponseHeaders;
    this.includeHttpMetadata = includeHttpMetadata;
    this.redactionEnabled = redactionEnabled;
    this.redactionFields = Collections.unmodifiableList(new ArrayList<>(redactionFields));
    this.responseHeadersFilterNames = Collections.unmodifiableList(normalizeHeaderNames(responseHeadersFilterNames));
    this.responseHeadersFilterRegex = responseHeadersFilterRegex;
    this.maxPayloadBytes = maxPayloadBytes;
    this.maxResponseBytes = maxResponseBytes;
  }

  public static ReportingOptions from(EtechHttpSinkConfig cfg) {
    Pattern headerFilterRegex = compileHeaderFilterRegex(cfg.reportResponseHeadersFilterRegex());
    return new ReportingOptions(
        ValueMode.valueOf(cfg.reportingValueMode().name()),
        cfg.reportInputMetadata(),
        cfg.reportInputKey(),
        cfg.reportInputPayload(),
        cfg.reportRequestBody(),
        cfg.reportRequestHeaders(),
        cfg.reportResponseContent(),
        cfg.reportResponseHeaders(),
        cfg.reportHttpMetadata(),
        cfg.reportRedactionEnabled(),
        cfg.reportRedactionFields(),
        cfg.reportResponseHeadersFilterNames(),
        headerFilterRegex,
        cfg.reportMaxPayloadBytes(),
        cfg.reportMaxResponseBytes());
  }

  public static ReportingOptions defaults() {
    return new ReportingOptions(
        ValueMode.REQUEST_BODY,
        true,
        true,
        true,
        false,
        false,
        true,
        false,
        true,
        false,
        List.of("iban", "taxNumber", "accountNumber", "Authorization", "client_secret"),
        List.of(),
        null,
        -1,
        -1);
  }

  private static List<String> normalizeHeaderNames(List<String> names) {
    List<String> normalized = new ArrayList<>();
    if (names == null) return normalized;
    for (String name : names) {
      if (name == null) continue;
      String trimmed = name.trim();
      if (!trimmed.isEmpty()) normalized.add(trimmed.toLowerCase(Locale.ROOT));
    }
    return normalized;
  }

  private static Pattern compileHeaderFilterRegex(String regex) {
    if (regex == null || regex.trim().isEmpty()) return null;
    try {
      return Pattern.compile(regex.trim(), Pattern.CASE_INSENSITIVE);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException("Invalid response header filter regex: " + regex, e);
    }
  }

  public ValueMode valueMode() { return valueMode; }
  public boolean includeInputMetadata() { return includeInputMetadata; }
  public boolean includeInputKey() { return includeInputKey; }
  public boolean includeInputPayload() { return includeInputPayload; }
  public boolean includeRequestBody() { return includeRequestBody; }
  public boolean includeRequestHeaders() { return includeRequestHeaders; }
  public boolean includeResponseContent() { return includeResponseContent; }
  public boolean includeResponseHeaders() { return includeResponseHeaders; }
  public boolean includeHttpMetadata() { return includeHttpMetadata; }
  public boolean redactionEnabled() { return redactionEnabled; }
  public List<String> redactionFields() { return redactionFields; }
  public List<String> responseHeadersFilterNames() { return responseHeadersFilterNames; }
  public Pattern responseHeadersFilterRegex() { return responseHeadersFilterRegex; }
  public boolean hasResponseHeadersFilter() {
    return !responseHeadersFilterNames.isEmpty() || responseHeadersFilterRegex != null;
  }
  public int maxPayloadBytes() { return maxPayloadBytes; }
  public int maxResponseBytes() { return maxResponseBytes; }
}
