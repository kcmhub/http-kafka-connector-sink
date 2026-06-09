package fr.etech.kafka.connect.http.reporting;

import fr.etech.kafka.connect.http.EtechHttpSinkConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
  private final boolean includeHttpMetadata;
  private final boolean redactionEnabled;
  private final List<String> redactionFields;
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
      boolean includeHttpMetadata,
      boolean redactionEnabled,
      List<String> redactionFields,
      int maxPayloadBytes,
      int maxResponseBytes) {
    this.valueMode = valueMode;
    this.includeInputMetadata = includeInputMetadata;
    this.includeInputKey = includeInputKey;
    this.includeInputPayload = includeInputPayload;
    this.includeRequestBody = includeRequestBody;
    this.includeRequestHeaders = includeRequestHeaders;
    this.includeResponseContent = includeResponseContent;
    this.includeHttpMetadata = includeHttpMetadata;
    this.redactionEnabled = redactionEnabled;
    this.redactionFields = Collections.unmodifiableList(new ArrayList<>(redactionFields));
    this.maxPayloadBytes = maxPayloadBytes;
    this.maxResponseBytes = maxResponseBytes;
  }

  public static ReportingOptions from(EtechHttpSinkConfig cfg) {
    return new ReportingOptions(
        ValueMode.valueOf(cfg.reportingValueMode().name()),
        cfg.reportInputMetadata(),
        cfg.reportInputKey(),
        cfg.reportInputPayload(),
        cfg.reportRequestBody(),
        cfg.reportRequestHeaders(),
        cfg.reportResponseContent(),
        cfg.reportHttpMetadata(),
        cfg.reportRedactionEnabled(),
        cfg.reportRedactionFields(),
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
        true,
        false,
        List.of("iban", "taxNumber", "accountNumber", "Authorization", "client_secret"),
        -1,
        -1);
  }

  public ValueMode valueMode() { return valueMode; }
  public boolean includeInputMetadata() { return includeInputMetadata; }
  public boolean includeInputKey() { return includeInputKey; }
  public boolean includeInputPayload() { return includeInputPayload; }
  public boolean includeRequestBody() { return includeRequestBody; }
  public boolean includeRequestHeaders() { return includeRequestHeaders; }
  public boolean includeResponseContent() { return includeResponseContent; }
  public boolean includeHttpMetadata() { return includeHttpMetadata; }
  public boolean redactionEnabled() { return redactionEnabled; }
  public List<String> redactionFields() { return redactionFields; }
  public int maxPayloadBytes() { return maxPayloadBytes; }
  public int maxResponseBytes() { return maxResponseBytes; }
}
