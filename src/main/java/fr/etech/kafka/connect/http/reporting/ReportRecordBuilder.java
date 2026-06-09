package fr.etech.kafka.connect.http.reporting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.sink.SinkRecord;

public final class ReportRecordBuilder {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final ReportingOptions options;

  public ReportRecordBuilder(ReportingOptions options) {
    this.options = options;
  }

  public ProducerRecord<byte[], byte[]> build(
      String reportTopic,
      SinkRecord original,
      String method,
      String url,
      String requestBody,
      Map<String, String> requestHeaders,
      int status,
      String responseBody) {
    String sanitizedRequestBody = sanitizePayload(requestBody, options.maxPayloadBytes());
    String sanitizedResponseBody = sanitizePayload(responseBody, options.maxResponseBytes());
    boolean inputKeyIsNull = original.key() == null;
    String sanitizedInputKey = inputKeyIsNull
        ? null
        : sanitizePayload(stringify(original.key()), options.maxPayloadBytes());
    String sanitizedInputPayload = sanitizePayload(stringify(original.value()), options.maxPayloadBytes());

    byte[] value = reportedValue(original, method, url, sanitizedRequestBody,
        sanitizeHeaders(requestHeaders), status, sanitizedResponseBody,
        sanitizedInputKey, sanitizedInputPayload);
    ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(
        reportTopic,
        null,
        toNullableBytes(original.key()),
        value);

    addHttpHeaders(rec, method, url, status);
    if (options.includeInputMetadata()) {
      addInputMetadataHeaders(rec, original);
    }
    if (options.includeInputKey()) {
      addHeader(rec, "input_key", sanitizedInputKey);
    }
    if (options.includeInputPayload()) {
      addHeader(rec, "input_payload", sanitizedInputPayload);
    }
    if (options.includeRequestBody()) {
      addHeader(rec, "request_body", sanitizedRequestBody);
    }
    if (options.includeResponseContent()) {
      addHeader(rec, "response_content", sanitizedResponseBody);
    }
    return rec;
  }

  private byte[] reportedValue(
      SinkRecord original,
      String method,
      String url,
      String requestBody,
      Map<String, String> requestHeaders,
      int status,
      String responseBody,
      String inputKey,
      String inputPayload) {
    if (options.valueMode() == ReportingOptions.ValueMode.REQUEST_BODY) {
      return toBytes(requestBody);
    }
    if (options.valueMode() == ReportingOptions.ValueMode.RESPONSE_ONLY) {
      return toBytes(options.includeResponseContent() ? responseBody : "");
    }

    Map<String, Object> envelope = new LinkedHashMap<>();
    if (options.includeInputMetadata() || options.includeInputKey() || options.includeInputPayload()) {
      Map<String, Object> input = new LinkedHashMap<>();
      if (options.includeInputMetadata()) {
        input.put("input_topic", original.topic());
        input.put("input_partition", original.kafkaPartition());
        input.put("input_offset", original.kafkaOffset());
        input.put("input_timestamp", original.timestamp());
      }
      if (options.includeInputKey()) input.put("input_key", inputKey);
      if (options.includeInputPayload()) input.put("input_payload", inputPayload);
      envelope.put("input", input);
    }

    Map<String, Object> request = new LinkedHashMap<>();
    request.put("method", method);
    request.put("url", url);
    if (options.includeRequestBody()) request.put("body", requestBody);
    if (options.includeRequestHeaders()) request.put("headers", requestHeaders);
    envelope.put("request", request);

    Map<String, Object> response = new LinkedHashMap<>();
    response.put("response_status", status);
    response.put("response_status_code", status);
    response.put("status_code", status);
    if (options.includeResponseContent()) response.put("response_content", responseBody);
    envelope.put("response", response);

    try {
      return MAPPER.writeValueAsBytes(envelope);
    } catch (JsonProcessingException e) {
      return toBytes("{\"error\":\"failed to render report envelope\"}");
    }
  }

  private void addHttpHeaders(ProducerRecord<byte[], byte[]> rec, String method, String url, int status) {
    addHeader(rec, "response_status_code", Integer.toString(status));
    addHeader(rec, "response_status", Integer.toString(status));
    addHeader(rec, "status_code", Integer.toString(status));
    if (options.includeHttpMetadata()) {
      addHeader(rec, "http_status_code", Integer.toString(status));
      addHeader(rec, "http_method", method);
      addHeader(rec, "http_url", url);
    }
  }

  private void addInputMetadataHeaders(ProducerRecord<byte[], byte[]> rec, SinkRecord original) {
    addHeader(rec, "input_topic", original.topic());
    addHeader(rec, "input_partition", Integer.toString(original.kafkaPartition()));
    addHeader(rec, "input_offset", Long.toString(original.kafkaOffset()));
    addHeader(rec, "input_timestamp", original.timestamp() == null ? "" : original.timestamp().toString());
  }

  private Map<String, String> sanitizeHeaders(Map<String, String> headers) {
    Map<String, String> sanitized = new LinkedHashMap<>();
    if (headers == null) return sanitized;
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      String key = entry.getKey();
      String value = sensitiveField(key) ? "***" : sanitizePayload(entry.getValue(), options.maxPayloadBytes());
      sanitized.put(key, value);
    }
    return sanitized;
  }

  private String sanitizePayload(String input, int maxChars) {
    if (input == null) return "";
    String redacted = options.redactionEnabled() ? redact(input) : input;
    if (maxChars >= 0 && redacted.length() > maxChars) {
      return redacted.substring(0, maxChars) + "...[truncated]";
    }
    return redacted;
  }

  private String redact(String input) {
    String out = input;
    for (String field : options.redactionFields()) {
      if (field == null || field.isBlank()) continue;
      String quoted = Pattern.quote(field.trim());
      out = out.replaceAll("(?i)(\"" + quoted + "\"\\s*:\\s*)(\"(?:\\\\.|[^\"])*\"|[^,}\\]]+)", "$1\"***\"");
      out = out.replaceAll("(?i)(" + quoted + "\\s*[=:]\\s*)([^,\\s}]+)", "$1***");
      out = out.replaceAll("(?i)(" + quoted + "\\s*:\\s*)(Bearer\\s+[^,\\s}]+)", "$1***");
    }
    return out;
  }

  private boolean sensitiveField(String field) {
    if (!options.redactionEnabled() || field == null) return false;
    for (String sensitive : options.redactionFields()) {
      if (sensitive != null && field.equalsIgnoreCase(sensitive.trim())) return true;
    }
    return false;
  }

  private static void addHeader(ProducerRecord<byte[], byte[]> rec, String name, String value) {
    rec.headers().add(new RecordHeader(name, value == null ? null : value.getBytes(StandardCharsets.UTF_8)));
  }

  private static byte[] toBytes(Object v) {
    return stringify(v).getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] toNullableBytes(Object v) {
    return v == null ? null : toBytes(v);
  }

  private static String stringify(Object v) {
    if (v == null) return "";
    if (v instanceof byte[]) return new String((byte[]) v, StandardCharsets.UTF_8);
    if (v instanceof Byte[]) {
      Byte[] boxed = (Byte[]) v;
      byte[] raw = new byte[boxed.length];
      for (int i = 0; i < boxed.length; i++) raw[i] = boxed[i];
      return new String(raw, StandardCharsets.UTF_8);
    }
    return String.valueOf(v);
  }
}
