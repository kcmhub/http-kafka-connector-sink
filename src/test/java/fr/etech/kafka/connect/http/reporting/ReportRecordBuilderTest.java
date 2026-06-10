package fr.etech.kafka.connect.http.reporting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class ReportRecordBuilderTest {

  @Test
  void defaultModeKeepsRenderedRequestBodyAsAckValue() {
    ReportRecordBuilder builder = new ReportRecordBuilder(ReportingOptions.defaults());
    SinkRecord record = record("input-key", "{\"payload\":\"transformed\"}", 2, 53L);
    String request = "{\"order\":1}";
    String response = "{\"success\":false,\"code\":\"ORDER_ALREADY_EXISTS\"}";

    ProducerRecord<byte[], byte[]> report = builder.build(
        "ack.success",
        record,
        "POST",
        "https://api.example.test/v1/orders",
        request,
        Map.of("Content-Type", "application/json"),
        200,
        response,
        Map.of("Content-Type", List.of("application/json")));

    assertEquals(request, asString(report.value()));
    assertEquals("source.topic", header(report, "input_topic"));
    assertEquals("2", header(report, "input_partition"));
    assertEquals("53", header(report, "input_offset"));
    assertEquals("input-key", header(report, "input_key"));
    assertEquals("{\"payload\":\"transformed\"}", header(report, "input_payload"));
    assertEquals(response, header(report, "response_content"));
    assertEquals("200", header(report, "response_status"));
    assertEquals("200", header(report, "response_status_code"));
    assertEquals("200", header(report, "status_code"));
    assertEquals("200", header(report, "http_status_code"));
    assertEquals("POST", header(report, "http_method"));
    assertEquals("https://api.example.test/v1/orders", header(report, "http_url"));
    assertNull(report.headers().lastHeader("http.status.code"));
    assertNull(report.headers().lastHeader("http.method"));
    assertNull(report.headers().lastHeader("input.topic"));
  }

  @Test
  void responseOnlyModeCanUseRawZuoraResponseAsValue() {
    ReportingOptions options = new ReportingOptions(
        ReportingOptions.ValueMode.RESPONSE_ONLY,
        true,
        false,
        false,
        false,
        false,
        true,
        false,
        false,
        false,
        List.of(),
        -1,
        -1);
    ReportRecordBuilder builder = new ReportRecordBuilder(options);
    SinkRecord record = record("input-key", "{\"payload\":\"transformed\"}", 2, 53L);
    String response = "{\"success\":false,\"code\":\"ORDER_ALREADY_EXISTS\"}";

    ProducerRecord<byte[], byte[]> report = builder.build(
        "ack.success",
        record,
        "POST",
        "https://api.example.test/v1/orders",
        "{\"order\":1}",
        Map.of("Content-Type", "application/json"),
        200,
        response,
        Map.of("Content-Type", List.of("application/json")));

    assertEquals(response, asString(report.value()));
  }

  @Test
  void envelopeCanIncludeInputRequestAndResponseWithRedaction() {
    ReportingOptions options = new ReportingOptions(
        ReportingOptions.ValueMode.ENVELOPE,
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        true,
        List.of("iban", "taxNumber", "accountNumber", "Authorization", "client_secret"),
        10_000,
        10_000);
    ReportRecordBuilder builder = new ReportRecordBuilder(options);
    SinkRecord record = record("customer-1",
        "{\"iban\":\"BE123\",\"taxNumber\":\"FR999\",\"name\":\"Acme\"}", 0, 7L);

    ProducerRecord<byte[], byte[]> report = builder.build(
        "ack.success",
        record,
        "POST",
        "https://api.example.test/v1/accounts",
        "{\"client_secret\":\"secret-value\",\"amount\":12}",
        Map.of("Authorization", "Bearer token", "Content-Type", "application/json"),
        201,
        "{\"success\":true,\"accountNumber\":\"A0001\"}",
        Map.of("Authorization", List.of("Bearer token"), "X-Correlation-Id", List.of("corr-1")));

    String envelope = asString(report.value());
    assertTrue(envelope.contains("\"input_offset\":7"));
    assertTrue(envelope.contains("\"input_key\":\"customer-1\""));
    assertTrue(envelope.contains("\"input_payload\""));
    assertTrue(envelope.contains("\"response_status\":201"));
    assertTrue(envelope.contains("\"response_status_code\":201"));
    assertTrue(envelope.contains("\"status_code\":201"));
    assertFalse(envelope.contains("BE123"));
    assertFalse(envelope.contains("FR999"));
    assertFalse(envelope.contains("A0001"));
    assertFalse(envelope.contains("secret-value"));
    assertFalse(envelope.contains("Bearer token"));
    assertTrue(envelope.contains("\"response_headers\""));
    String inputPayload = header(report, "input_payload");
    assertNotNull(inputPayload);
    assertEquals("***", inputPayload.contains("BE123") ? "leaked" : "***");
    String responseHeadersJson = header(report, "response_headers");
    assertNotNull(responseHeadersJson);
    assertTrue(responseHeadersJson.contains("\"Authorization\":[\"***\"]"));
  }

  @Test
  void reportedInputPayloadIsThePostSmtRecordSeenByTheTask() {
    ReportingOptions options = new ReportingOptions(
        ReportingOptions.ValueMode.ENVELOPE,
        true,
        false,
        true,
        true,
        false,
        true,
        false,
        false,
        false,
        List.of(),
        -1,
        -1);
    ReportRecordBuilder builder = new ReportRecordBuilder(options);

    SinkRecord postSmtRecord = record(null, "{\"payload\":\"after-smt\"}", 1, 12L);
    ProducerRecord<byte[], byte[]> report = builder.build(
        "ack.success",
        postSmtRecord,
        "POST",
        "https://api.example.test",
        "{\"payload\":\"after-smt\"}",
        Map.of(),
        200,
        "{\"success\":true}",
        Map.of());

    String envelope = asString(report.value());
    assertTrue(envelope.contains("after-smt"));
    assertEquals("{\"payload\":\"after-smt\"}", header(report, "input_payload"));
  }

  @Test
  void structPayloadFieldIsReportedAsRawPayloadForBppAckCompatibility() {
    ReportRecordBuilder builder = new ReportRecordBuilder(ReportingOptions.defaults());
    Schema schema = SchemaBuilder.struct()
        .field("payload", Schema.STRING_SCHEMA)
        .build();
    String payload = "{\"existingAccountNumber\":\"acc-1\",\"orderDate\":\"2025-03-19\"}";
    SinkRecord record = record("BE99024210046", new Struct(schema).put("payload", payload), 5, 44L);

    ProducerRecord<byte[], byte[]> report = builder.build(
        "ack.error",
        record,
        "POST",
        "https://api.example.test/v1/orders",
        payload,
        Map.of(),
        200,
        "{\"success\":false}",
        Map.of());

    assertEquals(payload, header(report, "input_payload"));
    String inputPayload = header(report, "input_payload");
    assertNotNull(inputPayload);
    assertFalse(inputPayload.startsWith("Struct{"));
  }

  @Test
  void genericStructWithoutPayloadFieldIsReportedAsJsonObject() {
    ReportRecordBuilder builder = new ReportRecordBuilder(ReportingOptions.defaults());
    Schema schema = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("amount", Schema.FLOAT64_SCHEMA)
        .build();
    SinkRecord record = record("customer-1", new Struct(schema).put("name", "Acme").put("amount", 12.5), 0, 8L);

    ProducerRecord<byte[], byte[]> report = builder.build(
        "ack.success",
        record,
        "POST",
        "https://api.example.test/v1/accounts",
        "{\"name\":\"Acme\",\"amount\":12.5}",
        Map.of(),
        201,
        "{\"success\":true}",
        Map.of());

    assertEquals("{\"name\":\"Acme\",\"amount\":12.5}", header(report, "input_payload"));
  }

  @Test
  void inputKeyHeaderIsPresentWhenSourceKeyIsNull() {
    ReportRecordBuilder builder = new ReportRecordBuilder(ReportingOptions.defaults());
    SinkRecord record = record(null, "{\"payload\":\"transformed\"}", 2, 30L);

    ProducerRecord<byte[], byte[]> report = builder.build(
        "ack.success",
        record,
        "POST",
        "https://api.example.test/v1/orders",
        "{\"order\":1}",
        Map.of(),
        200,
        "{\"success\":true}",
        Map.of());

    assertNotNull(report.headers().lastHeader("input_key"));
    assertNull(report.headers().lastHeader("input_key").value());
    assertNull(report.key());
  }

  @Test
  void responseHeadersAreDisabledByDefault() {
    ReportRecordBuilder builder = new ReportRecordBuilder(ReportingOptions.defaults());
    SinkRecord record = record("input-key", "{\"payload\":\"transformed\"}", 2, 53L);

    ProducerRecord<byte[], byte[]> report = builder.build(
        "ack.success",
        record,
        "POST",
        "https://api.example.test/v1/orders",
        "{\"order\":1}",
        Map.of(),
        200,
        "{\"success\":true}",
        Map.of("X-Request-Id", List.of("req-1")));

    assertNull(report.headers().lastHeader("response_headers"));
  }

  @Test
  void responseHeadersCanBePublishedAsKafkaHeaderJson() {
    ReportingOptions options = new ReportingOptions(
        ReportingOptions.ValueMode.REQUEST_BODY,
        true,
        true,
        true,
        false,
        false,
        true,
        true,
        true,
        false,
        List.of(),
        -1,
        -1);
    ReportRecordBuilder builder = new ReportRecordBuilder(options);
    SinkRecord record = record("input-key", "{\"payload\":\"transformed\"}", 2, 53L);

    ProducerRecord<byte[], byte[]> report = builder.build(
        "ack.success",
        record,
        "POST",
        "https://api.example.test/v1/orders",
        "{\"order\":1}",
        Map.of(),
        200,
        "{\"success\":true}",
        Map.of("X-Request-Id", List.of("req-1"), "Set-Cookie", List.of("a=1", "b=2")));

    String headerJson = header(report, "response_headers");
    assertNotNull(headerJson);
    assertTrue(headerJson.contains("\"X-Request-Id\":[\"req-1\"]"));
    assertTrue(headerJson.contains("\"Set-Cookie\":[\"a=1\",\"b=2\"]"));
  }

  private static SinkRecord record(Object key, Object value, int partition, long offset) {
    return new SinkRecord(
        "source.topic",
        partition,
        null,
        key,
        null,
        value,
        offset,
        1710000000000L,
        null);
  }

  private static String header(ProducerRecord<byte[], byte[]> report, String name) {
    Header header = report.headers().lastHeader(name);
    if (header == null) return null;
    return header.value() == null ? "" : asString(header.value());
  }

  private static String asString(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
