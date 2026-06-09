package fr.etech.kafka.connect.http.reporting;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes HTTP responses to a Kafka topic so downstream apps can observe
 * what the connector actually sent and what came back.
 *
 * <p>Layout of each report record:
 * <ul>
 *   <li>key   — the original record key (passthrough, as bytes)</li>
 *   <li>value — HTTP response body or a JSON envelope, depending on config</li>
 *   <li>headers — HTTP metadata plus optional input/request/response data</li>
 * </ul>
 *
 * <p>{@link #publish(SinkRecord, String, String, int, String)} is fire-and-forget:
 * any send failure is logged at WARN but never propagated to the task — the
 * point of reporting is observability, not correctness.
 */
public final class KafkaResponseReporter implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaResponseReporter.class);

  private final KafkaProducer<byte[], byte[]> producer;
  private final String topic;
  private final ReportRecordBuilder reportRecordBuilder;

  public KafkaResponseReporter(Map<String, Object> reporterProps, String topic, String clientId,
                               ReportingOptions options) {
    Properties p = new Properties();
    p.putAll(reporterProps);
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    p.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    p.put(ProducerConfig.ACKS_CONFIG, "1");
    p.put(ProducerConfig.LINGER_MS_CONFIG, "5");
    p.putIfAbsent(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
    // StringSerializer is loaded eagerly for compatibility — keep reference.
    @SuppressWarnings("unused") Class<?> _s = StringSerializer.class;
    this.producer = new KafkaProducer<>(p);
    this.topic = topic;
    this.reportRecordBuilder = new ReportRecordBuilder(options);
  }

  public void publish(SinkRecord original, String method, String url, String requestBody,
                      Map<String, String> requestHeaders, int status, String responseBody) {
    try {
      ProducerRecord<byte[], byte[]> rec = reportRecordBuilder.build(
          topic, original, method, url, requestBody, requestHeaders, status, responseBody);
      producer.send(rec, (md, ex) -> {
        if (ex != null) LOG.warn("Failed to publish HTTP report to {}: {}", topic, ex.getMessage());
      });
    } catch (Exception e) {
      LOG.warn("Failed to enqueue HTTP report to {}: {}", topic, e.getMessage());
    }
  }

  @Override public void close() {
    try { producer.flush(); } catch (Exception ignore) { /* drain */ }
    try { producer.close(java.time.Duration.ofSeconds(5)); } catch (Exception ignore) { /* shutdown */ }
  }

  public static Map<String, Object> sanitize(Map<String, Object> p) {
    Map<String, Object> out = new HashMap<>(p);
    out.remove("sasl.jaas.config");
    return out;
  }
}

