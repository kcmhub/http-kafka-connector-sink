package fr.etech.kafka.connect.http.reporting;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
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
 *   <li>value — the HTTP response body (bytes)</li>
 *   <li>headers — {@code http.status.code}, {@code http.method},
 *       {@code http.url}, plus original record's topic/partition/offset</li>
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

  public KafkaResponseReporter(Map<String, Object> reporterProps, String topic, String clientId) {
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
  }

  public void publish(SinkRecord original, String method, String url, int status, String responseBody) {
    try {
      byte[] key = original.key() == null ? null : original.key().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      byte[] body = responseBody == null ? new byte[0] : responseBody.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(topic, null, key, body);
      rec.headers().add(new RecordHeader("http.status.code", Integer.toString(status).getBytes()));
      rec.headers().add(new RecordHeader("http.method", method.getBytes()));
      rec.headers().add(new RecordHeader("http.url", url.getBytes()));
      rec.headers().add(new RecordHeader("input.topic", original.topic().getBytes()));
      rec.headers().add(new RecordHeader("input.partition", Integer.toString(original.kafkaPartition()).getBytes()));
      rec.headers().add(new RecordHeader("input.offset", Long.toString(original.kafkaOffset()).getBytes()));
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

