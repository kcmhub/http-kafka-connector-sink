package fr.etech.kafka.connect.http;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import fr.etech.kafka.connect.http.reporting.KafkaResponseReporter;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link EtechHttpSinkTask#handleTerminalFailure(SinkRecord, Exception)}.
 *
 * <p>Covers the contract documented in {@code CHANGELOG.md} for v1.4.0:
 * a record that fails terminally (non-retryable status / retries exhausted) must
 * not crash the task as long as it has been published to the error topic and the
 * cumulative error count stays below {@code connect.http.error.threshold}.</p>
 */
class EtechHttpSinkTaskTest {

  private static final String TOPIC = "input-topic";
  private static final SinkRecord RECORD =
      new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, "k", Schema.STRING_SCHEMA, "v", 42L);
  private static final ConnectException FAILURE =
      new ConnectException("HTTP PUT https://api.example/v1/object/account/x failed with status 500");

  @Test
  @DisplayName("default behavior (report_and_continue) with error reporter: swallows the exception")
  void defaultBehaviorSwallowsExceptionWhenReporterPresent() {
    KafkaResponseReporter reporter = Mockito.mock(KafkaResponseReporter.class);
    EtechHttpSinkTask task = new EtechHttpSinkTask(
        config(Map.of()),                   // default behavior.on.error
        reporter,
        "etech-http-sink");

    assertDoesNotThrow(() -> task.handleTerminalFailure(RECORD, FAILURE));
  }

  @Test
  @DisplayName("default behavior with NO error reporter: still swallows but logs a data-loss warning")
  void defaultBehaviorSwallowsEvenWithoutReporter() {
    EtechHttpSinkTask task = new EtechHttpSinkTask(
        config(Map.of()),
        null,                               // no error reporter — operator misconfigured
        "etech-http-sink");

    // Contract: even without a reporter we keep the task alive (behavior.on.error wins);
    // the warning in the logs is the operator's signal that records are being dropped.
    assertDoesNotThrow(() -> task.handleTerminalFailure(RECORD, FAILURE));
  }

  @Test
  @DisplayName("behavior.on.error=fail: throws to crash the task (strict mode)")
  void failModeThrows() {
    KafkaResponseReporter reporter = Mockito.mock(KafkaResponseReporter.class);
    EtechHttpSinkTask task = new EtechHttpSinkTask(
        config(Map.of(EtechHttpSinkConfig.BEHAVIOR_ON_ERROR, "fail")),
        reporter,
        "etech-http-sink");

    ConnectException thrown = assertThrows(ConnectException.class,
        () -> task.handleTerminalFailure(RECORD, FAILURE));
    assertTrue(thrown.getMessage().contains("failed with status 500"));
  }

  @Test
  @DisplayName("behavior.on.error=fail: wraps non-ConnectException in ConnectException")
  void failModeWrapsGenericException() {
    EtechHttpSinkTask task = new EtechHttpSinkTask(
        config(Map.of(EtechHttpSinkConfig.BEHAVIOR_ON_ERROR, "fail")),
        null,
        "etech-http-sink");

    Exception cause = new RuntimeException("boom");
    ConnectException thrown = assertThrows(ConnectException.class,
        () -> task.handleTerminalFailure(RECORD, cause));
    assertEquals(cause, thrown.getCause());
  }

  @Test
  @DisplayName("error.threshold acts as a circuit breaker even in report_and_continue mode")
  void thresholdBreachesAlwaysThrow() {
    KafkaResponseReporter reporter = Mockito.mock(KafkaResponseReporter.class);
    EtechHttpSinkTask task = new EtechHttpSinkTask(
        // threshold = 2 → first 2 failures swallowed, the 3rd trips the breaker
        config(Map.of(EtechHttpSinkConfig.ERROR_THRESHOLD, "2")),
        reporter,
        "etech-http-sink");

    assertDoesNotThrow(() -> task.handleTerminalFailure(RECORD, FAILURE)); // count=1
    assertDoesNotThrow(() -> task.handleTerminalFailure(RECORD, FAILURE)); // count=2
    ConnectException thrown = assertThrows(ConnectException.class,
        () -> task.handleTerminalFailure(RECORD, FAILURE));                // count=3 > 2
    assertTrue(thrown.getMessage().contains("Error threshold (2) exceeded"));
  }

  @Test
  @DisplayName("error.threshold breach in fail mode: also throws with the threshold-exceeded message")
  void thresholdBreachesInFailMode() {
    EtechHttpSinkTask task = new EtechHttpSinkTask(
        config(Map.of(
            EtechHttpSinkConfig.BEHAVIOR_ON_ERROR, "fail",
            EtechHttpSinkConfig.ERROR_THRESHOLD, "1")),
        null,
        "etech-http-sink");

    // count=1 → not yet > 1, so the per-record FAIL path throws with the original message
    ConnectException first = assertThrows(ConnectException.class,
        () -> task.handleTerminalFailure(RECORD, FAILURE));
    assertTrue(first.getMessage().contains("failed with status 500"));

    // count=2 → > 1 → the threshold check fires first, with its own message
    ConnectException second = assertThrows(ConnectException.class,
        () -> task.handleTerminalFailure(RECORD, FAILURE));
    assertTrue(second.getMessage().contains("Error threshold (1) exceeded"));
  }

  @Test
  @DisplayName("config defaults to report_and_continue")
  void configDefault() {
    EtechHttpSinkConfig cfg = config(Map.of());
    assertEquals(EtechHttpSinkConfig.BehaviorOnError.REPORT_AND_CONTINUE, cfg.behaviorOnError());
  }

  @Test
  @DisplayName("invalid behavior.on.error value is rejected at config time")
  void invalidBehaviorRejected() {
    assertThrows(org.apache.kafka.common.config.ConfigException.class,
        () -> config(Map.of(EtechHttpSinkConfig.BEHAVIOR_ON_ERROR, "ignore")));
  }

  // ---------------------------------------------------------------------------

  private static EtechHttpSinkConfig config(Map<String, String> overrides) {
    Map<String, String> props = new HashMap<>();
    props.put(EtechHttpSinkConfig.HTTP_ENDPOINT, "https://api.example.test");
    props.putAll(overrides);
    return new EtechHttpSinkConfig(props);
  }
}

