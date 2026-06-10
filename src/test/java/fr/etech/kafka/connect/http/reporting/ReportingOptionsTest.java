package fr.etech.kafka.connect.http.reporting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import fr.etech.kafka.connect.http.EtechHttpSinkConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ReportingOptionsTest {

  @Test
  void responseHeaderFilterNamesAreNormalizedCaseInsensitive() {
    ReportingOptions options = ReportingOptions.from(config(Map.of(
        EtechHttpSinkConfig.REP_RESPONSE_HEADERS_FILTER_NAMES, " Zuora-Track-Id ,zuora-version ")));

    assertEquals(2, options.responseHeadersFilterNames().size());
    assertTrue(options.responseHeadersFilterNames().contains("zuora-track-id"));
    assertTrue(options.responseHeadersFilterNames().contains("zuora-version"));
  }

  @Test
  void invalidResponseHeaderRegexFailsFast() {
    EtechHttpSinkConfig cfg = config(Map.of(
        EtechHttpSinkConfig.REP_RESPONSE_HEADERS_FILTER_REGEX, "zuora(*"));

    assertThrows(IllegalArgumentException.class, () -> ReportingOptions.from(cfg));
  }

  private static EtechHttpSinkConfig config(Map<String, String> overrides) {
    Map<String, String> props = new HashMap<>();
    props.put(EtechHttpSinkConfig.HTTP_ENDPOINT, "https://api.example.test");
    props.putAll(overrides);
    return new EtechHttpSinkConfig(props);
  }
}

