package fr.etech.kafka.connect.http;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class EtechHttpSinkConnectorTest {

  @Test
  void connectorVersionFallbackIsUnknownWhenManifestVersionIsMissing() {
    assertEquals("unknown", EtechHttpSinkConnector.connectorVersion());
  }
}

