package fr.etech.kafka.connect.http;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * HTTP sink connector (open-source, Apache 2.0).
 *
 *
 * @see EtechHttpSinkConfig
 * @see EtechHttpSinkTask
 */
public final class EtechHttpSinkConnector extends SinkConnector {

  private Map<String, String> props;

  @Override public String version() {
    return connectorVersion();
  }

  static String connectorVersion() {
    Package pkg = EtechHttpSinkConnector.class.getPackage();
    String implementationVersion = pkg == null ? null : pkg.getImplementationVersion();
    return implementationVersion == null || implementationVersion.isBlank()
        ? "unknown"
        : implementationVersion;
  }

  @Override public void start(Map<String, String> props) {
    // Fail fast on invalid config.
    new EtechHttpSinkConfig(props);
    this.props = new HashMap<>(props);
  }

  @Override public Class<? extends Task> taskClass() { return EtechHttpSinkTask.class; }

  @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> out = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; i++) out.add(new HashMap<>(props));
    return out;
  }

  @Override public void stop() { /* nothing to release */ }

  @Override public ConfigDef config() { return EtechHttpSinkConfig.CONFIG_DEF; }
}
