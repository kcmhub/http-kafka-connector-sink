package fr.etech.kafka.connect.http.template;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
/**
 * Renders Mustache-style {{...}} placeholders against a SinkRecord.
 *
 * <p>Supports both schema-ful (Struct) and schemaless (Map / raw scalar) values,
 * dotted paths and record metadata (topic/partition/offset/timestamp).
 *
 * <p>Missing values render to empty string; this avoids hard-failing the task
 * when an optional header or field is absent.
 */
public final class RecordTemplateRenderer {
  private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{\\s*([^}\\s]+)\\s*}}");
  private RecordTemplateRenderer() { /* utility */ }
  public static String render(String template, SinkRecord record) {
    if (template == null || template.isEmpty()) return template;
    Matcher m = PLACEHOLDER.matcher(template);
    StringBuilder out = new StringBuilder(template.length());
    while (m.find()) {
      String resolved = resolve(m.group(1), record);
      m.appendReplacement(out, Matcher.quoteReplacement(resolved == null ? "" : resolved));
    }
    m.appendTail(out);
    return out.toString();
  }
  public static Map<String, String> renderHeadersCsv(String csv, SinkRecord record) {
    Map<String, String> out = new LinkedHashMap<>();
    if (csv == null || csv.isBlank()) return out;
    for (String entry : csv.split(",")) {
      int i = entry.indexOf(':');
      if (i <= 0) continue;
      String name = render(entry.substring(0, i).trim(), record);
      String value = render(entry.substring(i + 1).trim(), record);
      if (name.isEmpty() || value.isEmpty()) continue;
      out.put(name, value);
    }
    return out;
  }
  private static String resolve(String expr, SinkRecord r) {
    switch (expr) {
      case "key":       return stringify(r.key());
      case "value":     return stringify(r.value());
      case "topic":     return r.topic();
      case "partition": return Integer.toString(r.kafkaPartition());
      case "offset":    return Long.toString(r.kafkaOffset());
      case "timestamp": return r.timestamp() == null ? "" : r.timestamp().toString();
      default:
    }
    if (expr.startsWith("value.")) {
      return stringify(walkPath(r.value(), expr.substring("value.".length())));
    }
    if (expr.startsWith("key.")) {
      return stringify(walkPath(r.key(), expr.substring("key.".length())));
    }
    if (expr.startsWith("header.")) {
      Header h = r.headers().lastWithName(expr.substring("header.".length()));
      return stringify(h == null ? null : h.value());
    }
    return "";
  }
  private static Object walkPath(Object root, String path) {
    if (root == null || path == null || path.isEmpty()) return null;
    Object cur = root;
    for (String seg : path.split("\\.")) {
      if (cur == null) return null;
      if (cur instanceof Struct) {
        cur = safeStructGet((Struct) cur, seg);
      } else if (cur instanceof Map) {
        cur = ((Map<?, ?>) cur).get(seg);
      } else {
        return null;
      }
    }
    return cur;
  }
  private static Object safeStructGet(Struct s, String field) {
    try { return s.get(field); }
    catch (Exception missingField) { return null; }
  }
  private static String stringify(Object v) {
    if (v == null) return "";
    if (v instanceof byte[]) return new String((byte[]) v, StandardCharsets.UTF_8);
    return v.toString();
  }
}