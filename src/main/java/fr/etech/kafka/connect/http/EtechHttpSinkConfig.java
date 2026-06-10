package fr.etech.kafka.connect.http;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

/**
 * Configuration of {@link EtechHttpSinkConnector}.
 *
 * <p>Keys live in the {@code connect.http.*} and {@code connect.reporting.*} namespaces.
 */
public final class EtechHttpSinkConfig extends AbstractConfig {

  // --- request ---------------------------------------------------------------
  public static final String HTTP_ENDPOINT             = "connect.http.endpoint";
  public static final String HTTP_METHOD               = "connect.http.method";
  public static final String HTTP_REQUEST_CONTENT      = "connect.http.request.content";
  public static final String HTTP_REQUEST_HEADERS      = "connect.http.request.headers";
  public static final String HTTP_CONNECT_TIMEOUT_MS   = "connect.http.connect.timeout.ms";
  public static final String HTTP_REQUEST_TIMEOUT_MS   = "connect.http.request.timeout.ms";

  // --- retries ---------------------------------------------------------------
  public static final String RETRY_MODE                = "connect.http.retry.mode";
  public static final String RETRIES_MAX               = "connect.http.retries.max.retries";
  public static final String RETRIES_STATUS_CODES      = "connect.http.retries.on.status.codes";
  public static final String RETRIES_INITIAL_DELAY_MS  = "connect.http.retries.initial.delay.ms";
  public static final String RETRIES_MAX_DELAY_MS      = "connect.http.retries.max.delay.ms";

  // --- error handling --------------------------------------------------------
  public static final String ERROR_THRESHOLD           = "connect.http.error.threshold";

  // --- authentication --------------------------------------------------------
  public static final String AUTH_TYPE                 = "connect.http.authentication.type";
  public static final String AUTH_BASIC_USER           = "connect.http.authentication.basic.user";
  public static final String AUTH_BASIC_PASSWORD       = "connect.http.authentication.basic.password";
  public static final String OAUTH2_TOKEN_URL          = "connect.http.authentication.oauth2.token.url";
  public static final String OAUTH2_CLIENT_ID          = "connect.http.authentication.oauth2.client.id";
  public static final String OAUTH2_CLIENT_SECRET      = "connect.http.authentication.oauth2.client.secret";
  public static final String OAUTH2_CLIENT_SCOPE       = "connect.http.authentication.oauth2.client.scope";
  public static final String OAUTH2_TOKEN_PROPERTY     = "connect.http.authentication.oauth2.token.property";
  public static final String OAUTH2_CLIENT_HEADERS     = "connect.http.authentication.oauth2.client.headers";

  // --- reporting (success + error) ------------------------------------------
  public static final String REP_SUCCESS_ENABLED       = "connect.reporting.success.config.enabled";
  public static final String REP_SUCCESS_BOOTSTRAP     = "connect.reporting.success.config.bootstrap.servers";
  public static final String REP_SUCCESS_TOPIC         = "connect.reporting.success.config.topic";
  public static final String REP_SUCCESS_SEC_PROTO     = "connect.reporting.success.config.security.protocol";
  public static final String REP_SUCCESS_SASL_MECH     = "connect.reporting.success.config.sasl.mechanism";
  public static final String REP_SUCCESS_SASL_JAAS     = "connect.reporting.success.config.sasl.jaas.config";

  public static final String REP_ERROR_ENABLED         = "connect.reporting.error.config.enabled";
  public static final String REP_ERROR_BOOTSTRAP       = "connect.reporting.error.config.bootstrap.servers";
  public static final String REP_ERROR_TOPIC           = "connect.reporting.error.config.topic";
  public static final String REP_ERROR_SEC_PROTO       = "connect.reporting.error.config.security.protocol";
  public static final String REP_ERROR_SASL_MECH       = "connect.reporting.error.config.sasl.mechanism";
  public static final String REP_ERROR_SASL_JAAS       = "connect.reporting.error.config.sasl.jaas.config";

  public static final String REP_VALUE_MODE            = "connect.reporting.value.mode";
  public static final String REP_INCLUDE_INPUT_METADATA = "connect.reporting.include.input.metadata";
  public static final String REP_INCLUDE_INPUT_KEY     = "connect.reporting.include.input.key";
  public static final String REP_INCLUDE_INPUT_PAYLOAD = "connect.reporting.include.input.payload";
  public static final String REP_INCLUDE_TRANSFORMED_INPUT_PAYLOAD =
      "connect.reporting.include.transformed.input.payload";
  public static final String REP_INCLUDE_REQUEST_BODY  = "connect.reporting.include.request.body";
  public static final String REP_INCLUDE_REQUEST_HEADERS = "connect.reporting.include.request.headers";
  public static final String REP_INCLUDE_RESPONSE_CONTENT = "connect.reporting.include.response.content";
  public static final String REP_INCLUDE_RESPONSE_HEADERS = "connect.reporting.include.response.headers";
  public static final String REP_INCLUDE_HTTP_METADATA = "connect.reporting.include.http.metadata";
  public static final String REP_REDACTION_ENABLED     = "connect.reporting.redaction.enabled";
  public static final String REP_REDACTION_FIELDS      = "connect.reporting.redaction.fields";
  public static final String REP_MAX_PAYLOAD_BYTES     = "connect.reporting.max.payload.bytes";
  public static final String REP_MAX_RESPONSE_BYTES    = "connect.reporting.max.response.bytes";

  // --- enums -----------------------------------------------------------------
  public enum AuthType { NONE, BASIC, OAUTH2 }
  public enum RetryMode { NONE, EXPONENTIAL }
  public enum ReportingValueMode { REQUEST_BODY, RESPONSE_ONLY, ENVELOPE }

  public EtechHttpSinkConfig(Map<String, String> originals) {
    super(CONFIG_DEF, originals);
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(HTTP_ENDPOINT, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH, "Target URL. Templated.")
      .define(HTTP_METHOD, ConfigDef.Type.STRING, "POST",
              ConfigDef.ValidString.in("GET", "POST", "PUT", "PATCH", "DELETE"),
              ConfigDef.Importance.HIGH, "HTTP method.")
      .define(HTTP_REQUEST_CONTENT, ConfigDef.Type.STRING, "",
              ConfigDef.Importance.MEDIUM, "Request body template.")
      .define(HTTP_REQUEST_HEADERS, ConfigDef.Type.STRING, "",
              ConfigDef.Importance.MEDIUM, "Comma-separated `Name:Value` header templates.")
      .define(HTTP_CONNECT_TIMEOUT_MS, ConfigDef.Type.LONG, 10_000L,
              ConfigDef.Importance.LOW, "TCP connect timeout (ms).")
      .define(HTTP_REQUEST_TIMEOUT_MS, ConfigDef.Type.LONG, 30_000L,
              ConfigDef.Importance.LOW, "Per-request total timeout (ms).")

      .define(RETRY_MODE, ConfigDef.Type.STRING, "exponential",
              ConfigDef.ValidString.in("none", "exponential"),
              ConfigDef.Importance.LOW, "Retry backoff strategy.")
      .define(RETRIES_MAX, ConfigDef.Type.INT, 3,
              ConfigDef.Importance.LOW, "Max retries per record.")
      .define(RETRIES_STATUS_CODES, ConfigDef.Type.LIST, "408,429,500,502,503,504",
              ConfigDef.Importance.LOW, "Status codes that trigger a retry.")
      .define(RETRIES_INITIAL_DELAY_MS, ConfigDef.Type.LONG, 500L,
              ConfigDef.Importance.LOW, "Initial backoff delay (ms).")
      .define(RETRIES_MAX_DELAY_MS, ConfigDef.Type.LONG, 30_000L,
              ConfigDef.Importance.LOW, "Backoff cap (ms).")

      .define(ERROR_THRESHOLD, ConfigDef.Type.INT, 1000,
              ConfigDef.Importance.LOW, "Failed sends before task fails.")

      .define(AUTH_TYPE, ConfigDef.Type.STRING, "none",
              ConfigDef.ValidString.in("none", "basic", "oauth2"),
              ConfigDef.Importance.HIGH, "Auth scheme.")
      .define(AUTH_BASIC_USER, ConfigDef.Type.STRING, "",
              ConfigDef.Importance.MEDIUM, "Basic auth username.")
      .define(AUTH_BASIC_PASSWORD, ConfigDef.Type.PASSWORD, "",
              ConfigDef.Importance.MEDIUM, "Basic auth password.")
      .define(OAUTH2_TOKEN_URL, ConfigDef.Type.STRING, "",
              ConfigDef.Importance.MEDIUM, "OAuth2 token endpoint.")
      .define(OAUTH2_CLIENT_ID, ConfigDef.Type.STRING, "",
              ConfigDef.Importance.MEDIUM, "OAuth2 client id.")
      .define(OAUTH2_CLIENT_SECRET, ConfigDef.Type.PASSWORD, "",
              ConfigDef.Importance.MEDIUM, "OAuth2 client secret.")
      .define(OAUTH2_CLIENT_SCOPE, ConfigDef.Type.STRING, "",
              ConfigDef.Importance.LOW, "OAuth2 scope.")
      .define(OAUTH2_TOKEN_PROPERTY, ConfigDef.Type.STRING, "access_token",
              ConfigDef.Importance.LOW, "JSON property name of the bearer token.")
      .define(OAUTH2_CLIENT_HEADERS, ConfigDef.Type.STRING,
              "Content-Type:application/x-www-form-urlencoded",
              ConfigDef.Importance.LOW, "Headers sent on the token request.")

      .define(REP_SUCCESS_ENABLED, ConfigDef.Type.BOOLEAN, false,
              ConfigDef.Importance.LOW, "Publish 2xx responses to a Kafka topic.")
      .define(REP_SUCCESS_BOOTSTRAP, ConfigDef.Type.STRING, "",
              ConfigDef.Importance.LOW, "Reporter bootstrap servers (success).")
      .define(REP_SUCCESS_TOPIC, ConfigDef.Type.STRING, "",
              ConfigDef.Importance.LOW, "Success report topic.")
      .define(REP_SUCCESS_SEC_PROTO, ConfigDef.Type.STRING, "PLAINTEXT",
              ConfigDef.Importance.LOW, "Reporter security.protocol (success).")
      .define(REP_SUCCESS_SASL_MECH, ConfigDef.Type.STRING, "",
              ConfigDef.Importance.LOW, "Reporter sasl.mechanism (success).")
      .define(REP_SUCCESS_SASL_JAAS, ConfigDef.Type.PASSWORD, "",
              ConfigDef.Importance.LOW, "Reporter sasl.jaas.config (success).")

      .define(REP_ERROR_ENABLED, ConfigDef.Type.BOOLEAN, false,
              ConfigDef.Importance.LOW, "Publish non-2xx responses to a Kafka topic.")
      .define(REP_ERROR_BOOTSTRAP, ConfigDef.Type.STRING, "",
              ConfigDef.Importance.LOW, "Reporter bootstrap servers (error).")
      .define(REP_ERROR_TOPIC, ConfigDef.Type.STRING, "",
              ConfigDef.Importance.LOW, "Error report topic.")
      .define(REP_ERROR_SEC_PROTO, ConfigDef.Type.STRING, "PLAINTEXT",
              ConfigDef.Importance.LOW, "Reporter security.protocol (error).")
      .define(REP_ERROR_SASL_MECH, ConfigDef.Type.STRING, "",
              ConfigDef.Importance.LOW, "Reporter sasl.mechanism (error).")
      .define(REP_ERROR_SASL_JAAS, ConfigDef.Type.PASSWORD, "",
              ConfigDef.Importance.LOW, "Reporter sasl.jaas.config (error).")

      .define(REP_VALUE_MODE, ConfigDef.Type.STRING, "request_body",
              ConfigDef.ValidString.in("request_body", "response_only", "envelope"),
              ConfigDef.Importance.LOW, "Reported value shape.")
      .define(REP_INCLUDE_INPUT_METADATA, ConfigDef.Type.BOOLEAN, true,
              ConfigDef.Importance.LOW, "Include source topic/partition/offset/timestamp in report headers and envelope.")
      .define(REP_INCLUDE_INPUT_KEY, ConfigDef.Type.BOOLEAN, true,
              ConfigDef.Importance.LOW, "Include the transformed source key in report headers/envelope.")
      .define(REP_INCLUDE_INPUT_PAYLOAD, ConfigDef.Type.BOOLEAN, true,
              ConfigDef.Importance.LOW, "Include the transformed source value in report headers/envelope.")
      .define(REP_INCLUDE_TRANSFORMED_INPUT_PAYLOAD, ConfigDef.Type.BOOLEAN, true,
              ConfigDef.Importance.LOW, "Clarifies that input payload reporting uses the post-SMT record seen by the task.")
      .define(REP_INCLUDE_REQUEST_BODY, ConfigDef.Type.BOOLEAN, false,
              ConfigDef.Importance.LOW, "Include the rendered HTTP request body in report headers/envelope.")
      .define(REP_INCLUDE_REQUEST_HEADERS, ConfigDef.Type.BOOLEAN, false,
              ConfigDef.Importance.LOW, "Include rendered HTTP request headers in the envelope.")
      .define(REP_INCLUDE_RESPONSE_CONTENT, ConfigDef.Type.BOOLEAN, true,
              ConfigDef.Importance.LOW, "Include the HTTP response body in report headers/envelope, or as the value in response_only mode.")
      .define(REP_INCLUDE_RESPONSE_HEADERS, ConfigDef.Type.BOOLEAN, false,
              ConfigDef.Importance.LOW, "Include HTTP response headers as JSON in report headers/envelope.")
      .define(REP_INCLUDE_HTTP_METADATA, ConfigDef.Type.BOOLEAN, true,
              ConfigDef.Importance.LOW, "Include HTTP status/method/url metadata as underscore headers.")
      .define(REP_REDACTION_ENABLED, ConfigDef.Type.BOOLEAN, false,
              ConfigDef.Importance.LOW, "Mask configured sensitive field names in reported text.")
      .define(REP_REDACTION_FIELDS, ConfigDef.Type.LIST,
              "iban,taxNumber,accountNumber,Authorization,client_secret",
              ConfigDef.Importance.LOW, "Field/header names to mask when redaction is enabled.")
      .define(REP_MAX_PAYLOAD_BYTES, ConfigDef.Type.INT, -1,
              ConfigDef.Importance.LOW, "Max reported input/request payload size in characters; -1 disables truncation.")
      .define(REP_MAX_RESPONSE_BYTES, ConfigDef.Type.INT, -1,
              ConfigDef.Importance.LOW, "Max reported response size in characters; -1 disables truncation.");

  // --- typed accessors -------------------------------------------------------
  public String endpoint()             { return getString(HTTP_ENDPOINT); }
  public String method()               { return getString(HTTP_METHOD).toUpperCase(); }
  public String requestBodyTemplate()  { return getString(HTTP_REQUEST_CONTENT); }
  public String requestHeadersTpl()    { return getString(HTTP_REQUEST_HEADERS); }
  public long   connectTimeoutMs()     { return getLong(HTTP_CONNECT_TIMEOUT_MS); }
  public long   requestTimeoutMs()     { return getLong(HTTP_REQUEST_TIMEOUT_MS); }

  public RetryMode retryMode()         { return RetryMode.valueOf(getString(RETRY_MODE).toUpperCase()); }
  public int    maxRetries()           { return getInt(RETRIES_MAX); }
  public List<Integer> retryStatusCodes() {
    List<Integer> out = new java.util.ArrayList<>();
    for (String s : getList(RETRIES_STATUS_CODES)) out.add(Integer.parseInt(s.trim()));
    return out;
  }
  public long   initialDelayMs()       { return getLong(RETRIES_INITIAL_DELAY_MS); }
  public long   maxDelayMs()           { return getLong(RETRIES_MAX_DELAY_MS); }
  public int    errorThreshold()       { return getInt(ERROR_THRESHOLD); }

  public AuthType authType()           { return AuthType.valueOf(getString(AUTH_TYPE).toUpperCase()); }
  public String basicUser()            { return getString(AUTH_BASIC_USER); }
  public Password basicPassword()      { return getPassword(AUTH_BASIC_PASSWORD); }
  public String oauth2TokenUrl()       { return getString(OAUTH2_TOKEN_URL); }
  public String oauth2ClientId()       { return getString(OAUTH2_CLIENT_ID); }
  public Password oauth2ClientSecret() { return getPassword(OAUTH2_CLIENT_SECRET); }
  public String oauth2Scope()          { return getString(OAUTH2_CLIENT_SCOPE); }
  public String oauth2TokenProperty()  { return getString(OAUTH2_TOKEN_PROPERTY); }
  public String oauth2ClientHeaders()  { return getString(OAUTH2_CLIENT_HEADERS); }

  public boolean successEnabled()      { return getBoolean(REP_SUCCESS_ENABLED); }
  public Map<String, Object> successReporterProps() {
    return reporterProps(getString(REP_SUCCESS_BOOTSTRAP), getString(REP_SUCCESS_SEC_PROTO),
        getString(REP_SUCCESS_SASL_MECH), getPassword(REP_SUCCESS_SASL_JAAS));
  }
  public String successTopic()         { return getString(REP_SUCCESS_TOPIC); }

  public boolean errorEnabled()        { return getBoolean(REP_ERROR_ENABLED); }
  public Map<String, Object> errorReporterProps() {
    return reporterProps(getString(REP_ERROR_BOOTSTRAP), getString(REP_ERROR_SEC_PROTO),
        getString(REP_ERROR_SASL_MECH), getPassword(REP_ERROR_SASL_JAAS));
  }
  public String errorTopic()           { return getString(REP_ERROR_TOPIC); }

  public ReportingValueMode reportingValueMode() {
    return ReportingValueMode.valueOf(getString(REP_VALUE_MODE).toUpperCase());
  }
  public boolean reportInputMetadata() { return getBoolean(REP_INCLUDE_INPUT_METADATA); }
  public boolean reportInputKey()      { return getBoolean(REP_INCLUDE_INPUT_KEY); }
  public boolean reportInputPayload()  {
    return getBoolean(REP_INCLUDE_INPUT_PAYLOAD) && getBoolean(REP_INCLUDE_TRANSFORMED_INPUT_PAYLOAD);
  }
  public boolean reportRequestBody()   { return getBoolean(REP_INCLUDE_REQUEST_BODY); }
  public boolean reportRequestHeaders(){ return getBoolean(REP_INCLUDE_REQUEST_HEADERS); }
  public boolean reportResponseContent(){ return getBoolean(REP_INCLUDE_RESPONSE_CONTENT); }
  public boolean reportResponseHeaders(){ return getBoolean(REP_INCLUDE_RESPONSE_HEADERS); }
  public boolean reportHttpMetadata()  { return getBoolean(REP_INCLUDE_HTTP_METADATA); }
  public boolean reportRedactionEnabled(){ return getBoolean(REP_REDACTION_ENABLED); }
  public List<String> reportRedactionFields(){ return getList(REP_REDACTION_FIELDS); }
  public int reportMaxPayloadBytes()   { return getInt(REP_MAX_PAYLOAD_BYTES); }
  public int reportMaxResponseBytes()  { return getInt(REP_MAX_RESPONSE_BYTES); }

  private static Map<String, Object> reporterProps(String bootstrap, String secProto,
                                                   String saslMech, Password saslJaas) {
    Map<String, Object> p = new HashMap<>();
    p.put("bootstrap.servers", bootstrap);
    if (secProto != null && !secProto.isEmpty()) p.put("security.protocol", secProto);
    if (saslMech != null && !saslMech.isEmpty()) p.put("sasl.mechanism", saslMech);
    if (saslJaas != null && !saslJaas.value().isEmpty()) p.put("sasl.jaas.config", saslJaas.value());
    return p;
  }
}

