package fr.etech.kafka.connect.http.auth;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Static HTTP Basic credential injection. */
public final class BasicAuthAuthenticator implements Authenticator {

  private final String header;

  public BasicAuthAuthenticator(String user, String password) {
    String raw = (user == null ? "" : user) + ":" + (password == null ? "" : password);
    this.header = "Basic " + Base64.getEncoder()
        .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
  }

  @Override public String authorizationHeader() { return header; }
}

