package fr.etech.kafka.connect.http.auth;

/** No-op authenticator for endpoints that require no Authorization header. */
public final class NoAuthAuthenticator implements Authenticator {
  @Override public String authorizationHeader() { return null; }
}

