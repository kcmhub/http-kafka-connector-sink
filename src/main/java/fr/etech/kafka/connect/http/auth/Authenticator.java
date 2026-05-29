package fr.etech.kafka.connect.http.auth;

/** Per-request authorization injection (e.g. an {@code Authorization} header). */
public interface Authenticator extends AutoCloseable {

  /**
   * Returns the value of the {@code Authorization} header to attach to the next
   * request, or {@code null} if no auth header is required.
   *
   * <p>Implementations are expected to cache credentials and refresh them as
   * needed. This method is invoked once per HTTP attempt.
   */
  String authorizationHeader();

  /** Hint that the previous token was rejected (HTTP 401) and must be refreshed. */
  default void invalidate() { /* no-op */ }

  @Override default void close() { /* no-op */ }
}

