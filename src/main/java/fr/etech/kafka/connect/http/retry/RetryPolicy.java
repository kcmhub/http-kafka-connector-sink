package fr.etech.kafka.connect.http.retry;

import fr.etech.kafka.connect.http.EtechHttpSinkConfig;
import java.util.Set;
import java.util.TreeSet;

/** Decides whether to retry and how long to back off. */
public final class RetryPolicy {

  public enum Decision { GIVE_UP, RETRY }

  private final EtechHttpSinkConfig.RetryMode mode;
  private final int maxRetries;
  private final long initialDelayMs;
  private final long maxDelayMs;
  private final Set<Integer> retryStatusCodes;

  public RetryPolicy(EtechHttpSinkConfig cfg) {
    this.mode = cfg.retryMode();
    this.maxRetries = cfg.maxRetries();
    this.initialDelayMs = cfg.initialDelayMs();
    this.maxDelayMs = cfg.maxDelayMs();
    this.retryStatusCodes = new TreeSet<>(cfg.retryStatusCodes());
  }

  public boolean isRetryableStatus(int statusCode) {
    return retryStatusCodes.contains(statusCode);
  }

  /** @param attempt 0-based attempt index of the *failed* call (so 0 = first failure). */
  public Decision decide(int attempt) {
    if (mode == EtechHttpSinkConfig.RetryMode.NONE) return Decision.GIVE_UP;
    return attempt < maxRetries ? Decision.RETRY : Decision.GIVE_UP;
  }

  public long backoffMs(int attempt) {
    if (mode == EtechHttpSinkConfig.RetryMode.NONE) return 0L;
    long delay = initialDelayMs * (1L << Math.min(attempt, 20));   // 2^attempt, capped to avoid overflow
    return Math.min(delay, maxDelayMs);
  }
}

