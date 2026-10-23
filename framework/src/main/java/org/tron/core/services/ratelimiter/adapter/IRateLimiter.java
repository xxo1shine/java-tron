package org.tron.core.services.ratelimiter.adapter;

import org.slf4j.LoggerFactory;
import org.tron.common.log.DiagnosticLog;
import org.tron.core.config.args.Args;
import org.tron.core.services.ratelimiter.RuntimeData;

public interface IRateLimiter {

  boolean tryAcquire(RuntimeData data);

  boolean acquire(RuntimeData data);

  default boolean acquirePermit(RuntimeData data) {
    boolean nonBlocking = Args.getInstance().isRateLimiterApiNonBlocking();
    boolean acquired = nonBlocking ? tryAcquire(data) : acquire(data);
    String event = nonBlocking
        ? (acquired ? "endpoint_nonblocking_permit_granted" : "endpoint_nonblocking_permit_denied")
        : (acquired ? "endpoint_blocking_permit_granted" : "endpoint_blocking_permit_denied");
    if (DiagnosticLog.shouldLog(event)) {
      LoggerFactory.getLogger(IRateLimiter.class)
          .info("@@@ pr=6761,6733 event={} adapter={}", event, getClass().getSimpleName());
    }
    return acquired;
  }
}
