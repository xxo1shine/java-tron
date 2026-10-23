package org.tron.common.log;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;

/** Bounds diagnostic output without retaining blocks, transactions or peers. */
public final class DiagnosticLog {

  private static final Cache<String, Boolean> EVENTS = CacheBuilder.newBuilder()
      .maximumSize(256)
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .build();

  private DiagnosticLog() {
  }

  /** Use a fixed event name, never an address, block ID or other input-dependent key. */
  public static boolean shouldLog(String event) {
    return EVENTS.asMap().putIfAbsent(event, Boolean.TRUE) == null;
  }
}
