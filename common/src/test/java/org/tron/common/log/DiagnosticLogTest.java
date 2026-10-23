package org.tron.common.log;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class DiagnosticLogTest {

  @Test
  public void suppressesRepeatedEventsWithoutSuppressingOtherEvents() {
    String event = UUID.randomUUID().toString();
    Assert.assertTrue(DiagnosticLog.shouldLog(event));
    Assert.assertFalse(DiagnosticLog.shouldLog(event));
    Assert.assertTrue(DiagnosticLog.shouldLog(event + ".failure"));
  }

  @Test
  public void admitsOnlyOneConcurrentOccurrence() throws Exception {
    String event = UUID.randomUUID().toString();
    ExecutorService executor = Executors.newFixedThreadPool(4,
        new ThreadFactoryBuilder().setNameFormat("diagnostic-log-test-%d").build());
    try {
      List<Callable<Boolean>> attempts = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        attempts.add(() -> DiagnosticLog.shouldLog(event));
      }
      int admitted = 0;
      for (Future<Boolean> result : executor.invokeAll(attempts)) {
        if (result.get()) {
          admitted++;
        }
      }
      Assert.assertEquals(1, admitted);
    } finally {
      executor.shutdown();
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    }
  }
}
