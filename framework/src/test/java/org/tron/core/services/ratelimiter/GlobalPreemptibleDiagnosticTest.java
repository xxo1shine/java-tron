package org.tron.core.services.ratelimiter;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;
import org.tron.common.log.DiagnosticLog;
import org.tron.core.services.ratelimiter.strategy.GlobalPreemptibleStrategy;

public class GlobalPreemptibleDiagnosticTest {

  @Test
  public void normalRejectionAndBalancedReleaseDoNotReportInvariantFailure() {
    assertPermitDiagnostic(false);
  }

  @Test
  public void excessReleaseReportsInvariantWithoutChangingSemaphoreBehavior() {
    assertPermitDiagnostic(true);
  }

  private void assertPermitDiagnostic(boolean excessRelease) {
    Logger logger = (Logger) LoggerFactory.getLogger(GlobalPreemptibleStrategy.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try (MockedStatic<DiagnosticLog> diagnostic = mockStatic(DiagnosticLog.class)) {
      diagnostic.when(() -> DiagnosticLog.shouldLog(anyString())).thenReturn(true);
      GlobalPreemptibleStrategy strategy = new GlobalPreemptibleStrategy("permit=1");
      Assert.assertTrue(strategy.tryAcquire());
      Assert.assertFalse(strategy.tryAcquire());
      strategy.release();
      if (excessRelease) {
        strategy.release();
      }
      Assert.assertEquals(excessRelease, appender.list.stream().anyMatch(event ->
          event.getFormattedMessage().startsWith("### pr=6733 event=semaphore_over_release")));
      Assert.assertTrue(strategy.tryAcquire());
      Assert.assertEquals(excessRelease, strategy.tryAcquire());
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
  }
}
