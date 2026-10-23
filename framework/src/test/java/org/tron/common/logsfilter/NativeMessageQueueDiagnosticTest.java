package org.tron.common.logsfilter;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;
import org.tron.common.log.DiagnosticLog;
import org.tron.common.logsfilter.nativequeue.NativeMessageQueue;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class NativeMessageQueueDiagnosticTest {

  @Test
  public void matchingSocketLimitReportsApplied() {
    assertQueueDiagnostic(1000, false, "@@@ pr=6965 event=publisher_hwm_applied");
  }

  @Test
  public void mismatchedSocketLimitReportsInvariantWithoutChangingBindResult() {
    assertQueueDiagnostic(0, false, "### pr=6965 event=publisher_hwm_mismatch");
  }

  @Test
  public void unavailableSocketInspectionDoesNotFailStartupOrReportImplementationError() {
    assertQueueDiagnostic(0, true, null);
  }

  private void assertQueueDiagnostic(int actualLimit, boolean getterFails, String expected) {
    ZMQ.Socket publisher = mock(ZMQ.Socket.class);
    when(publisher.bind(anyString())).thenReturn(true);
    if (getterFails) {
      when(publisher.getSndHWM()).thenThrow(new IllegalStateException("inspection unavailable"));
    } else {
      when(publisher.getSndHWM()).thenReturn(actualLimit);
    }
    Logger logger = (Logger) LoggerFactory.getLogger(NativeMessageQueue.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    NativeMessageQueue queue = new NativeMessageQueue();
    try (MockedConstruction<ZContext> contexts = mockConstruction(ZContext.class,
        (context, construction) -> when(context.createSocket(SocketType.PUB))
            .thenReturn(publisher));
        MockedStatic<DiagnosticLog> diagnostic = mockStatic(DiagnosticLog.class)) {
      diagnostic.when(() -> DiagnosticLog.shouldLog(anyString())).thenReturn(true);
      Assert.assertTrue(queue.start(6000, 1000));
      Assert.assertEquals(1, contexts.constructed().size());
      if (expected == null) {
        Assert.assertFalse(appender.list.stream().anyMatch(event ->
            event.getFormattedMessage().startsWith("###")
                || event.getFormattedMessage().startsWith("@@@")));
      } else {
        Assert.assertTrue(appender.list.stream().anyMatch(event ->
            event.getFormattedMessage().startsWith(expected)));
        Assert.assertEquals(1, appender.list.stream().filter(event ->
            event.getFormattedMessage().startsWith("###")
                || event.getFormattedMessage().startsWith("@@@")).count());
      }
    } finally {
      queue.stop();
      logger.detachAppender(appender);
      appender.stop();
    }
  }
}
