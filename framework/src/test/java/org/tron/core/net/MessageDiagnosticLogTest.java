package org.tron.core.net;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.cache.Cache;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnknownFieldSet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.common.log.DiagnosticLog;
import org.tron.core.net.message.adv.InventoryMessage;
import org.tron.protos.Protocol;

public class MessageDiagnosticLogTest {

  @Test
  public void reportsActualCompactionWithoutCallingHostilePaddingARegression() throws Exception {
    Cache<?, ?> events = (Cache<?, ?>) ReflectionTestUtils.getField(DiagnosticLog.class, "EVENTS");
    Assert.assertNotNull(events);
    events.invalidate("pr7048.inventory_proto_compacted");
    Logger logger = (Logger) LoggerFactory.getLogger("net");
    Level previousLevel = logger.getLevel();
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      Protocol.Inventory clean = Protocol.Inventory.newBuilder()
          .setType(Protocol.Inventory.InventoryType.BLOCK)
          .addIds(ByteString.copyFrom(new byte[32])).build();
      new InventoryMessage(clean.toByteArray());
      Assert.assertTrue(appender.list.isEmpty());
      // Start a new observation window without waiting for the diagnostic sampling interval.
      events.invalidate("pr7048.inventory_proto_compacted");

      Protocol.Inventory padded = clean.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder()
          .addField(1000, UnknownFieldSet.Field.newBuilder()
              .addLengthDelimited(ByteString.copyFrom(new byte[8192])).build()).build()).build();
      InventoryMessage parsed = new InventoryMessage(padded.toByteArray());
      Assert.assertEquals(clean, parsed.getInventory());
      Assert.assertEquals(1, appender.list.size());
      String message = appender.list.get(0).getFormattedMessage();
      Assert.assertTrue(message.startsWith("@@@ pr=7048 event=transient_proto_compacted"));
      Assert.assertTrue(message.contains("wireBytes=" + padded.getSerializedSize()));
      Assert.assertTrue(message.contains("protoBytes=" + clean.getSerializedSize()));

      new InventoryMessage(padded.toByteArray());
      Assert.assertEquals("Repeated padding must not flood diagnostics", 1, appender.list.size());
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previousLevel);
      appender.stop();
      events.invalidate("pr7048.inventory_proto_compacted");
    }
  }
}
