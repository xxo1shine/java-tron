package org.tron.core.services.event;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.es.ExecutorServiceManager;
import org.tron.common.log.DiagnosticLog;
import org.tron.common.logsfilter.EventPluginLoader;
import org.tron.common.logsfilter.capsule.BlockLogTriggerCapsule;
import org.tron.common.logsfilter.trigger.BlockLogTrigger;
import org.tron.common.logsfilter.trigger.TransactionLogTrigger;
import org.tron.common.logsfilter.trigger.Trigger;
import org.tron.core.services.event.bo.BlockEvent;
import org.tron.core.services.event.bo.Event;

@Slf4j(topic = "event")
@Component
public class RealtimeEventService {

  private EventPluginLoader instance = EventPluginLoader.getInstance();

  @Getter
  private static Object contractLock = new Object();

  @Autowired
  private SolidEventService solidEventService;

  private static BlockingQueue<Event> queue = new LinkedBlockingQueue<>();

  private static final int BUSY_EVENT_SIZE = 500;

  private final ScheduledExecutorService executor = ExecutorServiceManager
      .newSingleThreadScheduledExecutor("realtime-event");

  public void init() {
    executor.scheduleWithFixedDelay(() -> {
      try {
        work();
      } catch (Exception e) {
        logger.error("Realtime event service fail.", e);
      }
    }, 1, 1, TimeUnit.SECONDS);
    logger.info("Realtime event service start.");
  }

  public void close() {
    try {
      work();
      executor.shutdown();
      logger.info("Realtime event service close.");
    } catch (Exception e) {
      logger.warn("Close realtime event service fail. {}", e.getMessage());
    }
  }

  public void add(Event event) {
    boolean accepted = queue.offer(event);
    int queueSize = queue.size();
    if (accepted && queueSize >= BUSY_EVENT_SIZE
        && DiagnosticLog.shouldLog("realtime_event_retained")) {
      logger.info("@@@ pr=6965 event=realtime_event_retained queueSize={}"
              + " busyThreshold={} removed={}",
          queueSize, BUSY_EVENT_SIZE, event.isRemove());
    }
  }

  public boolean isBusy() {
    return queue.size() >= BUSY_EVENT_SIZE;
  }

  public synchronized void work() {
    while (queue.size() > 0) {
      Event event = queue.poll();
      flush(event.getBlockEvent(), event.isRemove());
    }
  }

  public void flush(BlockEvent blockEvent, boolean isRemove) {
    logger.info("Flush realtime event {}", blockEvent.getBlockId().getString());

    // Post block/transaction triggers synchronously to the plugin (processTrigger ->
    // EventPluginLoader serializes immediately) instead of the async triggerCapsuleQueue: the
    // capsule is a shared cached object whose removed flag is set per-flush, so an async consumer
    // could read it after a later flush overwrote it. This mirrors how contract triggers below
    // are posted directly. isRemove=true re-emits the block/transaction as rolled back on a reorg.
    if (instance.isBlockLogTriggerEnable()
        && !instance.isBlockLogTriggerSolidified()) {
      if (blockEvent.getBlockLogTriggerCapsule() == null) {
        logger.warn("BlockLogTriggerCapsule is null. {}", blockEvent.getBlockId().getString());
      } else {
        BlockLogTriggerCapsule capsule = blockEvent.getBlockLogTriggerCapsule();
        capsule.setRemoved(isRemove);
        BlockLogTrigger trigger = capsule.getBlockLogTrigger();
        boolean actualRemoved = trigger != null && trigger.isRemoved();
        boolean flagMatches = trigger != null && actualRemoved == isRemove;
        if (trigger != null && !flagMatches
            && DiagnosticLog.shouldLog("block_rollback_flag_mismatch")) {
          logger.error("### pr=6833,6965 event=block_rollback_flag_mismatch block={}"
                  + " expected={} actual={}",
              blockEvent.getBlockId().getString(), isRemove, actualRemoved);
        }
        capsule.processTrigger();
        if (flagMatches && DiagnosticLog.shouldLog(isRemove
            ? "rollback_block_dispatch_returned" : "forward_block_dispatch_returned")) {
          logger.info("@@@ pr=6833,6965 event=block_dispatch_returned block={} removed={}",
              blockEvent.getBlockId().getString(), isRemove);
        }
      }
    }

    if (instance.isTransactionLogTriggerEnable()
        && !instance.isTransactionLogTriggerSolidified()) {
      if (blockEvent.getTransactionLogTriggerCapsules() == null) {
        logger.warn("TransactionLogTriggerCapsules is null. {}",
            blockEvent.getBlockId().getString());
      } else {
        blockEvent.getTransactionLogTriggerCapsules().forEach(v -> {
          v.setRemoved(isRemove);
          TransactionLogTrigger trigger = v.getTransactionLogTrigger();
          boolean actualRemoved = trigger != null && trigger.isRemoved();
          boolean flagMatches = trigger != null && actualRemoved == isRemove;
          if (trigger != null && !flagMatches
              && DiagnosticLog.shouldLog("transaction_rollback_flag_mismatch")) {
            logger.error("### pr=6833,6965 event=transaction_rollback_flag_mismatch block={}"
                    + " expected={} actual={}",
                blockEvent.getBlockId().getString(), isRemove, actualRemoved);
          }
          v.processTrigger();
          String diagnosticEvent = isRemove
              ? "rollback_transaction_dispatch_returned" : "forward_transaction_dispatch_returned";
          if (flagMatches && DiagnosticLog.shouldLog(diagnosticEvent)) {
            logger.info("@@@ pr=6833,6965 event=transaction_dispatch_returned block={} removed={}",
                blockEvent.getBlockId().getString(), isRemove);
          }
        });
      }
    }

    synchronized (contractLock) {
      if (instance.isContractEventTriggerEnable()) {
        if (blockEvent.getSmartContractTrigger() == null) {
          logger.warn("SmartContractTrigger is null. {}", blockEvent.getBlockId().getString());
        } else {
          blockEvent.getSmartContractTrigger().getContractEventTriggers().forEach(v -> {
            v.setTriggerName(Trigger.CONTRACTEVENT_TRIGGER_NAME);
            v.setRemoved(isRemove);
            EventPluginLoader.getInstance().postContractEventTrigger(v);
          });
        }
      }

      if (instance.isContractLogTriggerEnable() && blockEvent.getSmartContractTrigger() != null) {
        blockEvent.getSmartContractTrigger().getContractLogTriggers().forEach(v -> {
          v.setTriggerName(Trigger.CONTRACTLOG_TRIGGER_NAME);
          v.setRemoved(isRemove);
          EventPluginLoader.getInstance().postContractLogTrigger(v);
        });
        if (instance.isContractLogTriggerRedundancy()) {
          blockEvent.getSmartContractTrigger().getRedundancies().forEach(v -> {
            v.setTriggerName(Trigger.CONTRACTLOG_TRIGGER_NAME);
            v.setRemoved(isRemove);
            EventPluginLoader.getInstance().postContractLogTrigger(v);
          });
        }
      }
    }
  }

}
