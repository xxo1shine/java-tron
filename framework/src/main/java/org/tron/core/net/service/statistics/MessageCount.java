package org.tron.core.net.service.statistics;

import lombok.extern.slf4j.Slf4j;
import org.tron.common.log.DiagnosticLog;

@Slf4j(topic = "net")
public class MessageCount {

  private static int SIZE = 60;

  private int[] szCount = new int[SIZE];

  private long indexTime = System.currentTimeMillis() / 1000;

  private int index = (int) (indexTime % SIZE);

  private long totalCount = 0;

  private synchronized void update() {
    long time = System.currentTimeMillis() / 1000;
    long gap = time - indexTime;
    int k = gap > SIZE ? SIZE : (int) gap;
    if (k > 0) {
      for (int i = 1; i <= k; i++) {
        szCount[(index + i) % SIZE] = 0;
      }
      index = (int) (time % SIZE);
      indexTime = time;
    }
  }

  public synchronized void add() {
    update();
    szCount[index]++;
    totalCount++;
    if (DiagnosticLog.shouldLog("pr6970.synchronized_message_count_updated")) {
      logger.info("@@@ pr=6970 event=synchronized_message_count_updated"
          + " delta={} index={} total={}",
          1,
          index,
          totalCount);
    }
  }

  public synchronized void add(int count) {
    update();
    szCount[index] += count;
    totalCount += count;
    if (count != 0
        && DiagnosticLog.shouldLog("pr6970.synchronized_message_count_updated")) {
      logger.info("@@@ pr=6970 event=synchronized_message_count_updated"
          + " delta={} index={} total={}",
          count,
          index,
          totalCount);
    }
  }

  public synchronized int getCount(int interval) {
    if (interval > SIZE) {
      logger.warn("Param interval({}) is gt SIZE({})", interval, SIZE);
      return 0;
    }
    update();
    int count = 0;
    for (int i = 0; i < interval; i++) {
      count += szCount[(SIZE + index - i) % SIZE];
    }
    return count;
  }

  public synchronized long getTotalCount() {
    return totalCount;
  }

  public synchronized void reset() {
    totalCount = 0;
  }

  @Override
  public synchronized String toString() {
    return String.valueOf(totalCount);
  }

}
