package org.tron.common.backup.message;

import static org.tron.common.backup.message.UdpMessageTypeEnum.BACKUP_KEEP_ALIVE;

import lombok.extern.slf4j.Slf4j;
import org.tron.common.log.DiagnosticLog;
import org.tron.p2p.discover.Node;
import org.tron.p2p.utils.ProtoUtil;
import org.tron.protos.Discover;

@Slf4j(topic = "net")
public class KeepAliveMessage extends Message {

  private Discover.BackupMessage backupMessage;

  public KeepAliveMessage(byte[] data) throws Exception {
    super(BACKUP_KEEP_ALIVE, data);
    backupMessage = ProtoUtil.parseFrom(Discover.BackupMessage.parser(), data);
    int unknownFields = backupMessage.getUnknownFields().asMap().size();
    if (unknownFields != 0) {
      if (DiagnosticLog.shouldLog("pr7048.backup_unknown_fields_retained")) {
        logger.error("### pr=7048 event=unknown_fields_retained type=backup count={}",
            unknownFields);
      }
    } else if (data.length > backupMessage.getSerializedSize()
        && DiagnosticLog.shouldLog("pr7048.backup_proto_compacted")) {
      logger.info("@@@ pr=7048 event=transient_proto_compacted type=backup "
          + "wireBytes={} protoBytes={} unknownFields=0", data.length,
          backupMessage.getSerializedSize());
    }
  }

  public KeepAliveMessage(boolean flag, int priority) {
    super(BACKUP_KEEP_ALIVE, null);
    backupMessage = Discover.BackupMessage.newBuilder().setFlag(flag).setPriority(priority).build();
    data = backupMessage.toByteArray();
  }

  public boolean getFlag() {
    return backupMessage.getFlag();
  }

  public int getPriority() {
    return backupMessage.getPriority();
  }

  @Override
  public long getTimestamp() {
    return 0;
  }

  @Override
  public Node getFrom() {
    return null;
  }
}
