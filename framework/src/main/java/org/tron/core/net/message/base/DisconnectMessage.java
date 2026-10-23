package org.tron.core.net.message.base;

import lombok.extern.slf4j.Slf4j;
import org.tron.common.log.DiagnosticLog;
import org.tron.core.net.message.MessageTypes;
import org.tron.core.net.message.TronMessage;
import org.tron.p2p.utils.ProtoUtil;
import org.tron.protos.Protocol;
import org.tron.protos.Protocol.ReasonCode;

@Slf4j(topic = "net")
public class DisconnectMessage extends TronMessage {

  private Protocol.DisconnectMessage disconnectMessage;

  public DisconnectMessage(byte[] data) throws Exception {
    super(MessageTypes.P2P_DISCONNECT.asByte(), data);
    this.disconnectMessage = ProtoUtil.parseFrom(Protocol.DisconnectMessage.parser(), data);
    int unknownFields = disconnectMessage.getUnknownFields().asMap().size();
    if (unknownFields != 0) {
      if (DiagnosticLog.shouldLog("pr7048.disconnect_unknown_fields_retained")) {
        logger.error("### pr=7048 event=unknown_fields_retained type=disconnect count={}",
            unknownFields);
      }
    } else if (data.length > disconnectMessage.getSerializedSize()
        && DiagnosticLog.shouldLog("pr7048.disconnect_proto_compacted")) {
      logger.info("@@@ pr=7048 event=transient_proto_compacted type=disconnect "
          + "wireBytes={} protoBytes={} unknownFields=0", data.length,
          disconnectMessage.getSerializedSize());
    }
  }

  public DisconnectMessage(ReasonCode reasonCode) {
    this.disconnectMessage = Protocol.DisconnectMessage
        .newBuilder()
        .setReason(reasonCode)
        .build();
    this.type = MessageTypes.P2P_DISCONNECT.asByte();
    this.data = this.disconnectMessage.toByteArray();
  }

  public ReasonCode getReason() {
    return this.disconnectMessage.getReason();
  }

  public ReasonCode getReasonCode() {
    return disconnectMessage.getReason();
  }

  @Override
  public String toString() {
    return new StringBuilder().append(super.toString()).append("reason: ")
        .append(this.disconnectMessage.getReason()).toString();
  }

  @Override
  public Class<?> getAnswerMessage() {
    return null;
  }
}
