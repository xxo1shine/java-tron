package org.tron.p2p.connection.message.handshake;

import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.tron.p2p.base.Parameter;
import org.tron.p2p.connection.business.handshake.DisconnectCode;
import org.tron.p2p.connection.message.Message;
import org.tron.p2p.connection.message.MessageType;
import org.tron.p2p.discover.Node;
import org.tron.p2p.protos.Connect;
import org.tron.p2p.protos.Discover;
import org.tron.p2p.utils.NetUtil;
import org.tron.p2p.utils.ProtoUtil;

@Slf4j(topic = "net")
public class HelloMessage extends Message {

  private static final AtomicBoolean COMPACTION_LOGGED = new AtomicBoolean();
  private static final AtomicBoolean RETENTION_FAILURE_LOGGED = new AtomicBoolean();

  private Connect.HelloMessage helloMessage;

  public HelloMessage(byte[] data) throws Exception {
    super(MessageType.HANDSHAKE_HELLO, data);
    this.helloMessage = ProtoUtil.parseFrom(Connect.HelloMessage.parser(), data);
    this.data = this.helloMessage.toByteArray();
    if (!helloMessage.getUnknownFields().asMap().isEmpty()
        || this.data.length != helloMessage.getSerializedSize()) {
      if (RETENTION_FAILURE_LOGGED.compareAndSet(false, true)) {
        logger.error("### pr=7048 event=p2p_hello_retention_invariant_failed "
            + "retainedBytes={} protoBytes={} unknownFields={}", this.data.length,
            helloMessage.getSerializedSize(), helloMessage.getUnknownFields().asMap().size());
      }
    } else if (data.length > this.data.length && COMPACTION_LOGGED.compareAndSet(false, true)) {
      logger.info("@@@ pr=7048 event=p2p_hello_payload_compacted wireBytes={} retainedBytes={}",
          data.length, this.data.length);
    }
  }

  public HelloMessage(DisconnectCode code, long time) {
    super(MessageType.HANDSHAKE_HELLO, null);
    Discover.Endpoint endpoint = Parameter.getHomeNode();
    this.helloMessage = Connect.HelloMessage.newBuilder()
      .setFrom(endpoint)
      .setNetworkId(Parameter.p2pConfig.getNetworkId())
      .setCode(code.getValue())
      .setVersion(Parameter.version)
      .setTimestamp(time).build();
    this.data = helloMessage.toByteArray();
  }

  public int getNetworkId() {
    return this.helloMessage.getNetworkId();
  }

  public int getVersion() {
    return this.helloMessage.getVersion();
  }

  public int getCode() {
    return this.helloMessage.getCode();
  }

  public long getTimestamp() {
    return this.helloMessage.getTimestamp();
  }

  public Node getFrom() {
    return NetUtil.getNode(helloMessage.getFrom());
  }

  @Override
  public String toString() {
    return "HelloMessage networkId: " + getNetworkId()
        + ", version: " + getVersion()
        + ", code: " + getCode();
  }

  @Override
  public boolean valid() {
    return NetUtil.validNode(getFrom());
  }
}
