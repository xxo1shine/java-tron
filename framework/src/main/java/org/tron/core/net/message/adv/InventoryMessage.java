package org.tron.core.net.message.adv;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.log.DiagnosticLog;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.net.message.MessageTypes;
import org.tron.core.net.message.TronMessage;
import org.tron.p2p.utils.ProtoUtil;
import org.tron.protos.Protocol;
import org.tron.protos.Protocol.Inventory;
import org.tron.protos.Protocol.Inventory.InventoryType;


@Slf4j(topic = "net")
public class InventoryMessage extends TronMessage {

  protected Inventory inv;

  public InventoryMessage(byte[] data) throws Exception {
    super(data);
    this.type = MessageTypes.INVENTORY.asByte();
    this.inv = ProtoUtil.parseFrom(Protocol.Inventory.parser(), data);
    int unknownFields = inv.getUnknownFields().asMap().size();
    if (unknownFields != 0) {
      if (DiagnosticLog.shouldLog("pr7048.inventory_unknown_fields_retained")) {
        logger.error("### pr=7048 event=unknown_fields_retained type=inventory count={}",
            unknownFields);
      }
    } else if (DiagnosticLog.shouldLog("pr7048.inventory_proto_compacted")
        && data.length > inv.getSerializedSize()) {
      logger.info("@@@ pr=7048 event=transient_proto_compacted type=inventory "
          + "wireBytes={} protoBytes={} unknownFields=0", data.length,
          inv.getSerializedSize());
    }
  }

  public InventoryMessage(Inventory inv) {
    this.inv = inv;
    this.type = MessageTypes.INVENTORY.asByte();
    this.data = inv.toByteArray();
  }

  public InventoryMessage(List<Sha256Hash> hashList, InventoryType type) {
    Inventory.Builder invBuilder = Inventory.newBuilder();

    for (Sha256Hash hash :
        hashList) {
      invBuilder.addIds(hash.getByteString());
    }
    invBuilder.setType(type);
    inv = invBuilder.build();
    this.type = MessageTypes.INVENTORY.asByte();
    this.data = inv.toByteArray();
  }

  @Override
  public Class<?> getAnswerMessage() {
    return null;
  }

  public Inventory getInventory() {
    return inv;
  }

  public MessageTypes getInvMessageType() {
    return getInventoryType().equals(InventoryType.BLOCK) ? MessageTypes.BLOCK : MessageTypes.TRX;

  }

  public InventoryType getInventoryType() {
    return inv.getType();
  }

  @Override
  public String toString() {
    Deque<Sha256Hash> hashes = new LinkedList<>(getHashList());
    StringBuilder builder = new StringBuilder();
    builder.append(super.toString()).append("invType: ").append(getInvMessageType())
        .append(", size: ").append(hashes.size())
        .append(", First hash: ").append(hashes.peekFirst());
    if (hashes.size() > 1) {
      builder.append(", End hash: ").append(hashes.peekLast());
    }
    return builder.toString();
  }

  public List<Sha256Hash> getHashList() {
    return getInventory().getIdsList().stream()
        .map(hash -> Sha256Hash.wrap(hash.toByteArray()))
        .collect(Collectors.toList());
  }

}
