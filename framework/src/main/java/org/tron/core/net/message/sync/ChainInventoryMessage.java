package org.tron.core.net.message.sync;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.log.DiagnosticLog;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.net.message.MessageTypes;
import org.tron.core.net.message.TronMessage;
import org.tron.p2p.utils.ProtoUtil;
import org.tron.protos.Protocol;
import org.tron.protos.Protocol.ChainInventory;

@Slf4j(topic = "net")
public class ChainInventoryMessage extends TronMessage {

  protected ChainInventory chainInventory;

  public ChainInventoryMessage(byte[] data) throws Exception {
    super(data);
    this.type = MessageTypes.BLOCK_CHAIN_INVENTORY.asByte();
    chainInventory = ProtoUtil.parseFrom(Protocol.ChainInventory.parser(), data);
    int unknownFields = chainInventory.getUnknownFields().asMap().size();
    if (unknownFields != 0) {
      if (DiagnosticLog.shouldLog("pr7048.chain_inventory_unknown_fields_retained")) {
        logger.error("### pr=7048 event=unknown_fields_retained type=chain_inventory count={}",
            unknownFields);
      }
    } else if (DiagnosticLog.shouldLog("pr7048.chain_inventory_proto_compacted")
        && data.length > chainInventory.getSerializedSize()) {
      logger.info("@@@ pr=7048 event=transient_proto_compacted type=chain_inventory "
          + "wireBytes={} protoBytes={} unknownFields=0", data.length,
          chainInventory.getSerializedSize());
    }
  }

  public ChainInventoryMessage(List<BlockId> blockIds, Long remainNum) {
    ChainInventory.Builder invBuilder = ChainInventory.newBuilder();
    blockIds.forEach(blockId -> {
      ChainInventory.BlockId.Builder b = ChainInventory.BlockId.newBuilder();
      b.setHash(blockId.getByteString());
      b.setNumber(blockId.getNum());
      invBuilder.addIds(b);
    });

    invBuilder.setRemainNum(remainNum);
    chainInventory = invBuilder.build();
    this.type = MessageTypes.BLOCK_CHAIN_INVENTORY.asByte();
    this.data = chainInventory.toByteArray();
  }

  @Override
  public Class<?> getAnswerMessage() {
    return null;
  }

  private ChainInventory getChainInventory() {
    return chainInventory;
  }

  public List<BlockId> getBlockIds() {

    try {
      return getChainInventory().getIdsList().stream()
          .map(blockId -> new BlockId(blockId.getHash(), blockId.getNumber()))
          .collect(Collectors.toCollection(ArrayList::new));
    } catch (Exception e) {
      logger.info("Failed to get blockIds of chain inventory message");
    }
    return null;
  }

  public Long getRemainNum() {
    return getChainInventory().getRemainNum();
  }

  @Override
  public String toString() {
    Deque<BlockId> blockIdWeGet = new LinkedList<>(getBlockIds());
    StringBuilder sb = new StringBuilder(super.toString());
    int size = blockIdWeGet.size();
    sb.append("size: ").append(size);
    if (size >= 1) {
      sb.append(", first blockId: ").append(blockIdWeGet.peek().getString());
      if (size > 1) {
        sb.append(", end blockId: ").append(blockIdWeGet.peekLast().getString());
      }
    }
    sb.append(", remain_num: ").append(chainInventory.getRemainNum());
    return sb.toString();
  }
}
