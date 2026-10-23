package org.tron.core.net.message.sync;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.log.DiagnosticLog;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.net.message.MessageTypes;
import org.tron.core.net.message.TronMessage;
import org.tron.p2p.utils.ProtoUtil;
import org.tron.protos.Protocol;
import org.tron.protos.Protocol.BlockInventory;

@Slf4j(topic = "net")
public class BlockInventoryMessage extends TronMessage {

  protected BlockInventory blockInventory;

  public BlockInventoryMessage(byte[] data) throws Exception {
    super(data);
    this.type = MessageTypes.BLOCK_INVENTORY.asByte();
    this.blockInventory = ProtoUtil.parseFrom(Protocol.BlockInventory.parser(), data);
    int unknownFields = blockInventory.getUnknownFields().asMap().size();
    if (unknownFields != 0) {
      if (DiagnosticLog.shouldLog("pr7048.block_inventory_unknown_fields_retained")) {
        logger.error("### pr=7048 event=unknown_fields_retained type=block_inventory count={}",
            unknownFields);
      }
    } else if (DiagnosticLog.shouldLog("pr7048.block_inventory_proto_compacted")
        && data.length > blockInventory.getSerializedSize()) {
      logger.info("@@@ pr=7048 event=transient_proto_compacted type=block_inventory "
          + "wireBytes={} protoBytes={} unknownFields=0", data.length,
          blockInventory.getSerializedSize());
    }
  }

  public BlockInventoryMessage(List<BlockId> blockIds, BlockInventory.Type type) {
    BlockInventory.Builder invBuilder = BlockInventory.newBuilder();
    blockIds.forEach(blockId -> {
      BlockInventory.BlockId.Builder b = BlockInventory.BlockId.newBuilder();
      b.setHash(blockId.getByteString());
      b.setNumber(blockId.getNum());
      invBuilder.addIds(b);
    });

    invBuilder.setType(type);
    blockInventory = invBuilder.build();
    this.type = MessageTypes.BLOCK_INVENTORY.asByte();
    this.data = blockInventory.toByteArray();
  }

  @Override
  public Class<?> getAnswerMessage() {
    return null;
  }

  private BlockInventory getBlockInventory() {
    return blockInventory;
  }

  public List<BlockId> getBlockIds() {
    return getBlockInventory().getIdsList().stream()
        .map(blockId -> new BlockId(blockId.getHash(), blockId.getNumber()))
        .collect(Collectors.toCollection(ArrayList::new));
  }

}
