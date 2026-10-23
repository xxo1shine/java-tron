package org.tron.core.net.messagehandler;

import static org.tron.core.config.Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.common.log.DiagnosticLog;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.config.Parameter.ChainConstant;
import org.tron.core.config.Parameter.NetConstants;
import org.tron.core.config.args.Args;
import org.tron.core.exception.P2pException;
import org.tron.core.exception.P2pException.TypeEnum;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.message.TronMessage;
import org.tron.core.net.message.sync.ChainInventoryMessage;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.peer.TronState;
import org.tron.core.net.service.sync.SyncService;

@Slf4j(topic = "net")
@Component
public class ChainInventoryMsgHandler implements TronMsgHandler {

  @Autowired
  private TronNetDelegate tronNetDelegate;

  @Autowired
  private SyncService syncService;

  private final long syncFetchBatchNum = Args.getInstance().getSyncFetchBatchNum();

  @Override
  public void processMessage(PeerConnection peer, TronMessage msg) throws P2pException {

    ChainInventoryMessage chainInventoryMessage = (ChainInventoryMessage) msg;

    check(peer, chainInventoryMessage);

    peer.setFetchAble(false);

    peer.setNeedSyncFromPeer(true);

    peer.setSyncChainRequested(null);

    Deque<BlockId> blockIdWeGet = new LinkedList<>(chainInventoryMessage.getBlockIds());

    if (blockIdWeGet.size() == 1 && tronNetDelegate.containBlock(blockIdWeGet.peek())) {
      peer.setTronState(TronState.SYNC_COMPLETED);
      peer.setNeedSyncFromPeer(false);
      return;
    }

    while (!peer.getSyncBlockToFetch().isEmpty()) {
      if (blockIdWeGet.peekFirst().equals(peer.getSyncBlockToFetch().peekLast())) {
        break;
      }
      peer.getSyncBlockToFetch().pollLast();
    }

    blockIdWeGet.poll();

    peer.setRemainNum(chainInventoryMessage.getRemainNum());
    peer.getSyncBlockToFetch().addAll(blockIdWeGet);

    synchronized (tronNetDelegate.getBlockLock()) {
      Deque<BlockId> toFetch = peer.getSyncBlockToFetch();
      BlockId blockId = null;
      BlockId next;
      while ((next = toFetch.peek()) != null && tronNetDelegate.containBlock(next)) {
        if (toFetch.remove(next)) {
          blockId = next;
          peer.setBlockBothHave(blockId);
          if (DiagnosticLog.shouldLog("pr6983.known_sync_block_removed")) {
            logger.info("@@@ pr=6983 event=known_sync_block_removed"
                + " block={}",
                blockId);
          }
        }
      }
      if (blockId != null) {
        logger.info("Block {} from {} is processed", blockId.getString(), peer.getInetAddress());
      }

      peer.setFetchAble(true);
      if ((chainInventoryMessage.getRemainNum() == 0 && !peer.getSyncBlockToFetch().isEmpty())
          || (chainInventoryMessage.getRemainNum() != 0
          && peer.getSyncBlockToFetch().size() > syncFetchBatchNum)) {
        syncService.setFetchFlag(true);
        if (DiagnosticLog.shouldLog("pr6970.chain_inventory_fetch_scheduled")) {
          logger.info("@@@ pr=6970 event=chain_inventory_fetch_scheduled"
              + " remaining={}",
              chainInventoryMessage.getRemainNum());
        }
      } else {
        syncService.syncNext(peer);
        if (DiagnosticLog.shouldLog("pr6970.chain_inventory_summary_path_selected")) {
          logger.info("@@@ pr=6970 event=chain_inventory_summary_path_selected"
              + " remaining={}",
              chainInventoryMessage.getRemainNum());
        }
      }
    }
  }

  private void check(PeerConnection peer, ChainInventoryMessage msg) throws P2pException {
    if (peer.getSyncChainRequested() == null) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "not send syncBlockChainMsg");
    }

    List<BlockId> blockIds = msg.getBlockIds();
    if (CollectionUtils.isEmpty(blockIds)) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "blockIds is empty");
    }

    if (msg.getRemainNum() < 0) {
      if (DiagnosticLog.shouldLog("pr6966.negative_sync_remaining_rejected")) {
        logger.info("@@@ pr=6966 event=negative_sync_remaining_rejected"
            + " remaining={}",
            msg.getRemainNum());
      }
      throw new P2pException(TypeEnum.BAD_MESSAGE,
          "remainNum is negative: " + msg.getRemainNum());
    }

    if (blockIds.size() > NetConstants.SYNC_FETCH_BATCH_NUM + 1) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "big blockIds size: " + blockIds.size());
    }

    if (msg.getRemainNum() != 0 && blockIds.size() < NetConstants.SYNC_FETCH_BATCH_NUM) {
      throw new P2pException(TypeEnum.BAD_MESSAGE,
          "remain: " + msg.getRemainNum() + ", blockIds size: " + blockIds.size());
    }

    long num = blockIds.get(0).getNum();
    for (BlockId id : msg.getBlockIds()) {
      if (id.getNum() != num++) {
        throw new P2pException(TypeEnum.BAD_MESSAGE, "not continuous block");
      }
    }

    if (!peer.getSyncChainRequested().getKey().contains(blockIds.get(0))) {
      throw new P2pException(TypeEnum.BAD_MESSAGE, "unlinked block, my head: "
          + peer.getSyncChainRequested().getKey().getLast().getString()
          + ", peer: " + blockIds.get(0).getString());
    }

    if (tronNetDelegate.getHeadBlockId().getNum() > 0) {
      long maxRemainTime =
          ChainConstant.CLOCK_MAX_DELAY + System.currentTimeMillis() - tronNetDelegate
              .getBlockTime(tronNetDelegate.getSolidBlockId());
      long maxFutureNum =
          maxRemainTime / BLOCK_PRODUCED_INTERVAL + tronNetDelegate.getSolidBlockId().getNum();
      long lastNum = blockIds.get(blockIds.size() - 1).getNum();
      long declaredHighestNum = lastNum + msg.getRemainNum();
      if (declaredHighestNum < 0 || declaredHighestNum > maxFutureNum) {
        if (DiagnosticLog.shouldLog("pr6966.invalid_sync_tip_rejected")) {
          logger.info("@@@ pr=6966 event=invalid_sync_tip_rejected"
              + " declared={} last={} remaining={} max={}",
              declaredHighestNum,
              lastNum,
              msg.getRemainNum(),
              maxFutureNum);
        }
        throw new P2pException(TypeEnum.BAD_MESSAGE,
            "Invalid declared highest block number: " + declaredHighestNum
                + ", lastNum: " + lastNum + ", remainNum: " + msg.getRemainNum()
                + ", futureMaxNum: " + maxFutureNum);
      }
    }
  }

}
