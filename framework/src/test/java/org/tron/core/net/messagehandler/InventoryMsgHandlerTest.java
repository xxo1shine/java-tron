package org.tron.core.net.messagehandler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.tron.common.TestConstants;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.capsule.BlockCapsule.BlockId;
import org.tron.core.config.args.Args;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.message.adv.InventoryMessage;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.service.adv.AdvService;
import org.tron.p2p.connection.Channel;
import org.tron.protos.Protocol.Inventory.InventoryType;

public class InventoryMsgHandlerTest {

  @Test
  public void testProcessMessage() throws Exception {
    InventoryMsgHandler handler = new InventoryMsgHandler();
    Args.setParam(new String[] {}, TestConstants.TEST_CONF);
    Args.logConfig();

    InventoryMessage msg = new InventoryMessage(new ArrayList<>(), InventoryType.TRX);
    PeerConnection peer = new PeerConnection();
    peer.setChannel(getChannel("1.0.0.3", 1000));
    peer.setNeedSyncFromPeer(true);
    peer.setNeedSyncFromUs(true);
    handler.processMessage(peer, msg);

    peer.setNeedSyncFromPeer(true);
    peer.setNeedSyncFromUs(false);
    handler.processMessage(peer, msg);

    peer.setNeedSyncFromPeer(false);
    peer.setNeedSyncFromUs(true);
    handler.processMessage(peer, msg);

    peer.setNeedSyncFromUs(false);

    TronNetDelegate tronNetDelegate = mock(TronNetDelegate.class);
    Mockito.when(tronNetDelegate.isBlockUnsolidified()).thenReturn(true);

    Field field = handler.getClass().getDeclaredField("tronNetDelegate");
    field.setAccessible(true);
    field.set(handler, tronNetDelegate);

    handler.processMessage(peer, msg);
  }

  @Test
  public void testLastInteractiveTimeNotUpdatedForSolidifiedBlock() throws Exception {
    InventoryMsgHandler handler = new InventoryMsgHandler();
    Args.setParam(new String[]{}, TestConstants.TEST_CONF);

    TronNetDelegate tronNetDelegate = mock(TronNetDelegate.class);
    AdvService advService = mock(AdvService.class);
    Mockito.when(advService.addInv(any())).thenReturn(true);
    // block num 100 is at head boundary — should NOT update
    Mockito.when(tronNetDelegate.getHeadBlockId())
        .thenReturn(new BlockId(Sha256Hash.ZERO_HASH, 100L));

    Field delegateField = handler.getClass().getDeclaredField("tronNetDelegate");
    delegateField.setAccessible(true);
    delegateField.set(handler, tronNetDelegate);
    Field advField = handler.getClass().getDeclaredField("advService");
    advField.setAccessible(true);
    advField.set(handler, advService);

    PeerConnection peer = new PeerConnection();
    peer.setChannel(getChannel("1.0.0.4", 1001));
    peer.setNeedSyncFromPeer(false);
    peer.setNeedSyncFromUs(false);
    peer.setLastInteractiveTime(0L);

    // Block hash encodes num=100 (at solidified boundary — should NOT update)
    Sha256Hash blockHash = new BlockId(Sha256Hash.ZERO_HASH, 100L);
    InventoryMessage msg = new InventoryMessage(Arrays.asList(blockHash), InventoryType.BLOCK);
    handler.processMessage(peer, msg);

    Assert.assertEquals("lastInteractiveTime should NOT be updated for solidified block",
        0L, peer.getLastInteractiveTime());
  }

  @Test
  public void testLastInteractiveTimeUpdatedForBothPeersWithSameAboveSolidifiedBlock()
      throws Exception {
    InventoryMsgHandler handler = new InventoryMsgHandler();
    Args.setParam(new String[]{}, TestConstants.TEST_CONF);

    TronNetDelegate tronNetDelegate = mock(TronNetDelegate.class);
    AdvService advService = mock(AdvService.class);
    // First call returns true (peer1), second call returns false (peer2 — already in cache)
    Mockito.when(advService.addInv(any())).thenReturn(true).thenReturn(false);
    Mockito.when(tronNetDelegate.getHeadBlockId())
        .thenReturn(new BlockId(Sha256Hash.ZERO_HASH, 99L));

    Field delegateField = handler.getClass().getDeclaredField("tronNetDelegate");
    delegateField.setAccessible(true);
    delegateField.set(handler, tronNetDelegate);
    Field advField = handler.getClass().getDeclaredField("advService");
    advField.setAccessible(true);
    advField.set(handler, advService);

    PeerConnection peer1 = new PeerConnection();
    peer1.setChannel(getChannel("1.0.0.5", 1002));
    peer1.setNeedSyncFromPeer(false);
    peer1.setNeedSyncFromUs(false);
    peer1.setLastInteractiveTime(0L);

    PeerConnection peer2 = new PeerConnection();
    peer2.setChannel(getChannel("1.0.0.6", 1003));
    peer2.setNeedSyncFromPeer(false);
    peer2.setNeedSyncFromUs(false);
    peer2.setLastInteractiveTime(0L);

    // block num 100 > solidified 99 — both peers should update
    Sha256Hash blockHash = new BlockId(Sha256Hash.ZERO_HASH, 100L);
    InventoryMessage msg = new InventoryMessage(Arrays.asList(blockHash), InventoryType.BLOCK);

    handler.processMessage(peer1, msg);
    handler.processMessage(peer2, msg);

    Assert.assertTrue("peer1 lastInteractiveTime should be updated",
        peer1.getLastInteractiveTime() > 0L);
    Assert.assertTrue("peer2 lastInteractiveTime should be updated even when addInv returns false",
        peer2.getLastInteractiveTime() > 0L);
  }

  private Channel getChannel(String host, int port) throws Exception {
    Channel channel = new Channel();
    InetSocketAddress inetSocketAddress = new InetSocketAddress(host, port);

    Field field =  channel.getClass().getDeclaredField("inetSocketAddress");
    field.setAccessible(true);
    field.set(channel, inetSocketAddress);

    InetAddress inetAddress = inetSocketAddress.getAddress();
    field =  channel.getClass().getDeclaredField("inetAddress");
    field.setAccessible(true);
    field.set(channel, inetAddress);

    return channel;
  }
}
