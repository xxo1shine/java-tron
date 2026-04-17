package org.tron.core.net.services;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;

import com.google.common.cache.Cache;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;
import org.tron.common.BaseMethodTest;
import org.tron.common.utils.ReflectUtils;
import org.tron.common.utils.Sha256Hash;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.net.P2pEventHandlerImpl;
import org.tron.core.net.TronNetDelegate;
import org.tron.core.net.message.adv.BlockMessage;
import org.tron.core.net.messagehandler.PbftDataSyncHandler;
import org.tron.core.net.peer.PeerConnection;
import org.tron.core.net.peer.PeerManager;
import org.tron.core.net.peer.TronState;
import org.tron.core.net.service.sync.SyncService;
import org.tron.p2p.connection.Channel;
import org.tron.protos.Protocol;

public class SyncServiceTest extends BaseMethodTest {
  private SyncService service;
  private PeerConnection peer;
  private P2pEventHandlerImpl p2pEventHandler;
  private ApplicationContext ctx;
  private InetSocketAddress inetSocketAddress =
          new InetSocketAddress("127.0.0.2", 10001);

  @Override
  protected String[] extraArgs() {
    return new String[]{"--debug"};
  }

  @Override
  protected void afterInit() {
    service = context.getBean(SyncService.class);
    p2pEventHandler = context.getBean(P2pEventHandlerImpl.class);
    ctx = (ApplicationContext) ReflectUtils.getFieldObject(p2pEventHandler, "ctx");
  }

  @Override
  protected void beforeDestroy() {
    for (PeerConnection p : PeerManager.getPeers()) {
      PeerManager.remove(p.getChannel());
    }
  }

  @Test
  public void testStartSync() {
    try {
      ReflectUtils.setFieldValue(service, "fetchFlag", true);
      ReflectUtils.setFieldValue(service, "handleFlag", true);
      service.init();
      Assert.assertTrue((boolean) ReflectUtils.getFieldObject(service, "fetchFlag"));
      Assert.assertTrue((boolean) ReflectUtils.getFieldObject(service, "handleFlag"));
      peer = context.getBean(PeerConnection.class);
      Assert.assertNull(peer.getSyncChainRequested());

      Channel c1 = new Channel();
      ReflectUtils.setFieldValue(c1, "inetSocketAddress", inetSocketAddress);
      ReflectUtils.setFieldValue(c1, "inetAddress", inetSocketAddress.getAddress());

      peer.setChannel(c1);

      ReflectUtils.setFieldValue(peer, "tronState", TronState.SYNCING);

      service.startSync(peer);

      ReflectUtils.setFieldValue(peer, "tronState", TronState.INIT);

      try {
        peer.setBlockBothHave(new BlockCapsule.BlockId(Sha256Hash.ZERO_HASH, -1));
        service.syncNext(peer);
      } catch (Exception e) {
        // no need to deal with
      }

      service.startSync(peer);
    } catch (Exception e) {
      // no need to deal with
    }
    service.close();
  }

  @Test
  public void testProcessBlock() {
    peer = context.getBean(PeerConnection.class);
    Assert.assertNull(peer.getSyncChainRequested());
    Channel c1 = new Channel();
    ReflectUtils.setFieldValue(c1, "inetSocketAddress", inetSocketAddress);
    ReflectUtils.setFieldValue(c1, "inetAddress", inetSocketAddress.getAddress());
    peer.setChannel(c1);
    service.processBlock(peer,
            new BlockMessage(new BlockCapsule(Protocol.Block.newBuilder().build())));
    boolean fetchFlag = (boolean) ReflectUtils.getFieldObject(service, "fetchFlag");
    boolean handleFlag = (boolean) ReflectUtils.getFieldObject(service, "handleFlag");
    Assert.assertTrue(fetchFlag);
    Assert.assertTrue(handleFlag);
  }

  @Test
  public void testOnDisconnect() {
    Cache<BlockCapsule.BlockId, PeerConnection> requestBlockIds =
            (Cache) ReflectUtils.getFieldObject(service, "requestBlockIds");
    peer = context.getBean(PeerConnection.class);
    Assert.assertNull(peer.getSyncChainRequested());
    Channel c1 = mock(Channel.class);
    Mockito.when(c1.getInetSocketAddress()).thenReturn(inetSocketAddress);
    Mockito.when(c1.getInetAddress()).thenReturn(inetSocketAddress.getAddress());
    peer.setChannel(c1);
    BlockCapsule.BlockId blockId = new BlockCapsule.BlockId();
    requestBlockIds.put(blockId, peer);
    peer.getSyncBlockRequested().put(blockId, System.currentTimeMillis());
    service.onDisconnect(peer);
    Assert.assertTrue(requestBlockIds.getIfPresent(blockId) == null);
  }

  @Test
  public void testStartFetchSyncBlock() throws Exception {
    Field field = PeerManager.class.getDeclaredField("peers");
    field.setAccessible(true);
    field.set(PeerManager.class, Collections.synchronizedList(new ArrayList<>()));

    BlockCapsule.BlockId blockId = new BlockCapsule.BlockId();

    Method method = service.getClass().getDeclaredMethod("startFetchSyncBlock");
    method.setAccessible(true);

    Cache<BlockCapsule.BlockId, PeerConnection> requestBlockIds =
            (Cache<BlockCapsule.BlockId, PeerConnection>)
                    ReflectUtils.getFieldObject(service, "requestBlockIds");

    Channel c1 = mock(Channel.class);
    Mockito.when(c1.getInetSocketAddress()).thenReturn(inetSocketAddress);
    Mockito.when(c1.getInetAddress()).thenReturn(inetSocketAddress.getAddress());

    PeerManager.add(ctx, c1);
    peer = PeerManager.getPeers().get(0);

    Method method1 = PeerManager.class.getDeclaredMethod("check");
    method1.setAccessible(true);
    method1.invoke(PeerManager.class);
    Method method2 = PeerManager.class.getDeclaredMethod("logPeerStats");
    method2.setAccessible(true);
    method2.invoke(PeerManager.class);

    method.invoke(service);
    Assert.assertTrue(peer.getSyncBlockRequested().get(blockId) == null);

    peer.getSyncBlockToFetch().add(blockId);
    method.invoke(service);
    Assert.assertTrue(peer.getSyncBlockToFetch().size() == 1);
    Assert.assertTrue(peer.getSyncBlockRequested().get(blockId) == null);

    peer.setFetchAble(true);
    method.invoke(service);
    Assert.assertTrue(peer.getSyncBlockToFetch().size() == 1);
    Assert.assertTrue(peer.getSyncBlockRequested().get(blockId) != null);
    Assert.assertTrue(requestBlockIds.getIfPresent(blockId) != null);

    peer.getSyncBlockRequested().remove(blockId);
    method.invoke(service);
    Assert.assertTrue(peer.getSyncBlockRequested().get(blockId) == null);
  }

  @Test
  public void testProcessSyncBlockSetsBlockRcvTime() throws Exception {
    TronNetDelegate mockDelegate = mock(TronNetDelegate.class);
    PbftDataSyncHandler mockPbft = mock(PbftDataSyncHandler.class);
    Mockito.doNothing().when(mockDelegate).validSignature(any(BlockCapsule.class));
    Mockito.doNothing().when(mockDelegate).processBlock(any(BlockCapsule.class), anyBoolean());
    Mockito.doReturn(new ArrayList<PeerConnection>()).when(mockDelegate).getActivePeer();
    Mockito.doNothing().when(mockPbft).processPBFTCommitData(any(BlockCapsule.class));

    Object originDelegate = ReflectUtils.getFieldObject(service, "tronNetDelegate");
    Object originPbft = ReflectUtils.getFieldObject(service, "pbftDataSyncHandler");
    try {
      ReflectUtils.setFieldValue(service, "tronNetDelegate", mockDelegate);
      ReflectUtils.setFieldValue(service, "pbftDataSyncHandler", mockPbft);

      peer = context.getBean(PeerConnection.class);
      Channel c1 = new Channel();
      ReflectUtils.setFieldValue(c1, "inetSocketAddress", inetSocketAddress);
      ReflectUtils.setFieldValue(c1, "inetAddress", inetSocketAddress.getAddress());
      peer.setChannel(c1);
      peer.setBlockRcvTime(0L);

      BlockCapsule blockCapsule = new BlockCapsule(Protocol.Block.newBuilder().build());

      Method method = service.getClass()
          .getDeclaredMethod("processSyncBlock", BlockCapsule.class, PeerConnection.class);
      method.setAccessible(true);

      long before = System.currentTimeMillis();
      method.invoke(service, blockCapsule, peer);
      long after = System.currentTimeMillis();

      Assert.assertTrue("blockRcvTime should be set after successful processSyncBlock",
          peer.getBlockRcvTime() >= before && peer.getBlockRcvTime() <= after);
    } finally {
      ReflectUtils.setFieldValue(service, "tronNetDelegate", originDelegate);
      ReflectUtils.setFieldValue(service, "pbftDataSyncHandler", originPbft);
    }
  }

  @Test
  public void testProcessSyncBlockDoesNotSetBlockRcvTimeOnFailure() throws Exception {
    TronNetDelegate mockDelegate = mock(TronNetDelegate.class);
    PbftDataSyncHandler mockPbft = mock(PbftDataSyncHandler.class);
    Mockito.doNothing().when(mockDelegate).validSignature(any(BlockCapsule.class));
    Mockito.doThrow(new RuntimeException("process failed"))
        .when(mockDelegate).processBlock(any(BlockCapsule.class), anyBoolean());
    Mockito.doReturn(new ArrayList<PeerConnection>()).when(mockDelegate).getActivePeer();

    Object originDelegate = ReflectUtils.getFieldObject(service, "tronNetDelegate");
    Object originPbft = ReflectUtils.getFieldObject(service, "pbftDataSyncHandler");
    try {
      ReflectUtils.setFieldValue(service, "tronNetDelegate", mockDelegate);
      ReflectUtils.setFieldValue(service, "pbftDataSyncHandler", mockPbft);

      peer = context.getBean(PeerConnection.class);
      Channel c1 = new Channel();
      ReflectUtils.setFieldValue(c1, "inetSocketAddress", inetSocketAddress);
      ReflectUtils.setFieldValue(c1, "inetAddress", inetSocketAddress.getAddress());
      peer.setChannel(c1);
      peer.setBlockRcvTime(0L);

      BlockCapsule blockCapsule = new BlockCapsule(Protocol.Block.newBuilder().build());

      Method method = service.getClass()
          .getDeclaredMethod("processSyncBlock", BlockCapsule.class, PeerConnection.class);
      method.setAccessible(true);
      method.invoke(service, blockCapsule, peer);

      Assert.assertEquals("blockRcvTime must stay 0 when processBlock throws",
          0L, peer.getBlockRcvTime());
    } finally {
      ReflectUtils.setFieldValue(service, "tronNetDelegate", originDelegate);
      ReflectUtils.setFieldValue(service, "pbftDataSyncHandler", originPbft);
    }
  }

  @Test
  public void testHandleSyncBlock() throws Exception {

    Field field = PeerManager.class.getDeclaredField("peers");
    field.setAccessible(true);
    field.set(PeerManager.class, Collections.synchronizedList(new ArrayList<>()));

    Method method = service.getClass().getDeclaredMethod("handleSyncBlock");
    method.setAccessible(true);

    Map<BlockMessage, PeerConnection> blockJustReceived =
            (Map<BlockMessage, PeerConnection>)
            ReflectUtils.getFieldObject(service, "blockJustReceived");
    Protocol.BlockHeader.raw.Builder blockHeaderRawBuild = Protocol.BlockHeader.raw.newBuilder();
    Protocol.BlockHeader.raw blockHeaderRaw = blockHeaderRawBuild
        .setNumber(100000)
        .build();

    // block header
    Protocol.BlockHeader.Builder blockHeaderBuild = Protocol.BlockHeader.newBuilder();
    Protocol.BlockHeader blockHeader = blockHeaderBuild.setRawData(blockHeaderRaw).build();

    BlockCapsule blockCapsule = new BlockCapsule(Protocol.Block.newBuilder()
        .setBlockHeader(blockHeader).build());

    BlockCapsule.BlockId blockId = blockCapsule.getBlockId();


    InetSocketAddress a1 = new InetSocketAddress("127.0.0.1", 10001);
    Channel c1 = mock(Channel.class);
    Mockito.when(c1.getInetSocketAddress()).thenReturn(a1);
    Mockito.when(c1.getInetAddress()).thenReturn(a1.getAddress());
    PeerManager.add(ctx, c1);
    peer = PeerManager.getPeers().get(0);

    blockJustReceived.put(new BlockMessage(blockCapsule), peer);

    peer.getSyncBlockToFetch().add(blockId);

    Cache<BlockCapsule.BlockId, PeerConnection> requestBlockIds =
            (Cache<BlockCapsule.BlockId, PeerConnection>)
                    ReflectUtils.getFieldObject(service, "requestBlockIds");

    requestBlockIds.put(blockId, peer);

    method.invoke(service);

    Assert.assertTrue(requestBlockIds.getIfPresent(blockId) == null);
  }
}
