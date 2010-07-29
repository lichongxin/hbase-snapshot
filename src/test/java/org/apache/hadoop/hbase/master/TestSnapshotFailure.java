package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HSnapshotDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.SnapshotMonitor.SnapshotStatus;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.TestZKSnapshotWatcher;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSnapshotFailure implements Watcher {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestZKSnapshotWatcher.class);

  private static final byte[] TABLENAME = Bytes.toBytes("testtable");
  private static final byte[] fam1 = Bytes.toBytes("colfamily1");
  private static final byte[] fam2 = Bytes.toBytes("colfamily2");
  private static final byte[] fam3 = Bytes.toBytes("colfamily3");
  private static final byte[][] FAMILIES = { fam1, fam2, fam3 };
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");

  private static int countOfRegions;

  private static HMaster master;
  private static HRegionServer server1;
  private static HRegionServer server2;
  private static ZooKeeperWrapper zk;
  private static FileSystem fs;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    c.setInt("hbase.regionserver.info.port", 0);
    // set up the cluster and multiple table regions
    // start at lease 2 region servers
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.createTable(TABLENAME, FAMILIES);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    countOfRegions = TEST_UTIL.createMultiRegions(t, FAMILIES[0]);
    waitUntilAllRegionsAssigned(countOfRegions);
    addToEachStartKey(countOfRegions);
    // flush the region to persist data in HFiles
    zk = TEST_UTIL.getHBaseCluster().getMaster().getZooKeeperWrapper();
    master = TEST_UTIL.getHBaseCluster().getMaster();
    server1 = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    server2 = TEST_UTIL.getHBaseCluster().getRegionServer(1);
    fs = master.getFileSystem();
    for (HRegion region : server1.getOnlineRegions()) {
      region.flushcache();
    }
    for (HRegion region : server2.getOnlineRegions()) {
      region.flushcache();
    }
    // we add some data again and don't flush the cache
    // so that the HLog is not empty
    addToEachStartKey(countOfRegions);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testSnapshotRSError() throws IOException {
    // start creating a snapshot for testtable
    byte[] snapshotName = Bytes.toBytes("snapshot4");
    HSnapshotDescriptor hsd = new HSnapshotDescriptor(snapshotName, TABLENAME);

    // this watcher is used to simulate snapshot error on one region server
    zk.registerListener(this);

    // start monitor first and then start the snapshot via ZK
    master.getSnapshotMonitor().start(hsd);
    assertTrue(zk.startSnapshot(hsd));
    
    SnapshotStatus status = master.getSnapshotMonitor().waitToFinish();
    assertEquals(SnapshotStatus.M_ABORTED, status);
  }

  @Override
  public void process(WatchedEvent event) {
    String path = event.getPath();
    EventType type = event.getType();
    // remove a region server znode under ready directory, and
    // this will notify the master that exception occurs during 
    // the snapshot on one region server
    if (type.equals(EventType.NodeChildrenChanged) &&
        path.equals(zk.getSnapshotReadyZNode())) {
      List<String> rss = zk.listZnodes(zk.getSnapshotReadyZNode());
      
      zk.removeRSForSnapshot(rss.get(0), SnapshotStatus.RS_READY);
      zk.unregisterListener(this);
    }
  }

  @Test
  public void testSnapshotTimeout() throws IOException {
    // start creating a snapshot for testtable
    byte[] snapshotName = Bytes.toBytes("snapshot3");
    HSnapshotDescriptor hsd = new HSnapshotDescriptor(snapshotName, TABLENAME);

    // Cut region server connection with ZK:
    // unregister region server's ZKSnapshotWatcher so it will not
    // receive the snapshot request from ZK
    server2.getZooKeeperWrapper().unregisterListener(
        server2.getSnapshotWatcher());

    // start monitor first and then start the snapshot via ZK
    master.getSnapshotMonitor().start(hsd);
    assertTrue(zk.startSnapshot(hsd));

    SnapshotStatus status = master.getSnapshotMonitor().waitToFinish();
    assertEquals(SnapshotStatus.M_ABORTED, status);
  }

  private static void waitUntilAllRegionsAssigned(final int countOfRegions)
  throws IOException {
    HTable meta = new HTable(TEST_UTIL.getConfiguration(),
      HConstants.META_TABLE_NAME);
    while (true) {
      int rows = 0;
      Scan scan = new Scan();
      scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
      ResultScanner s = meta.getScanner(scan);
      for (Result r = null; (r = s.next()) != null;) {
        byte [] b =
          r.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
        if (b == null || b.length <= 0) break;
        rows++;
      }
      s.close();
      // If I get to here and all rows have a Server, then all have been assigned.
      if (rows == countOfRegions) break;
      LOG.info("Found=" + rows);
      Threads.sleep(1000); 
    }
  }

  /*
   * Add to each of the regions in .META. a value.  Key is the startrow of the
   * region (except its 'aaa' for first region).  Actual value is the row name.
   * @param expected
   * @return
   * @throws IOException
   */
  private static int addToEachStartKey(final int expected) throws IOException {
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    HTable meta = new HTable(TEST_UTIL.getConfiguration(),
        HConstants.META_TABLE_NAME);
    int rows = 0;
    Scan scan = new Scan();
    scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    ResultScanner s = meta.getScanner(scan);
    for (Result r = null; (r = s.next()) != null;) {
      byte [] b =
        r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      if (b == null || b.length <= 0) break;
      HRegionInfo hri = Writables.getHRegionInfo(b);
      // If start key, add 'aaa'.
      byte [] row = getStartKey(hri);
      Put p = new Put(row);
      p.add(FAMILIES[0], QUALIFIER, row);
      t.put(p);
      rows++;
    }
    s.close();
    Assert.assertEquals(expected, rows);
    return rows;
  }

  private static byte [] getStartKey(final HRegionInfo hri) {
    return Bytes.equals(HConstants.EMPTY_START_ROW, hri.getStartKey())?
        Bytes.toBytes("aaa"): hri.getStartKey();
  }
}
