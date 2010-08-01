package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HSnapshotDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.Reference.Range;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZKSnapshotWatcher {
  private static final Log LOG = LogFactory.getLog(TestZKSnapshotWatcher.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final byte[] TABLENAME = Bytes.toBytes("testtable");
  private static final byte[] fam1 = Bytes.toBytes("colfamily1");
  private static final byte[] fam2 = Bytes.toBytes("colfamily2");
  private static final byte[] fam3 = Bytes.toBytes("colfamily3");
  private static final byte[][] FAMILIES = { fam1, fam2, fam3 };
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");

  private static int countOfRegions;
  /*
   * number of snapshots, increase this number each time when
   * a snapshot is created
   */
  private static int countOfSnapshot = 0;

  private static HRegionServer server;
  private static ZooKeeperWrapper zk;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // set up the cluster and multiple table regions
    // all the test cases will create snapshots for this table
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.createTable(TABLENAME, FAMILIES);
    HTable t = new HTable(TEST_UTIL.getConfiguration(), TABLENAME);
    countOfRegions = TEST_UTIL.createMultiRegions(t, FAMILIES[0]);
    waitUntilAllRegionsAssigned(countOfRegions);
    addToEachStartKey(countOfRegions);
    // flush the region to put data in HFiles
    server = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    for (HRegion region : server.getOnlineRegions()) {
      region.flushcache();
    }
    // we add some data again and don't flush the cache
    // so that the HLog is not empty
    addToEachStartKey(countOfRegions);
    zk = TEST_UTIL.getHBaseCluster().getMaster().getZooKeeperWrapper();
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
  public void testHandleSnapshotStart() throws IOException, InterruptedException {
    // start creating a snapshot for testtable
    byte[] snapshotName = Bytes.toBytes("snapshot1");
    HSnapshotDescriptor hsd = new HSnapshotDescriptor(snapshotName, TABLENAME);

    long start = System.currentTimeMillis();
    assertTrue(zk.startSnapshot(hsd));
    waitUntilTerminated();
    System.out.println("Time elapsed for creating snapshot: " +
        (System.currentTimeMillis() - start) + "ms");
    countOfSnapshot++;

    // verify the snapshot regions
    Path snapshotDir = HSnapshotDescriptor.getSnapshotDir(server.getRootDir(),
        snapshotName);
    verifyHLogsList(snapshotDir);
    for (HRegion region : server.getOnlineRegions()) {
      if (Bytes.equals(region.getTableDesc().getName(), TABLENAME)) {
        Set<Path> regionFiles = new HashSet<Path>();
        for (byte[] f : FAMILIES) {
          Path storeDir = new Path(region.getRegionDir(), Bytes.toString(f));
          FileStatus[] hfiles = server.getFileSystem().listStatus(storeDir);
          for (FileStatus file : hfiles) {
            regionFiles.add(file.getPath());
          }
        }
        verifyRegionSnapshot(region.getRegionInfo(), regionFiles, snapshotDir);
      }
    }

    //server znodes should be created under both ready and finish directory
    String readyNode = zk.getZNode(zk.getSnapshotReadyZNode(),
        server.getServerInfo().getServerName());
    String finishNode = zk.getZNode(zk.getSnapshotFinishZNode(),
        server.getServerInfo().getServerName());
    assertTrue(zk.exists(readyNode, false));
    assertTrue(zk.exists(finishNode, false));
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

  private void waitUntilTerminated() throws InterruptedException {
    while (server.snapshotWatcher.snapshotThread == null) {
      Thread.sleep(100);
    }
    // wait for the snapshot thread to finish
    server.snapshotWatcher.snapshotThread.join();
  }

  /*
   * Verify if the dump log list is the same as current log list.
   */
  private void verifyHLogsList(Path snapshotDir) throws IOException {
    SortedMap<Long, Path> logFiles = server.getLog().getCurrentLogFiles();
    Path oldLog = new Path(snapshotDir, HLog.getHLogDirectoryName(
        server.getServerInfo()));
    FSDataInputStream in = server.getFileSystem().open(oldLog);
    int logNum = in.readInt();
    assertEquals(logNum, logFiles.size());
    for (int i = 0; i < logNum - 1; i++) {
      long sequence = in.readLong();
      Path log = logFiles.get(sequence);
      assertNotNull(log);
      String fileName = in.readUTF();
      assertEquals(fileName, log.getName());
    }
    // the current log file is also in the dumped log list
    // but the sequence number might be different
    in.readLong();
    String currentLogName = in.readUTF();
    Path currentLog = logFiles.get(logFiles.lastKey());
    assertEquals(currentLog.getName(), currentLogName);
  }

  /*
   * Verify if snapshot of a region is created properly.
   */
  private void verifyRegionSnapshot(HRegionInfo srcInfo, Set<Path> regionFiles,
      Path snapshotDir) throws IOException {
    // 1. check .regioninfo
    verifyRegionInfo(srcInfo, snapshotDir);

    // 2. All HFiles in the source table are included in the snapshot.
    Path snapshotRegion = new Path(snapshotDir, srcInfo.getEncodedName());
    FileSystem fs = server.getFileSystem();
    Set<Path> snapshotFiles = new HashSet<Path>();
    for (byte[] f : FAMILIES) {
      Path storeDir = new Path(snapshotRegion, Bytes.toString(f));
      FileStatus[] refFiles = fs.listStatus(storeDir);
      if (refFiles != null) {
        for (FileStatus file : refFiles) {
          snapshotFiles.add(file.getPath());
        }
      }
    }
    assertEquals(regionFiles.size(), snapshotFiles.size());
    System.out.println("HFiles for source region " + srcInfo.getRegionId() + ":");
    for (Path file : regionFiles) {
      System.out.println(file.getName());
    }
    System.out.println("HFiles for snapshot region:");
    for (Path file : snapshotFiles) {
      System.out.println(file.getName());
      Reference ref = Reference.read(fs, file);
      assertEquals(ref.getFileRegion(), Range.entire);
    }

    // 3. The reference count for each HFile is increased by 1.
    HTable metaTable = new HTable(HConstants.META_TABLE_NAME);
    for (Path file : regionFiles) {
      Get get = new Get(srcInfo.getReferenceMetaRow());
      get.addColumn(HConstants.SNAPSHOT_FAMILY, Bytes.toBytes(
          FSUtils.getPath(file)));
      Result result = metaTable.get(get);
      byte[] val = result.getValue(HConstants.SNAPSHOT_FAMILY,
          Bytes.toBytes(FSUtils.getPath(file)));
      assertEquals(Bytes.toLong(val), countOfSnapshot);
    }
  }

  /*
   * Check if the dumped region info is the same as the original
   * region info.
   */
  private void verifyRegionInfo(HRegionInfo srcInfo, Path snapshotDir)
    throws IOException {
    FileSystem fs = server.getFileSystem();
    Path snapshotRegion = new Path(snapshotDir, srcInfo.getEncodedName());
    Path regioninfo = new Path(snapshotRegion, HRegion.REGIONINFO_FILE);
    FSDataInputStream in = fs.open(regioninfo);
    HRegionInfo dumpedInfo = new HRegionInfo();
    dumpedInfo.readFields(in);
    assertEquals(dumpedInfo, srcInfo);
  }
}
