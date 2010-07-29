package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HSnapshotDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.Reference.Range;
import org.apache.hadoop.hbase.master.SnapshotMonitor.SnapshotStatus;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.TestZKSnapshotWatcher;
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

public class TestSnapshot {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestZKSnapshotWatcher.class);

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
  private static long firstFlushTime;

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
    // start at lease 2 region servers
    TEST_UTIL.startMiniCluster(2);
    // set up the table regions, all the test cases will create
    // snapshots for this table
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
    firstFlushTime = System.currentTimeMillis();
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
  public void testSnapshotForOnlineTable() throws IOException {
    // start creating a snapshot for testtable
    byte[] snapshotName = Bytes.toBytes("snapshot1");
    HSnapshotDescriptor hsd = new HSnapshotDescriptor(snapshotName, TABLENAME);

    long start = System.currentTimeMillis();
    // start monitor first and then start the snapshot via ZK
    master.getSnapshotMonitor().start(hsd);
    assertTrue(zk.startSnapshot(hsd));

    // add other region server which doesn't serve the table regions
    // under ready or finish directory won't change the status
    zk.registerRSForSnapshot("other-rs", SnapshotStatus.RS_READY);
    zk.registerRSForSnapshot("other-rs", SnapshotStatus.RS_FINISH);

    // wait for the snapshot to finish
    SnapshotStatus finalStatus = master.getSnapshotMonitor().waitToFinish();
    System.out.println("Time elapsed for creating snapshot: " +
        (System.currentTimeMillis() - start) + "ms");
    assertEquals(finalStatus, SnapshotStatus.M_ALLFINISH);
    countOfSnapshot++;

    // verify snapshot of this table
    verifySnapshot(snapshotName, true);
  }

  @Test
  public void testSnapshotForOfflineTable() throws IOException, InterruptedException {
    // start creating a snapshot for testtable
    byte[] snapshotName = Bytes.toBytes("snapshot2");
    HSnapshotDescriptor hsd = new HSnapshotDescriptor(snapshotName, TABLENAME);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.disableTable(TABLENAME);
    assertTrue(admin.isTableDisabled(TABLENAME));

    long start = System.currentTimeMillis();
    new TableSnapshot(master, hsd).process();
    System.out.println("Time elapsed for creating snapshot: " +
        (System.currentTimeMillis() - start) + "ms");
    countOfSnapshot++;

    // verify snapshot of this table
    verifySnapshot(snapshotName, false);
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

  /*
   * Verify if the table snapshot is created successfully
   */
  private void verifySnapshot(final byte[] snapshotName, final boolean isOnline)
    throws IOException {
    Path rootDir = master.getRootDir();
    Path snapshotDir = HSnapshotDescriptor.getSnapshotDir(rootDir, snapshotName);
    if (isOnline) {
      // verify hlogs for both region server
      verifyHLogsList(server1, snapshotDir);
      verifyHLogsList(server2, snapshotDir);
    }

    // because this table might be offline (for testSnapshotForOfflineTable)
    // we can't use HTable here. Scan meta table instead
    HTable meta = new HTable(TEST_UTIL.getConfiguration(),
        HConstants.META_TABLE_NAME);
    Scan scan = new Scan(TABLENAME).addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner s = meta.getScanner(scan);
    for (Result r = null; (r = s.next()) != null;) {
      byte [] b = 
        r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
      if (b == null || b.length <= 0) break;
      HRegionInfo info = Writables.getHRegionInfo(b);
      if (Bytes.equals(info.getTableDesc().getName(), TABLENAME)) {
        // get all the HFiles for this region
        Set<Path> regionFiles = new HashSet<Path>();
        for (byte[] f : FAMILIES) {
          Path storeDir = new Path(HRegion.getRegionDir(rootDir, info),
              Bytes.toString(f));
          FileStatus[] hfiles = fs.listStatus(storeDir);
          for (FileStatus file : hfiles) {
            regionFiles.add(file.getPath());
          }
        }
        // verify region snapshot
        verifyRegionSnapshot(info, regionFiles, snapshotDir);
      }
    }
  }

  /*
   * Verify if the dump log list is the same as current log list.
   */
  private void verifyHLogsList(HRegionServer server, Path snapshotDir) throws IOException {
    SortedMap<Long, Path> logFiles = server.getLog().getCurrentLogFiles();
    Path oldLog = new Path(snapshotDir, HLog.getHLogDirectoryName(
        server.getServerInfo()));
    FSDataInputStream in = fs.open(oldLog);
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
    // but the sequence number might be different.
    // skipping the sequence number for current log file
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
      if (Long.valueOf(file.getName()) < firstFlushTime) {
        // for those files which are created in the first flushing,
        // the reference count equals to the number of snapshots
        assertEquals(countOfSnapshot, Bytes.toLong(val));
      } else {
        // for those files which are created after the first flushing,
        // that is created when the table is disabled, the reference
        // count is 1 because it is only used by the second snapshot
        assertEquals(countOfSnapshot - 1, Bytes.toLong(val));
      }
    }
  }

  /*
   * Check if the dumped region info is the same as the original
   * region info. 
   */
  private void verifyRegionInfo(HRegionInfo srcInfo, Path snapshotDir)
    throws IOException {
    Path snapshotRegion = new Path(snapshotDir, srcInfo.getEncodedName());
    Path regioninfo = new Path(snapshotRegion, HRegion.REGIONINFO_FILE);
    FSDataInputStream in = fs.open(regioninfo);
    HRegionInfo dumpedInfo = new HRegionInfo();
    dumpedInfo.readFields(in);
    assertEquals(dumpedInfo, srcInfo);
  }
}