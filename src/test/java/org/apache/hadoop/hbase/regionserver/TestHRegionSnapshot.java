/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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
import org.apache.hadoop.hbase.SnapshotDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.Reference.Range;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test snapshot of HRegion
 */
public class TestHRegionSnapshot {
  private static final Log LOG = LogFactory.getLog(TestHRegionSnapshot.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Configuration conf = TEST_UTIL.getConfiguration();

  protected static final byte[] TABLENAME = Bytes.toBytes("testtable");;
  protected static final byte[] fam1 = Bytes.toBytes("colfamily1");
  protected static final byte[] fam2 = Bytes.toBytes("colfamily2");
  protected static final byte[] fam3 = Bytes.toBytes("colfamily3");
  protected static final byte[][] FAMILIES = { fam1, fam2, fam3 };

  @Rule
  public TestName name = new TestName();

  private HRegion region = null;
  private FileSystem fs = null;

  private final Path DIR = HBaseTestingUtility.getTestDir("TestHRegionSnapshot");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
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
    fs = FileSystem.get(conf);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testSnapshot() throws IOException {
    int startRow = 100;
    int numRows = 10;
    byte [] qualifier = Bytes.toBytes("qualifier");
    Path testRoot = new Path(DIR, name.getMethodName());
    // set up region
    region = TEST_UTIL.createHRegion(TABLENAME, testRoot, FAMILIES);
    HTable metaTable = null;
    if (region.getRegionInfo().isMetaTable()) {
      // if this is a meta table, we need update the ROOT table here
      metaTable = new HTable(HConstants.ROOT_TABLE_NAME);
    } else {
      metaTable = new HTable(TEST_UTIL.getConfiguration(),
          HConstants.META_TABLE_NAME);
    }

    try {
      // prepare the region data
      putData(region, startRow, numRows, qualifier, FAMILIES);
      region.flushcache();

      // get all the HFiles of this region which would be
      // used to check against the snapshot
      Set<Path> tableFiles = new HashSet<Path>();
      for (byte[] f : FAMILIES) {
        Path storeDir = new Path(region.getRegionDir(), Bytes.toString(f));
        FileStatus[] hfiles = fs.listStatus(storeDir);
        for (FileStatus file : hfiles) {
          tableFiles.add(file.getPath());
        }
      }

      Path snapshotDir = SnapshotDescriptor.getSnapshotDir(testRoot,
          Bytes.toBytes("snapshot1"));
      // create the snapshot for this region
      long start = System.currentTimeMillis();
      region.startSnapshot();
      region.completeSnapshot(snapshotDir, metaTable);
      System.out.println("Time elapsed for snapshot: " +
          (System.currentTimeMillis() - start) + "ms");

      // verify snapshot region
      verifyRegionSnapshot(region.getRegionInfo(), tableFiles,
          snapshotDir, false);
    } finally {
      if (region != null) {
        region.close();
        region.getLog().closeAndDelete();
      }
    }
  }

  /**
   * Test creating snapshot for a daughter region after split.
   * HFiles would be copied directly instead of creating references.
   * @throws IOException
   */
  @Test
  public void testSnapshotForSplitDaughter() throws IOException {
    int startRow = 100;
    int numRows = 10;
    byte [] qualifier = Bytes.toBytes("qualifier");
    Path testRoot = new Path(DIR, name.getMethodName());
    // setting up region
    region = TEST_UTIL.createHRegion(TABLENAME, testRoot, FAMILIES);
    HTable metaTable = null;
    if (region.getRegionInfo().isMetaTable()) {
      // if this is a meta table, we need update the ROOT table here
      metaTable = new HTable(HConstants.ROOT_TABLE_NAME);
    } else {
      metaTable = new HTable(TEST_UTIL.getConfiguration(),
          HConstants.META_TABLE_NAME);
    }

    try {
      // prepare the region data
      putData(region, startRow, numRows, qualifier, FAMILIES);
      int splitRow = startRow + numRows;
      putData(region, splitRow, numRows, qualifier, FAMILIES);
      region.flushcache();

      HRegion [] regions = null;
      try {
        regions = splitRegion(region, Bytes.toBytes("" + splitRow));
        // Opening the regions returned.
        for (int i = 0; i < regions.length; i++) {
          regions[i] = TEST_UTIL.openClosedRegion(regions[i]);
        }
        // verify that the region has been split
        assertEquals(2, regions.length);

        Path snapshotDir = SnapshotDescriptor.getSnapshotDir(testRoot,
            Bytes.toBytes("snapshot2"));
        // create snapshot for a split daughter region
        regions[0].startSnapshot();
        regions[0].completeSnapshot(snapshotDir, metaTable);

        // get all the HFiles of this region which would be
        // used to check against the snapshot
        Set<Path> tableFiles = new HashSet<Path>();
        for (byte[] f : FAMILIES) {
          Path storeDir = new Path(regions[0].getRegionDir(), Bytes.toString(f));
          FileStatus[] hfiles = fs.listStatus(storeDir);
          for (FileStatus file : hfiles) {
            tableFiles.add(file.getPath());
          }
        }

        //  verify snapshot region
        verifyRegionSnapshot(regions[0].getRegionInfo(), tableFiles, snapshotDir, true);
      } finally {
        if (regions != null) {
          for (HRegion r : regions) {
            r.close();
          }
        }
      }
    } finally {
      if (region != null) {
        region.close();
        region.getLog().closeAndDelete();
      }
    }
  }

  /**
   * @param parent Region to split.
   * @param midkey Key to split around.
   * @return The Regions we created.
   * @throws IOException
   */
  HRegion [] splitRegion(final HRegion parent, final byte [] midkey)
  throws IOException {
    PairOfSameType<HRegion> result = null;
    SplitTransaction st = new SplitTransaction(parent, midkey);
    // If prepare does not return true, for some reason -- logged inside in
    // the prepare call -- we are not ready to split just now.  Just return.
    if (!st.prepare()) return null;
    try {
      result = st.execute(null);
    } catch (IOException ioe) {
      try {
        LOG.info("Running rollback of failed split of " +
          parent.getRegionNameAsString() + "; " + ioe.getMessage());
        st.rollback(null);
        LOG.info("Successful rollback of failed split of " +
          parent.getRegionNameAsString());
        return null;
      } catch (RuntimeException e) {
        // If failed rollback, kill this server to avoid having a hole in table.
        LOG.info("Failed rollback of failed split of " +
          parent.getRegionNameAsString() + " -- aborting server", e);
      }
    }
    return new HRegion [] {result.getFirst(), result.getSecond()};
  }

  @Test
  public void testCompactionAfterSnapshot() throws IOException {
    int startRow = 100;
    int numRows = 10;
    byte [] qualifier = Bytes.toBytes("qualifier");
    Path testRoot = new Path(DIR, name.getMethodName());
    // setting up region
    region = TEST_UTIL.createHRegion(TABLENAME, testRoot, FAMILIES);
    HTable metaTable = null;
    if (region.getRegionInfo().isMetaTable()) {
      // if this is a meta table, we need update the ROOT table here
      metaTable = new HTable(HConstants.ROOT_TABLE_NAME);
    } else {
      metaTable = new HTable(TEST_UTIL.getConfiguration(),
          HConstants.META_TABLE_NAME);
    }

    try {
      // prepare the region data
      putData(region, startRow, numRows, qualifier, FAMILIES);
      region.flushcache();

      Path snapshotDir = SnapshotDescriptor.getSnapshotDir(testRoot,
          Bytes.toBytes("snapshot3"));
      // create the snapshot for this region
      region.startSnapshot();
      region.completeSnapshot(snapshotDir, metaTable);

      // get all the HFiles of this region which would be
      // used to check against the snapshot
      Set<Path> tableFiles = new HashSet<Path>();
      for (byte[] f : FAMILIES) {
        Path storeDir = new Path(region.getRegionDir(), Bytes.toString(f));
        FileStatus[] hfiles = fs.listStatus(storeDir);
        for (FileStatus file : hfiles) {
          tableFiles.add(file.getPath());
        }
      }

      // verify snapshot region
      verifyRegionSnapshot(region.getRegionInfo(), tableFiles, snapshotDir, false);

      // put some data and flush again, then for each store, there would be
      // two HFiles
      putData(region, startRow, numRows, qualifier, FAMILIES);
      region.flushcache();

      // major compaction would compact two HFiles into one and old files
      // would be deleted if the reference count is 0
      region.compactStores(true);

      // those old HFiles which are used by snapshot should be archived
      // instead of deleted
      Path oldFilesDir = FSUtils.getOldHFilesDir(conf);
      Set<Path> archivedFiles = getArchivedHFiles(oldFilesDir);
      System.out.println("Current archived files: ");
      for (Path file : archivedFiles) {
        System.out.println(file);
      }
      for (Path oldFile : tableFiles) {
        Path archivePath = FSUtils.getHFileArchivePath(oldFile, oldFilesDir);
        assertTrue(archivedFiles.contains(archivePath));
      }
    } finally {
      if (region != null) {
        region.close();
        region.getLog().closeAndDelete();
      }
    }
  }

  /*
   * Put some data into the region
   */
  private void putData(HRegion region, int startRow, int numRows, byte [] qf,
      byte [] ...families) throws IOException {
    for(int i=startRow; i<startRow+numRows; i++) {
      Put put = new Put(Bytes.toBytes("" + i));
      for(byte [] family : families) {
        put.add(family, qf, HConstants.EMPTY_BYTE_ARRAY);
      }
      region.put(put);
    }
  }

  /*
   * Verify if snapshot of this region is created properly.
   */
  private void verifyRegionSnapshot(HRegionInfo srcInfo, Set<Path> tableFiles,
      Path snapshotDir, boolean isSplit) throws IOException {
    // 1. check .regioninfo
    verifyRegionInfo(srcInfo, snapshotDir);

    // 2. All HFiles in the source table are included in the snapshot.
    Path snapshotRegion = new Path(snapshotDir, srcInfo.getEncodedName());
    Set<Path> snapshotFiles = new HashSet<Path>();
    for (byte[] f : FAMILIES) {
      Path storeDir = new Path(snapshotRegion, Bytes.toString(f));
      FileStatus[] refFiles = fs.listStatus(storeDir);
      for (FileStatus file : refFiles) {
        snapshotFiles.add(file.getPath());
      }
    }
    assertEquals(tableFiles.size(), snapshotFiles.size());
    System.out.println("HFiles for source table " + TABLENAME + ":");
    for (Path file : tableFiles) {
      System.out.println(file.getName());
    }
    System.out.println("HFiles for snapshot mySnapshot:");
    for (Path file : snapshotFiles) {
      System.out.println(file.getName());
      Reference ref = Reference.read(fs, file);
      if (!isSplit) {
        assertEquals(ref.getFileRange(), Range.ENTIRE);
      }
    }

    // 3. The reference count for each HFile is increased by 1.
    // Because the initial value is 0 in this case, the result reference
    // count should be 1
    HTable metaTable = new HTable(TEST_UTIL.getConfiguration(),
        HConstants.META_TABLE_NAME);
    for (Path file : tableFiles) {
      Get get = new Get(srcInfo.getReferenceMetaRow());
      get.addColumn(HConstants.SNAPSHOT_FAMILY, Bytes.toBytes(
          FSUtils.getPath(file)));
      Result result = metaTable.get(get);
      byte[] val = result.getValue(HConstants.SNAPSHOT_FAMILY,
          Bytes.toBytes(FSUtils.getPath(file)));
      assertEquals(Bytes.toLong(val), 1);
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

  /*
   * Get all the archived HFiles in the archive dir
   */
  private Set<Path> getArchivedHFiles(Path archiveDir) throws IOException {
    Set<Path> archivedFiles = new HashSet<Path>();
    FileStatus[] fileList = fs.listStatus(archiveDir);
    for (FileStatus path : fileList) {
      if (path.isDir()) {
        Set<Path> files = getArchivedHFiles(path.getPath());
        archivedFiles.addAll(files);
      } else {
        archivedFiles.add(path.getPath());
      }
    }
    return archivedFiles;
  }
}
