package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSnapshotExport {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final byte[] SNAPSHOT_NAME = Bytes.toBytes("testSnapshot");
  private static final byte[] TABLE_NAME = Bytes.toBytes("testTable");
  private static final byte[] FAMILY1 = Bytes.toBytes("family1");
  private static final byte[] FAMILY2 = Bytes.toBytes("family2");
  private static final byte[] FAMILY3 = Bytes.toBytes("family3");

  private Path outputDir = TEST_UTIL.getTestDir("export-output");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.startMiniMapReduceCluster();
    createSnapshot();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniMapReduceCluster();
  }

  /*
   * Create a snapshot which is used to export and import
   */
  private static void createSnapshot() throws IOException, InterruptedException {
    // create table and put some data into the table
    HTable table = TEST_UTIL.createTable(TABLE_NAME,
        new byte[][] {FAMILY1, FAMILY2, FAMILY3});
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    for (int i = 0; i < 10; i++) {
      byte[] val = Bytes.toBytes(i);
      Put put = new Put(val);
      put.add(FAMILY1, null, val);
      put.add(FAMILY2, null, val);
      put.add(FAMILY3, null, val);
      table.put(put);
    }
    // flush the table so that the above data would be
    // persistent on the file system
    admin.flush(TABLE_NAME);
    // data below would be still in the commit edits log
    for (int i = 10; i < 20; i++) {
      byte[] val = Bytes.toBytes(i);
      Put put = new Put(val);
      put.add(FAMILY1, null, val);
      put.add(FAMILY2, null, val);
      put.add(FAMILY3, null, val);
      table.put(put);
    }
    // wait a while for the flush to finish
    Thread.sleep(2000);
    admin.snapshot(SNAPSHOT_NAME, TABLE_NAME);
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

  /**
   * Export the snapshot to the output dir
   * @throws Exception
   */
  @Test
  public void testSnapshotExport() throws Exception {
    ExportSnapshot.exportSnapshot(TEST_UTIL.getConfiguration(),
        new String[] {Bytes.toString(SNAPSHOT_NAME), outputDir.toString()});
  }

  /**
   * Import the above exported snapshot to another table
   * @throws Exception
   */
  @Test
  public void testSnapshotImport()throws Exception {
    String exportName =
      Bytes.toString(SNAPSHOT_NAME) + ExportSnapshot.SNAPSHOT_EXPORT_SUFFIX;
    Path exp = new Path(outputDir, exportName);
    ImportSnapshot.importSnapshot(TEST_UTIL.getConfiguration(),
        new String[] {"anotherTable", exp.toString()});

    // verify the table data
    HTable table = new HTable("anotherTable");
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    Result r = null;
    int rowCount = 0;
    System.out.println("Table Values: ");
    while ((r = scanner.next()) != null) {
      rowCount++;
      int val = Bytes.toInt(r.getRow());
      System.out.println("Row: " + val);
      assertEquals(val, Bytes.toInt(r.getValue(FAMILY1, null)));
      assertEquals(val, Bytes.toInt(r.getValue(FAMILY2, null)));
      assertEquals(val, Bytes.toInt(r.getValue(FAMILY3, null)));
    }
    assertEquals(20, rowCount);
    System.out.println("Table rows: " + rowCount);
  }
}
