package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.SnapshotDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.ExportSnapshot.FilePair;
import org.apache.hadoop.hbase.mapreduce.ExportSnapshot.SnapshotCopier;
import org.apache.hadoop.hbase.mapreduce.ExportSnapshot.SnapshotInputFormat;
import org.apache.hadoop.hbase.master.SnapshotOperation;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * A Map-reduce program to import a table from an exported snapshot.
 *
 * Import of snapshot works in two steps:
 * 1. Copy all snapshot files into the table directory. This is finished
 * by a Map-Reduce job.
 * 2. Set up and enable the imported table.
 */
public class ImportSnapshot {
  static final String NAME = "importsnapshot";

  // Split parent region in exported snapshot has a suffix ".split"
  static final Pattern SPLIT_REGION_NAME_PATTERN =
    Pattern.compile("^(\\w+)(\\.split)$");

  static final String TMP_DIR_LABEL = NAME + ".tmp.dir";
  static final String DST_DIR_LABEL = NAME + ".dst.dir";
  static final String SRC_LIST_LABEL = NAME + ".src.list";
  static final String TOTAL_SIZE_LABEL = NAME + ".total.size";

  // tmp dir for split logs
  static final String TMP_SPLIT_DIR = ".tmp";

  private static String tableName;
  private static String inputSnapshot;

  /**
   * This map contains the region info for the restored table
   * Mapping original region name ->  region info
   */
  private static Map<byte[], HRegionInfo> regionInfoMap =
    new HashMap<byte[], HRegionInfo>();

  /**
   * Prepare the input snapshot files and job configuration.
   *
   * @param snapshotDir directory of the exported snapshot
   * @param tableName import table name
   * @param c Configuration
   * @throws IOException
   */
  static void prepare(Path snapshotDir, String tableName,
      Configuration c) throws IOException {
    Path root = FSUtils.getRootDir(c);
    Path tableDir = new Path(root, tableName);
    FileSystem fs = tableDir.getFileSystem(c);

    // check table existence
    checkAndCreateTableDir(tableDir, fs, c);
    c.set(DST_DIR_LABEL, tableDir.toString());

    // walk through exported snapshot to get the snapshot files
    List<FilePair> snapshotFiles = getSnapshotFiles(snapshotDir, tableDir, c);

    // create a tmp directory which would be used when copy files
    Path tmpDir = new Path(snapshotDir, TMP_SPLIT_DIR);
    if (!fs.mkdirs(tmpDir)) {
      throw new IOException("Failed to create temp dir " + tmpDir);
    }
    c.set(TMP_DIR_LABEL, tmpDir.toString());

    // write the snapshot files into a list which would be used as
    // the input for the mr job
    Path srcFileList = new Path(tmpDir, "_src_files_list");
    c.set(SRC_LIST_LABEL, srcFileList.toString());
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, c,
        srcFileList, LongWritable.class, FilePair.class,
        SequenceFile.CompressionType.NONE);
    long totalBytes = 0;
    try {
      for (FilePair file : snapshotFiles) {
        long len = file.input.getLen();
        totalBytes += len;
        writer.append(new LongWritable(len), file);
      }
      writer.syncFs();
    } finally {
      writer.close();
    }
    c.setLong(TOTAL_SIZE_LABEL, totalBytes);
  }

  /*
   * Check whether the table exists in file system or META. Create
   * the table dir if it does not exist.
   */
  private static void checkAndCreateTableDir(Path tableDir, FileSystem fs,
      Configuration conf) throws IOException {
    // check table existence on file system
    if (fs.exists(tableDir)) {
      throw new IOException("Table " + tableName + " already exists");
    }

    // check table existence in META
    HTable meta = getMetaTable(tableName, conf);
    byte [] firstRowInMeta = Bytes.toBytes(tableName + ",,");
    Scan scan =
      new Scan(firstRowInMeta).addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner s = meta.getScanner(scan);
    try {
      Result r = s.next();
      if (r != null && !r.isEmpty()) {
        byte[] b = r.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER);
        HRegionInfo info = Writables.getHRegionInfo(b);
        if (tableName.equals(info.getTableDesc().getNameAsString())) {
          throw new TableExistsException("Table " + tableName
              + " already exists");
        }
      }
    } finally {
      s.close();
    }

    if (!fs.mkdirs(tableDir)) {
      throw new IOException("Failed to create table dir " + tableDir);
    }
  }

  /*
   * Get all the files under snapshot directory and compose the dst
   * path for these files.
   *
   * @param snapshotDir snapshot directory
   * @param tableDir destination table directory
   * @param conf configuration
   * @return a list of FilePairs for snapshot files 
   * @throws IOException
   */
  private static List<FilePair> getSnapshotFiles(Path snapshotDir, Path tableDir,
      Configuration conf) throws IOException {
    List<FilePair> snapshotFiles = new ArrayList<FilePair>();
    FileSystem srcFs = snapshotDir.getFileSystem(conf);
    Stack<FileStatus> pathStack = new Stack<FileStatus>();
    FileStatus base = srcFs.getFileStatus(snapshotDir);
    pathStack.add(base);
    while (!pathStack.empty()) {
      FileStatus cur = pathStack.pop();
      if (cur.isDir()) {
        FileStatus[] files = srcFs.listStatus(cur.getPath());
        for (FileStatus file : files) {
          pathStack.add(file);
        }
      } else {
        Path dst = makeDstPath(cur.getPath(), snapshotDir, tableDir);
        if (dst != null) {
          snapshotFiles.add(new FilePair(cur, dst.toString()));
        }
      }
    }
    return snapshotFiles;
  }

  /*
   * Compose the destination path under directory <code>dstDir</code> for
   * path <code>p</code> under <code>srcDir</code>.
   *
   * @param p
   * @param srcDir source directory
   * @param dstDir destination directory
   * @return
   */
  private static Path makeDstPath(Path p, Path srcDir, Path dstDir) {
    StringTokenizer tokens = new StringTokenizer(p.toUri().getPath(),
        Path.SEPARATOR);
    StringTokenizer dirTokens = new StringTokenizer(srcDir.toUri().getPath(),
        Path.SEPARATOR);
    assert(tokens.countTokens() >= dirTokens.countTokens());
    while (dirTokens.hasMoreTokens()) {
      String token = tokens.nextToken();
      String dirToken = dirTokens.nextToken();
      if (!token.equals(dirToken)) {
        // this path does not belong to the srcDir
        return null;
      }
    }

    StringBuilder sb = new StringBuilder(dstDir.toString());
    while (tokens.hasMoreTokens()) {
      sb.append(Path.SEPARATOR);
      sb.append(tokens.nextToken());
    }
    return new Path(sb.toString());
  }

  /*
   * Set up the table when the copy job completes successfully, i.e. after all
   * snapshot files are moved into the table dir. After set up, this table
   * could be read and written as other tables.
   *
   * @param tableName table name
   * @param conf Configuration
   * @throws IOException
   */
  static void setupTable(final Configuration conf)
  throws IOException {
    Path root = FSUtils.getRootDir(conf);
    Path tableDir = new Path(root, tableName);
    final FileSystem fs = tableDir.getFileSystem(conf);

    Path snapshotInfo = new Path(tableDir, SnapshotDescriptor.SNAPSHOTINFO_FILE);
    SnapshotDescriptor info = readSnapshotInfo(snapshotInfo, fs);
    // compare the restored table name and snapshot table name
    boolean tableChanged = !info.getTableNameAsString().equals(tableName);

    // split the commit edits logs
    Path logsDir = new Path(tableDir, HConstants.HREGION_LOGDIR_NAME);
    Path tmpSplitDir = getTmpSplitDir(logsDir);
    splitLogs(logsDir, info.getTableName(), fs, conf);

    final List<Path> ordinaryRegions = new ArrayList<Path>();
    final List<Path> splitParents = new ArrayList<Path>();
    fs.listStatus(tableDir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          boolean isDir = fs.getFileStatus(p).isDir();
          Matcher m1 = SnapshotOperation.REGION_NAME_PATTERN.matcher(p.getName());
          Matcher m2 = SPLIT_REGION_NAME_PATTERN.matcher(p.getName());
          if (isDir && m1.matches()) {
            ordinaryRegions.add(p);
          } else if (isDir && m2.matches()) {
            splitParents.add(p);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
        return result;
      }
    });

    /*
     * Set up regions of this table. Since split parent regions would
     * add split daughters references in META, we setup ordinary regions
     * first and then split parent regions.
     */
    HTable meta = getMetaTable(tableName, conf);
    for (Path regionDir : ordinaryRegions) {
      setupRegion(regionDir, tmpSplitDir, tableChanged, meta, fs, conf);
    }
    for (Path regionDir : splitParents) {
      setupSplitParent(regionDir, meta, fs, conf);
    }

    // finally, enable the table
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.enableTable(tableName);

    // clean up the unused files
    cleanup(tableDir, fs);
  }

  /*
   * Set up an ordinary table region. This method works in 4 steps:
   * 1. create new region info if the table name is changed
   * 2. rename the region directory if the region info is changed
   * 3. move the split logs into recover edits dir of this region
   * 4. add this region to META and check out the region info
   *
   * @param srcRegionDir original region dir
   * @param tmpSplitDir tmp directory which contains the split logs
   * @param tableChanged whether the table name is changed
   * @param meta meta table
   * @param fs
   * @param conf
   * @throws IOException
   */
  private static void setupRegion(Path srcRegionDir, Path tmpSplitDir,
      boolean tableChanged, HTable meta, FileSystem fs, Configuration conf)
  throws IOException {
    HRegionInfo dumpedInfo =
      readHRegionInfo(srcRegionDir, HRegion.REGIONINFO_FILE, fs);
    HRegionInfo regionInfo = dumpedInfo;
    Path dstRegionDir = srcRegionDir;

    if (tableChanged) {
      // 1. create new region info if table name is changed
      regionInfo = createNewRegionInfo(dumpedInfo);
      // 2. rename region dir
      dstRegionDir = new Path(srcRegionDir.getParent(),
          regionInfo.getEncodedName());
      if (!fs.rename(srcRegionDir, dstRegionDir)) {
        throw new IOException("Failed to rename region");
      }
    }

    // 3. move split log for this region to the dst region dir
    NavigableSet<Path> splits = getSplitLogsForRegion(tmpSplitDir, dumpedInfo, fs);
    HLog.moveSplitLogsForRegion(splits, dumpedInfo, regionInfo, fs, conf);

    // 4. add region to meta and check out the new region info
    addRegionToMETA(regionInfo, null, null, meta);
    HRegion.checkRegioninfoOnFilesystem(fs, regionInfo, dstRegionDir);

    // Since the split daughters region info would be used by split parent
    // regions, add it to the map
    regionInfoMap.put(regionInfo.getRegionName(), regionInfo);
  }

  /*
   * Reconstruct the split parent region.
   *
   * @param srcRegionDir region directory
   * @param meta META table
   * @param fs File System
   * @param conf configuration
   * @throws IOException
   */
  private static void setupSplitParent(final Path srcRegionDir,
      final HTable meta, final FileSystem fs, Configuration conf)
  throws IOException {
    // read the split daughters region info from the dumped files
    HRegionInfo splitA = readHRegionInfo(srcRegionDir,
        ExportSnapshot.SPLITA_REGION_INFO_NAME, fs);
    HRegionInfo splitB = readHRegionInfo(srcRegionDir,
        ExportSnapshot.SPLITB_REGION_INFO_NAME, fs);

    // the split daughters region info have been updated
    // for restored table, find them in the region info map
    splitA = regionInfoMap.get(splitA.getRegionName());
    splitB = regionInfoMap.get(splitB.getRegionName());
    if (splitA == null || splitB == null) {
      throw new IOException("Missing split daughters for parent "
          + srcRegionDir);
    }

    HRegionInfo parent = createParentRegionInfo(splitA, splitB);

    // Rename the parent region dir and update all the hfiles
    // suffix for the split daughters
    String encodedName = parent.getEncodedName();
    Path splitParent = new Path(srcRegionDir.getParent(), encodedName);
    if (!fs.rename(srcRegionDir, splitParent)) {
      throw new IOException("Failed to rename split parent from " +
          srcRegionDir + " to " + splitParent);
    }
    updateSplitParent(splitA, encodedName, fs, conf);
    updateSplitParent(splitB, encodedName, fs, conf);

    addRegionToMETA(parent, splitA, splitB, meta);
    HRegion.checkRegioninfoOnFilesystem(fs, parent, srcRegionDir);

    // clean up splitA and splitB region info in parent region dir
    if (!fs.delete(new Path(splitParent,
        ExportSnapshot.SPLITA_REGION_INFO_NAME), true)) {
      throw new IOException();
    }
    if (!fs.delete(new Path(splitParent,
        ExportSnapshot.SPLITB_REGION_INFO_NAME), true)) {
      throw new IOException();
    }
  }

  /*
   * Split the snapshot edits logs into a tmp directory.
   *
   * @param logsDir edits log directory
   * @param originalTable original table name for the snapshot
   */
  private static void splitLogs(Path logsDir, byte[] originalTable,
      FileSystem fs, Configuration conf)
  throws IOException {
    FileStatus[] logs = fs.listStatus(logsDir);
    Path tmpRoot = getTmpSplitDir(logsDir);
    HLog.splitLog(tmpRoot, logs, originalTable, fs, conf);
  }

  private static Path getTmpSplitDir(Path logsDir) {
    return new Path(logsDir + TMP_SPLIT_DIR);
  }

  /*
   * Create corresponding region info for <code>oldRegionInfo</code>
   * @param oldRegionInfo
   * @return region info whose name is the restored table name
   */
  private static HRegionInfo createNewRegionInfo(HRegionInfo oldRegionInfo) {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (HColumnDescriptor column : oldRegionInfo.getTableDesc().getColumnFamilies()) {
      htd.addFamily(column);
    }
    HRegionInfo regionInfo = new HRegionInfo(htd,
        oldRegionInfo.getStartKey(), oldRegionInfo.getEndKey(),
        oldRegionInfo.isSplit(), oldRegionInfo.getRegionId());
    return regionInfo;
  }

  /*
   * Construct the split parent region info from the split daughters
   * @param splitA
   * @param splitB
   * @return
   */
  private static HRegionInfo createParentRegionInfo(HRegionInfo splitA,
      HRegionInfo splitB) {
    // Regionid is timestamp. Can't be greater than that of daughter
    // else will insert at wrong location in .META. (See HBASE-710).
    long rid = Math.min(splitA.getRegionId(), splitB.getRegionId()) - 1;
    return new HRegionInfo(splitA.getTableDesc(),
        splitA.getStartKey(), splitB.getEndKey(), false, rid);
  }

  /*
   * Get the split recover edits logs for the given region under
   * tmp split directory.
   */
  private static NavigableSet<Path> getSplitLogsForRegion(Path tmpSplitDir,
      HRegionInfo r, FileSystem fs) throws IOException {
    Path tmpRegionDir = HRegion.getRegionDir(tmpSplitDir, r);
    return HLog.getSplitEditFilesSorted(fs, tmpRegionDir);
  }

  /*
   * Get the meta table for the given table
   */
  private static HTable getMetaTable(final String tableName,
      final Configuration conf) throws IOException {
    HTable meta = null;
    if (Bytes.equals(Bytes.toBytes(tableName), HConstants.META_TABLE_NAME)) {
      meta = new HTable(conf, HConstants.ROOT_TABLE_NAME);
    } else {
      meta = new HTable(conf, HConstants.META_TABLE_NAME);
    }
    return meta;
  }

  /*
   * Read the region info from file.
   * @param regionDir region directory which contains the region info file
   * @param fileName region info file name
   * @param fs File System
   * @return
   * @throws IOException
   */
  private static HRegionInfo readHRegionInfo(Path regionDir,
      String fileName, FileSystem fs) throws IOException {
    Path regionInfo = new Path(regionDir, fileName);
    FSDataInputStream in = fs.open(regionInfo);
    HRegionInfo info = new HRegionInfo();
    try {
      info.readFields(in);
    } finally {
      in.close();
    }
    return info;
  }

  /*
   * Read the snapshot info from file
   */
  private static SnapshotDescriptor readSnapshotInfo(Path p,
      FileSystem fs) throws IOException {
    FSDataInputStream in = fs.open(p);
    SnapshotDescriptor sd = new SnapshotDescriptor();
    try {
      sd.readFields(in);
    } finally {
      in.close();
    }
    return sd;
  }

  /*
   * Update the split parent for split daughter region if the parent
   * encoded name has changed.
   *
   * @param daughter daughter region
   * @param parentName encoded parent region name
   * @param root hbase root dir
   * @param fs
   * @throws IOException
   */
  private static void updateSplitParent(HRegionInfo daughter, String parentName,
      FileSystem fs, Configuration conf) throws IOException {
    Path root = FSUtils.getRootDir(conf);
    Path daughterRegion = HRegion.getRegionDir(root, daughter);
    HColumnDescriptor[] columns = daughter.getTableDesc().getColumnFamilies();
    for (HColumnDescriptor column : columns) {
      Path familyDir = new Path(daughterRegion, column.getNameAsString());
      FileStatus[] files = fs.listStatus(familyDir);
      for (FileStatus file : files) {
        String fileName = removeSuffix(file.getPath().getName());
        // update the suffix of hfile to the new parent region name 
        Path p = new Path(familyDir, fileName + "." + parentName);
        if (!fs.rename(file.getPath(), p)) {
          throw new IOException("Failed to rename file from " + file.getPath()
              + " to " + p);
        }
      }
    }
  }

  /*
   * Add the region info as well as split daughters into META
   *
   * @param region region info to be added
   * @param splitA splitA for region
   * @param splitB splitB for region
   * @param meta META table
   * @throws IOException
   */
  private static void addRegionToMETA(HRegionInfo region,
      HRegionInfo splitA, HRegionInfo splitB, HTable meta) throws IOException {
    Put put = new Put(region.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
        HConstants.EMPTY_BYTE_ARRAY);
    put.add(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
        HConstants.EMPTY_BYTE_ARRAY);
    region.setOffline(true);

    if (splitA != null && splitB != null) {
      region.setSplit(true);
      put.add(HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER,
        Writables.getBytes(splitA));
      put.add(HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER,
        Writables.getBytes(splitB));
    }
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(region));
    meta.put(put);
  }

  private static String removeSuffix(String s) {
    return s.substring(0, s.indexOf("."));
  }

  /*
   * Clean up the snapshot info and edit logs which are no longer
   * used by the table.
   */
  private static void cleanup(Path tableDir, FileSystem fs)
  throws IOException {
    // clean up snapshot info file
    Path snapshotInfo = new Path(tableDir,
        SnapshotDescriptor.SNAPSHOTINFO_FILE);
    if (fs.exists(snapshotInfo)) {
      if (!fs.delete(snapshotInfo, false)) {
        throw new IOException("Failed to delete snapshot info file "
            + snapshotInfo);
      }
    }

    // clean up old logs dir
    Path logDir = new Path(tableDir, HConstants.HREGION_LOGDIR_NAME);
    if (fs.exists(logDir)) {
      if (!fs.delete(logDir, true)) {
        throw new IOException("Failed to delete log dir " + logDir);
      }
    }
  }

  /**
   * Import a table from an hbase snapshot.
   *
   * @param conf
   * @param args
   * @return
   */
  static boolean importSnapshot(Configuration conf, String[] args)
  throws IOException, InterruptedException, ClassNotFoundException {
    tableName = args[0];
    inputSnapshot = args[1];

    // checking snapshot directory
    Path snapshotDir = new Path(inputSnapshot);
    FileSystem srcFs = snapshotDir.getFileSystem(conf);
    if (!srcFs.exists(snapshotDir)) {
      throw new IOException("Input snapshot " + inputSnapshot +
          " does not exist");
    } else if (srcFs.isFile(snapshotDir)) {
      throw new IOException("Input snapshot " + inputSnapshot +
          " is not a directory");
    }

    try {
      // 1. copy the snapshot files into table dir by MR job
      prepare(snapshotDir, tableName, conf);
      Job job = createSubmittableJob(conf);
      boolean result = job.waitForCompletion(true);

      if (result) {
        // 2. if all files are copied successfully, set up
        // the table
        setupTable(conf);
        return true;
      }
    } finally {
      // delete tmp dir
      Path tmpDir = new Path(conf.get(TMP_DIR_LABEL));
      if (!tmpDir.getFileSystem(conf).delete(tmpDir, true)) {
        tmpDir.getFileSystem(conf).delete(tmpDir, true); // try again
      }
    }
    return false;
  }

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  private static Job createSubmittableJob(Configuration conf) throws IOException {
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(ImportSnapshot.class);

    job.setInputFormatClass(SnapshotInputFormat.class);
    job.setMapperClass(SnapshotCopier.class);
    job.setNumReduceTasks(0);
    
    Path tmpDir = new Path(conf.get(TMP_DIR_LABEL));
    Path outDir = new Path(tmpDir, "lichongxin");
    FileOutputFormat.setOutputPath(job, outDir);
    return job;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: ImportSnapshot <tablename> <inputsnapshotdir>");
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      usage("Wrong number of arguments: " + otherArgs.length);
      System.exit(-1);
    }
    boolean succeed = importSnapshot(conf, args);
    System.exit(succeed ? 0 : 1);
  }
}
