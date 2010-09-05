package org.apache.hadoop.hbase.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SnapshotDescriptor;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.master.SnapshotOperation;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSUtils.DirFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

/**
 * A Map-reduce program to export an hbase snapshot to a different file system
 */
public class ExportSnapshot {
  static final String NAME = "exportsnapshot";

  static final String TMP_DIR_LABEL = NAME + ".tmp.dir";
  static final String DST_DIR_LABEL = NAME + ".dst.dir";
  static final String SRC_LIST_LABEL = NAME + ".src.list";
  static final String TOTAL_SIZE_LABEL = NAME + ".total.size";
  static final String BYTES_PER_MAP_LABEL = NAME + ".bytes.per.map";
  static final String FILE_RETRIES_LABEL = NAME + ".file.retries";

  static final long BYTES_PER_MAP =  256 * 1024 * 1024;
  static final int DEFAULT_FILE_RETRIES = 3;

  static final String SNAPSHOT_EXPORT_SUFFIX = ".exp";
  static final String SPLIT_PARENT_SUFFIX = ".split";
  static final String SPLITA_REGION_INFO_NAME = ".splitA";
  static final String SPLITB_REGION_INFO_NAME = ".splitB";

  private static String snapshotName;
  private static String outputDir;

  /**
   * An input/output pair of filenames.
   */
  static class FilePair implements Writable, Comparable<FilePair> {
    FileStatus input = new FileStatus();
    String output;

    FilePair() { }

    FilePair(FileStatus input, String output) {
      this.input = input;
      this.output = output;
    }

    public void readFields(DataInput in) throws IOException {
      input.readFields(in);
      output = Text.readString(in);
    }

    public void write(DataOutput out) throws IOException {
      input.write(out);
      Text.writeString(out, output);
    }

    @Override
    public String toString() {
      return input + " : " + output;
    }

    @Override
    public int compareTo(FilePair o) {
      // two file pair are different only if the output path is different
      return output.compareTo(o.output);
    }
  }

  /**
   * Prepare snapshot src files list and the job configuration for export.
   *
   * @param snapshotDir snapshot directory
   * @param srcFs file system for the snapshot
   * @param conf
   * @throws IOException
   */
  static void prepare(Path snapshotDir, FileSystem srcFs, Configuration conf)
  throws IOException {
    Path dstDir = new Path(outputDir);
    FileSystem dstFs = dstDir.getFileSystem(conf);
    // create the export destination
    Path expdir = checkAndCreateExportDir(dstDir, snapshotName, dstFs);
    conf.set(DST_DIR_LABEL, expdir.toString());

    Path tmpDir = new Path(expdir, ".tmp");
    if (!dstFs.mkdirs(tmpDir)) {
      throw new IOException("Failed to create temp dir " + tmpDir);
    }
    conf.set(TMP_DIR_LABEL, tmpDir.toString());

    Path srcFileList = new Path(tmpDir, "_src_files_list");
    conf.set(SRC_LIST_LABEL, srcFileList.toString());
    SequenceFile.Writer writer = SequenceFile.createWriter(dstFs, conf,
        srcFileList, LongWritable.class, FilePair.class,
        SequenceFile.CompressionType.NONE);

    // get the snapshot files to be exported and write them to a file
    long totalBytes = 0;
    Set<FilePair> snapshotFiles =
      prepareSnapshotFiles(snapshotDir, srcFs, expdir, conf);
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
    conf.setLong(TOTAL_SIZE_LABEL, totalBytes);
  }

  /*
   * Check and create the export destination directory. It would be
   * <snapshotname>.exp under <code>outputDir</code>.
   *
   * @param outputDir output directory for the exporting
   * @param snapshotName name of the snapshot
   * @param dstFs destination file system
   * @return path to the export destination directory
   * @throws IOException
   */
  private static Path checkAndCreateExportDir(Path outputDir,
      String snapshotName, FileSystem dstFs) throws IOException {
    if (dstFs.exists(outputDir)) {
      FileStatus dstStatus = dstFs.getFileStatus(outputDir);
      if (!dstStatus.isDir()) {
        throw new IOException("OutputDir " + outputDir + " is not a directory");
      }
    } else if (!dstFs.mkdirs(outputDir)) {
      throw new IOException("Failed to create output dir " + outputDir);
    }

    // create the snapshot export dir
    Path expdir = new Path(outputDir, snapshotName + SNAPSHOT_EXPORT_SUFFIX);
    if (dstFs.exists(expdir)) {
      throw new IOException("Snapshot export dir " + expdir + " already exists");
    } else if (!dstFs.mkdirs(expdir)) {
      throw new IOException("Failed to create snapshot export dir " + expdir);      
    }
    return expdir;
  }

  /*
   * Prepare snapshot files to be exported.
   *
   * @param snapshotDir snapshot directory
   * @param srcFs source file system for the snapshot
   * @param dstDir export destination directory
   * @param conf configuration
   * @return a set of files for the snapshot to be exported
   * @throws IOException
   */
  private static Set<FilePair> prepareSnapshotFiles(final Path snapshotDir,
      final FileSystem srcFs, final Path dstDir, final Configuration conf)
  throws IOException {
    NavigableSet<FilePair> snapshotFiles = new TreeSet<FilePair>();

    // add snapshot info file
    Path snapshotInfo = new Path(snapshotDir, SnapshotDescriptor.SNAPSHOTINFO_FILE);
    FileStatus infoFile = srcFs.getFileStatus(snapshotInfo);
    Path dstInfo = new Path(dstDir, SnapshotDescriptor.SNAPSHOTINFO_FILE);
    snapshotFiles.add(new FilePair(infoFile, dstInfo.toString()));

    // add snapshot old logs
    Path logsDir = new Path(snapshotDir, HConstants.HREGION_LOGDIR_NAME);
    Set<FilePair> logFiles = getSnapshotLogs(logsDir, srcFs, dstDir, conf);
    snapshotFiles.addAll(logFiles);

    // add snapshot region files
    FileStatus[] regions = srcFs.listStatus(snapshotDir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          Matcher m = SnapshotOperation.REGION_NAME_PATTERN.matcher(p.getName());
          result = srcFs.getFileStatus(p).isDir() && m.matches();
        } catch (IOException e) {
          e.printStackTrace();
        }
        return result;
      }
    });
    for (FileStatus region : regions) {
      Set<FilePair> regionFiles =
        getSnapshotRegionFiles(region.getPath(), srcFs, dstDir, conf);
      snapshotFiles.addAll(regionFiles);
    }

    return snapshotFiles;
  }

  /*
   * Get the edits log files for snapshot.
   * For snapshot, it just keeps a list of log file names for each
   * region server under the log directory. This method would get
   * the actual path of these log files.
   *
   * @param logsDir snapshot log directory
   * @param srcFs source file system for the snapshot
   * @param dstDir export destination directory
   * @param conf configuration
   * @return a set of files for snapshot edit logs
   * @throws IOException
   */
  private static Set<FilePair> getSnapshotLogs(final Path logsDir,
      final FileSystem srcFs, final Path dstDir, final Configuration conf)
  throws IOException {
    NavigableSet<FilePair> snapshotLogs = new TreeSet<FilePair>();
    Path dstLogsDir = new Path(dstDir, HConstants.HREGION_LOGDIR_NAME);

    FileStatus[] logLists = srcFs.listStatus(logsDir);
    for (FileStatus logListFile : logLists) {
      // each log list file contains a list of the logs on a region server
      // and the file name is the region server's name
      FSDataInputStream in = srcFs.open(logListFile.getPath());
      String serverName = logListFile.getPath().getName();
      try {
        int logCount = in.readInt();
        for (int i = 0; i < logCount; i++) {
          String logName = in.readUTF();
          // get the current position of the log file
          Path curPos = HLog.getLogCurrentPosition(
              serverName, logName, conf);
          Path dstPos = new Path(dstLogsDir, logName);
          snapshotLogs.add(new FilePair(
              srcFs.getFileStatus(curPos), dstPos.toString()));
        }
      } finally {
        in.close();
      }
    }
    return snapshotLogs;
  }

  /*
   * Get all the region files to be exported, including region info file
   * as well as hfiles. Since hfiles are all reference files for a snapshot,
   * the actual referred files would be obtained for exporting.
   * 
   * @param regionDir region home directory
   * @param srcFs source file system for the snapshot
   * @param dstDir export destination directory
   * @return a set of files for this snapshot region
   * @throws IOException
   */
  private static Set<FilePair> getSnapshotRegionFiles(final Path regionDir,
      final FileSystem srcFs, final Path dstDir, Configuration conf) throws IOException {
    NavigableSet<FilePair> regionFiles = new TreeSet<FilePair>();
    Path dstRegion = new Path(dstDir, regionDir.getName());

    // add region info file
    FileStatus regionInfo = srcFs.getFileStatus(
        new Path(regionDir, HRegion.REGIONINFO_FILE));
    Path dstRegionInfo = new Path(dstRegion, HRegion.REGIONINFO_FILE);
    regionFiles.add(new FilePair(regionInfo, dstRegionInfo.toString()));

    boolean isSplit = false;
    Path splitParent = null;
    String splitDaughter = null;

    FileStatus[] families = srcFs.listStatus(regionDir, new DirFilter(srcFs));
    for (FileStatus family : families) {
      Path dstFamily = new Path(dstRegion, family.getPath().getName());
      FileStatus[] refFiles = srcFs.listStatus(family.getPath());
      for (FileStatus refFile : refFiles) {
        // all snapshot hfiles should be reference files
        assert(StoreFile.isReference(refFile.getPath()));
        Reference r = Reference.read(srcFs, refFile.getPath());
        Path referredFile = getReferredFile(refFile.getPath(), r, srcFs, conf);
        if (r.isEntireRange()) {
          // for entire reference files, just export the referred file
          Path dstFile = new Path(dstFamily, referredFile.getName());
          regionFiles.add(new FilePair(
              srcFs.getFileStatus(referredFile), dstFile.toString()));
        } else {
          // for half reference files after split, both reference file and
          // referred file would be needed when compaction, thus we need
          // reconstruct the parent region for exporting
          if (!isSplit) {
            isSplit = true;
            String region = referredFile.getParent().getParent().getName();
            // add the suffix to indicate this region is a split parent
            splitParent = new Path(dstDir, region + SPLIT_PARENT_SUFFIX);
            if (!r.isTopFileRange()) {
              // BOTTOM
              splitDaughter = SPLITA_REGION_INFO_NAME;
            } else {
              // TOP
              splitDaughter = SPLITB_REGION_INFO_NAME;
            }
          }
          // export the half reference file directly
          Path dstFile = new Path(dstFamily, refFile.getPath().getName());
          regionFiles.add(new FilePair(refFile, dstFile.toString()));

          // export the referred file to the parent region
          Path dstSplitFile = new Path(new Path(splitParent,
              family.getPath().getName()), referredFile.getName());
          regionFiles.add(new FilePair(srcFs.getFileStatus(referredFile),
              dstSplitFile.toString()));
        }
      }
    }

    // also export the daughter region info into parent region dir
    // this would be used to reconstruct the parent region meta
    if (isSplit) {
      Path splitDaughterInfo = new Path(splitParent, splitDaughter);
      regionFiles.add(new FilePair(regionInfo, splitDaughterInfo.toString()));
    }

    return regionFiles;
  }

  /*
   * Get the actual referred file for a reference file <code>p</code>.
   * 
   * @param p reference file path
   * @param ref reference
   * @param fs file system
   * @param conf Configuration
   * @return the path to the referred file
   * @throws IOException
   */
  private static Path getReferredFile(final Path p, final Reference ref,
      final FileSystem fs, final Configuration conf) throws IOException {
    Path referredFile = StoreFile.getReferredToFile(p, ref, conf);
    if (!fs.exists(referredFile)) {
      // this file has been archived, find it in the archive dir
      Path archiveDir = new Path(FSUtils.getRootDir(conf),
          HConstants.ARCHIVE_DIR);
      referredFile = FSUtils.getHFileArchivePath(referredFile, archiveDir);
      if (!fs.exists(referredFile)) {
        throw new IOException("Could not find the reference file: "
            + referredFile);
      }
    }
    return referredFile;
  }

  /**
   * InputFormat of a export snapshot job responsible for generating splits
   * of the snapshot src file list.
   */
  static class SnapshotInputFormat extends InputFormat<LongWritable, FilePair> {
    @Override
    public RecordReader<LongWritable, FilePair> createRecordReader(
        InputSplit split, TaskAttemptContext context) throws IOException,
        InterruptedException {
      return new SequenceFileRecordReader<LongWritable, FilePair>();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
        InterruptedException {
      Configuration conf = context.getConfiguration();
      long totalSize = conf.getLong(TOTAL_SIZE_LABEL, -1);
      String srcFileList = conf.get(SRC_LIST_LABEL, "");
      if (totalSize < 0 || "".equals(srcFileList)) {
        throw new RuntimeException("Invalid metadata: #total_size(" +
            totalSize + ") listuri(" + srcFileList + ")");
      }
      // target size for a map
      final long targetSize = conf.getLong(BYTES_PER_MAP_LABEL, BYTES_PER_MAP);
      Path src = new Path(srcFileList);
      FileSystem fs = src.getFileSystem(conf);
      FileStatus srcst = fs.getFileStatus(src);

      List<InputSplit> splits = new ArrayList<InputSplit>();
      LongWritable key = new LongWritable();
      FilePair value = new FilePair();
      long pos = 0L;
      long last = 0L;
      long acc = 0L;
      long cbrem = srcst.getLen();
      SequenceFile.Reader sl = new SequenceFile.Reader(fs, src, conf);
      try {
        for (; sl.next(key, value); last = sl.getPosition()) {
          // if adding this split would put this split past the target size,
          // cut the last split and put this next file in the next split.
          if (acc + key.get() > targetSize && acc != 0) {
            long splitSize = last - pos;
            splits.add(new FileSplit(src, pos, splitSize, (String[])null));
            cbrem -= splitSize;
            pos = last;
            acc = 0L;
          }
          acc += key.get();
        }
      }
      finally {
        sl.close();
      }
      if (cbrem != 0) {
        splits.add(new FileSplit(src, pos, cbrem, (String[])null));
      }

      return splits;
    }
  }

  /**
   * Mapper to copy files between file system
   */
  static class SnapshotCopier extends
      Mapper<LongWritable, FilePair, WritableComparable<?>, Text> {
    private FileSystem dstFs;

    @Override
    protected void setup(Context context) {
      Configuration conf = context.getConfiguration();
      Path dstDir = new Path(conf.get(DST_DIR_LABEL, ""));
      try {
        dstFs = dstDir.getFileSystem(conf);
      } catch (IOException ex) {
        throw new RuntimeException("Unable to get the named file system.", ex);
      }
    }

    /**
     * Map method. Copies one file from source file system to
     * destination.
     *
     * @param key src len
     * @param value FilePair (FileStatus src, Path dst)
     * @param context Log of failed copies
     */
    @Override
    protected void map(LongWritable key, FilePair value,
      Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      final FileStatus src = value.input;
      final Path dst = new Path(value.output);
      // max tries to copy when validation of copy fails
      final int maxRetries =
        conf.getInt(FILE_RETRIES_LABEL, DEFAULT_FILE_RETRIES);

      int retryCnt = 1;
      for (; retryCnt <= maxRetries; retryCnt++) {
        try {
          copy(src, dst, conf);
          break;  // copy successfully
        } catch (IOException e) {
          if (retryCnt >= maxRetries) {
            // no more retries... Give up
            throw new IOException("Copy of file failed even with "
                + retryCnt + " tries.", e);
          }
        }
      }
    }

    /*
     * Copy a file to a destination and validate the copy by
     * checking the checksums
     *
     * @param srcstat src path and metadata
     * @param dstpath dst path
     * @param conf
     * @throws IOException if copy fails(even if the validation of copy fails)
     */
    private void copy(FileStatus src, Path dst, Configuration conf)
    throws IOException {
      if (src.isDir()) {
        if (dstFs.exists(dst)) {
          if (dstFs.isFile(dst)) {
            throw new IOException("Failed to mkdirs: " + dst +" is a file.");
          }
        }
        else if (!dstFs.mkdirs(dst)) {
          throw new IOException("Failed to mkdirs " + dst);
        }
        // mkdirs does not currently return an error when the directory
        // already exists
        return;
      }

      System.out.print("====================================");
      System.out.println("TMP DIR: " + conf.get(TMP_DIR_LABEL));
      System.out.println(dst.getName());
      System.out.print("====================================");
      Path tmpfile = new Path(conf.get(TMP_DIR_LABEL), dst.getName());
      // do the actual copy to tmpfile
      long bytesCopied = doCopyFile(src, tmpfile, conf);

      if (bytesCopied != src.getLen()) {
        throw new IOException("File size not matched: copied "
            + bytesString(bytesCopied) + " to tmpfile (=" + tmpfile
            + ") but expected " + bytesString(src.getLen()) 
            + " from " + src.getPath());        
      }

      // create parent dir and rename
      if (dstFs.exists(dst) &&
          dstFs.getFileStatus(dst).isDir()) {
        throw new IOException(dst + " is a directory");
      }
      if (!dstFs.mkdirs(dst.getParent())) {
        throw new IOException("Failed to create parent dir: " + dst.getParent());
      }
      rename(tmpfile, dst);

      if (!validateCopy(src, dst, conf)) {
        dstFs.delete(dst, false);
        throw new IOException("Validation of copy of file "
            + src.getPath() + " failed.");
      }
    }

    static String bytesString(long b) {
      return b + " bytes (" + StringUtils.humanReadableInt(b) + ")";
    }

    /*
     * Copies single file to the path specified by tmpfile.
     *
     * @param src  src path and metadata
     * @param tmpfile  temporary file to which copy is to be done
     * @param conf
     * @return Number of bytes copied
     */
    private long doCopyFile(FileStatus src, Path tmpfile,
        Configuration conf) throws IOException {
      Path srcPath = src.getPath();
      // delete the tmpfile if it exists
      if (dstFs.exists(tmpfile)) {
        dstFs.delete(tmpfile, false);
      }
      // open src file
      FSDataInputStream in = srcPath.getFileSystem(conf).open(srcPath);
      // open tmp file
      int sizeBuf = conf.getInt("copy.buf.size", 128 * 1024);
      FSDataOutputStream out = dstFs.create(tmpfile, true, sizeBuf);
      long bytesCopied = 0L;
      try {
        // copy file
        byte[] buffer = new byte[sizeBuf];
        for(int bytesRead; (bytesRead = in.read(buffer)) >= 0; ) {
          out.write(buffer, 0, bytesRead);
          bytesCopied += bytesRead;
        }
      } finally {
        in.close();
        out.close();
      }
      return bytesCopied;
    }

    /*
     * rename tmp to dst, delete dst if already exists
     */
    private void rename(Path tmp, Path dst) throws IOException {
      try {
        if (dstFs.exists(dst)) {
          dstFs.delete(dst, true);
        }
        if (!dstFs.rename(tmp, dst)) {
          throw new IOException();
        }
      }
      catch(IOException cause) {
        throw (IOException)new IOException("Fail to rename tmp file (=" + tmp 
            + ") to destination file (=" + dst + ")").initCause(cause);
      }
    }

    /*
     * Validates copy by checking the sizes of files first and then
     * checksums, if the filesystems support checksums.
     *
     * This method first checks size of the files. If the files have
     * different sizes, return false. Otherwise the file checksums
     * will be compared. When file checksum is not supported in any
     * of file systems, two files are considered as the same if they
     * have the same size.
     */
    private boolean validateCopy(FileStatus srcStatus,
        Path dst, Configuration conf) throws IOException {
      FileSystem srcFs = srcStatus.getPath().getFileSystem(conf);
      FileStatus dstStatus;
      try {
        dstStatus = dstFs.getFileStatus(dst);
      } catch(FileNotFoundException fnfe) {
        return false;
      }

      // same length?
      if (srcStatus.getLen() != dstStatus.getLen()) {
        return false;
      }

      // get src checksum
      final FileChecksum srccs;
      try {
        srccs = srcFs.getFileChecksum(srcStatus.getPath());
      } catch(FileNotFoundException fnfe) {
        /*
         * Two possible cases:
         * (1) src existed once but was deleted between the time period that
         *     srcstatus was obtained and the try block above.
         * (2) srcfs does not support file checksum and (incorrectly) throws
         *     FNFE, e.g. some previous versions of HftpFileSystem.
         * For case (1), it is okay to return true since src was already deleted.
         * For case (2), true should be returned.  
         */
        return true;
      }

      // compare checksums
      try {
        final FileChecksum dstcs = dstFs.getFileChecksum(dstStatus.getPath());
        // return true if checksum is not supported
        // (i.e. some of the checksums is null)
        return srccs == null || dstcs == null || srccs.equals(dstcs);
      } catch(FileNotFoundException fnfe) {
        return false;
      }
    }
  }

  /**
   * @param conf Job Configuration
   * @param args arguments
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  static boolean exportSnapshot(Configuration conf, String[] args)
  throws IOException, InterruptedException, ClassNotFoundException {
    snapshotName = args[0];
    outputDir = args[1];

    // check snapshot dir
    Path hbseRoot = new Path(conf.get(HConstants.HBASE_DIR));
    Path snapshotDir = SnapshotDescriptor.getSnapshotDir(hbseRoot,
        Bytes.toBytes(snapshotName));
    FileSystem srcFs = snapshotDir.getFileSystem(conf);
    if (!srcFs.exists(snapshotDir)) {
      throw new IOException("Snapshot " + snapshotName + " does not exist");
    }

    try {
      prepare(snapshotDir, srcFs, conf);
      Job job = createSubmittableJob(conf);
      return job.waitForCompletion(true);
    } finally {
      // delete tmp dir
      Path tmpDir = new Path(conf.get(TMP_DIR_LABEL));
      if (!tmpDir.getFileSystem(conf).delete(tmpDir, true)) {
        tmpDir.getFileSystem(conf).delete(tmpDir, true); // try again
      }
    }
  }

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  private static Job createSubmittableJob(Configuration conf) throws IOException {
    Job job = new Job(conf, NAME + "_" + snapshotName);
    job.setJarByClass(ExportSnapshot.class);

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
    System.err.println("Usage: ExportSnapshot <snapshotname> <outputdir>");
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
    boolean succeed = exportSnapshot(conf, args);
    System.exit(succeed ? 0 : 1);
  }
}
