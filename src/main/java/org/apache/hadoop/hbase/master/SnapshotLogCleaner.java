package org.apache.hadoop.hbase.master;

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
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HSnapshotDescriptor;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Implementation of a log cleaner that checks if a log is still used by
 * snapshots of HBase tables.
 */
public class SnapshotLogCleaner implements LogCleanerDelegate {
  private static final Log LOG = LogFactory.getLog(SnapshotLogCleaner.class);

  private Configuration conf;
  private FileSystem fs;
  private Path rootDir;
  private Set<String> hlogs = new HashSet<String>();

  /**
   * Default constructor. Do nothing
   */
  public SnapshotLogCleaner() {}

  @Override
  public boolean isLogDeletable(Path filePath) {
    String log = filePath.getName();
    if (this.hlogs.contains(log)) {
      return false;
    }

    /*
     * This solution makes every miss very expensive to process since we
     * iterate the snapshot directory to refresh the cache each time.
     * Actually we only have to refresh the cache once in LogsCleaner.chore.
     * Move refreshing this cleaner into LogsCleaner.chore?
     */
    return !refreshHLogsAndSearch(log);
  }

  /**
   * Search through all the hlogs we have under snapshot dir to refresh the cache
   *
   * @param searchedLog log we are searching for, pass null to cache everything
   *                    that's under the directory of snapshot
   * @return true if we should keep the searched log
   */
  private boolean refreshHLogsAndSearch(String searchedLog) {
    this.hlogs.clear();
    final boolean lookForLog = searchedLog != null;

    Path snapshotRoot = HSnapshotDescriptor.getSnapshotRootDir(rootDir);
    PathFilter dirFilter = new FSUtils.DirFilter(fs);

    // refresh the cache with logs under snapshot directory
    try {
      // get snapshots under snapshot directory
      FileStatus[] snapshots = fs.listStatus(snapshotRoot, dirFilter);
      for (FileStatus snapshot : snapshots) {
        Path oldLogDir = new Path(snapshot.getPath(),
            HConstants.HREGION_LOGDIR_NAME);
        // get logs list file for each region server
        FileStatus[] rss = fs.listStatus(oldLogDir);
        for (FileStatus rsLog : rss) {
          FSDataInputStream in = fs.open(rsLog.getPath());
          int num = in.readInt();
          for (int i = 0; i < num; i++) {
            in.readLong();  // the sequence number, we don't use it here
            hlogs.add(in.readUTF());
          }
        }
      }

      if (lookForLog && hlogs.contains(searchedLog)) {
        LOG.debug("Found log under snapshot directory, keeping: " + searchedLog);
        return true;
      }
    } catch (IOException e) {
      // Keep the log file because we don't know if it is still used by 
      // snapshots
      LOG.warn("Failed to get old logs for snapshots!", e);
      return true;
    }

    LOG.debug("Didn't find this log under snapshot dir, deleting: " + searchedLog);
    return false;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      this.fs = FileSystem.get(this.conf);
      this.rootDir = FSUtils.getRootDir(this.conf);

      // initialize the cache
      refreshHLogsAndSearch(null);
    } catch (IOException e) {
      LOG.error(e);
    }
  }
}
