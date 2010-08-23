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
import org.apache.hadoop.hbase.SnapshotDescriptor;
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

  public SnapshotLogCleaner() {}

  @Override
  public boolean isLogDeletable(Path filePath) {
    String log = filePath.getName();
    if (this.hlogs.contains(log)) {
      return false;
    }
    return true;
  }

  /**
   * Refresh the HLogs cache to get the current logs which are used
   * by snapshots under snapshot directory
   *
   * @throws IOException
   */
  public void refreshHLogsCache() throws IOException {
    this.hlogs.clear();

    Path snapshotRoot = SnapshotDescriptor.getSnapshotRootDir(rootDir);
    PathFilter dirFilter = new FSUtils.DirFilter(fs);

    // iterate the snapshots under snapshot directory
    try {
      FileStatus[] snapshots = fs.listStatus(snapshotRoot, dirFilter);
      for (FileStatus snapshot : snapshots) {
        Path oldLogDir = new Path(snapshot.getPath(),
            HConstants.HREGION_LOGDIR_NAME);
        // get logs list file for each region server
        FileStatus[] rss = fs.listStatus(oldLogDir);
        if (rss != null) {
          for (FileStatus rsLog : rss) {
            FSDataInputStream in = fs.open(rsLog.getPath());
            int num = in.readInt();
            for (int i = 0; i < num; i++) {
              hlogs.add(in.readUTF());
            }
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to refresh hlogs cache for snapshots!", e);
      throw e;
    }
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
      refreshHLogsCache();
    } catch (IOException e) {
      LOG.error(e);
    }
  }
}
