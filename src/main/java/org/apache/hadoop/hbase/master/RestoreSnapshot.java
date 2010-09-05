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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;

/**
 * Instantiated to restore a table from a snapshot
 */
public class RestoreSnapshot extends SnapshotOperation {
  private static final Log LOG = LogFactory.getLog(RestoreSnapshot.class);

  private Path tableDir;

  // HLogs dumped at the creation time of the snapshot
  // Mapping: region server name -> dumped logs on this region server
  protected Map<String, Set<String>> hlogs = new HashMap<String, Set<String>>();

  RestoreSnapshot(HMaster master, byte[] snapshotName) throws IOException {
    super(master, snapshotName);
    this.tableDir = new Path(master.getRootDir(), hsd.getTableNameAsString());

    checkTableExistence();
    if (!fs.mkdirs(tableDir)) {
      LOG.warn("Failed to create table dir " + tableDir);
      throw new IOException("Failed to create table dir " + tableDir);
    }
  }

  /*
   * Verify if the table exists in META
   */
  private void checkTableExistence() throws IOException {
    byte [] tableNameMetaStart =
      Bytes.toBytes(Bytes.toString(hsd.getTableName()) + ",,");
    Scan scan =
      new Scan(tableNameMetaStart).addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner s = meta.getScanner(scan);
    try {
      Result r = s.next();
      if (r != null && !r.isEmpty()) {
        HRegionInfo info = master.getHRegionInfo(r.getRow(), r);
        if (Bytes.equals(info.getTableDesc().getName(), hsd.getTableName())) {
          LOG.info("Table " + hsd.getTableNameAsString() + " already exists");
          throw new TableExistsException("Table " + hsd.getTableNameAsString()
              + " already exists");
        }
      }
    } finally {
      s.close();
    }
  }

  @Override
  protected void beforeProcess() throws IOException {
    // load the dumped HLogs
    Path logsDir = new Path(snapshotDir, HConstants.HREGION_LOGDIR_NAME);
    FileStatus[] logLists = fs.listStatus(logsDir);
    for (FileStatus logListFile : logLists) {
      // each log list file contains a list of the logs on a region server
      // and the file name is the region server's name
      FSDataInputStream in = fs.open(logListFile.getPath());
      try {
        int logCount = in.readInt();
        Set<String> logs = new TreeSet<String>();
        for (int i = 0; i < logCount; i++) {
          logs.add(in.readUTF());
        }
        hlogs.put(logListFile.getPath().getName(), logs);
      } finally {
        in.close();
      }
    }
  }

  @Override
  protected void afterProcess() throws IOException {
    // when all regions are in place, split the logs under .logs dir
    splitLogs();
    // at last, try to enable the table
    master.enableTable(hsd.getTableName());
  }

  @Override
  protected void processRegion(Path regionDir) throws IOException {
    // 1. copy all files for this region recursively to destination
    // region dir
    Path dstRegionDir = new Path(tableDir, regionDir.getName());
    FileUtil.copy(fs, regionDir, fs, dstRegionDir, false, fs.getConf());

    // 2. add new region to META
    Path info = new Path(regionDir, HRegion.REGIONINFO_FILE);
    FSDataInputStream in = fs.open(info);
    HRegionInfo regionInfo = new HRegionInfo();
    regionInfo.readFields(in);
    in.close();
    regionInfo.setOffline(true);
    Put put = new Put(regionInfo.getRegionName());
    put.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(regionInfo));
    meta.put(put);
  }

  /*
   * Split the dumped commit logs.
   */
  private void splitLogs() throws IOException {
    List<FileStatus> logfiles = new ArrayList<FileStatus>();
    for (String server : hlogs.keySet()) {
      Set<String> rsLogs = hlogs.get(server);
      for (String logName : rsLogs) {
        Path logCurPos = HLog.getLogCurrentPosition(server, logName,
            master.getConfiguration());
        logfiles.add(fs.getFileStatus(logCurPos));
      }
    }

    HLog.splitLog(master.getRootDir(),
        logfiles.toArray(new FileStatus[logfiles.size()]), hsd.getTableName(),
        fs, master.getConfiguration());
  }
}
