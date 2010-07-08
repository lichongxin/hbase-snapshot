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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HSnapshotDescriptor;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Instantiated to create a snapshot.
 */
public class TableSnapshot extends TableOperation {
  private final Log LOG = LogFactory.getLog(this.getClass());
  
  protected final HSnapshotDescriptor snapshot;
  protected final Path snapshotDir;
  
  protected final HTable metaTable;
  
  TableSnapshot(final HMaster master, final HSnapshotDescriptor snapshot) throws IOException {
    super(master, snapshot.getTableName());
    
    this.snapshot = snapshot;
    
    // dir for a snapshot is {rootDir}/.snapshot/<snapshotName>
    this.snapshotDir = new Path(new Path(master.getRootDir(), HConstants.SNAPSHOT_DIR), 
        Bytes.toString(snapshot.getSnapshotName()));
    
    if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
      metaTable = new HTable(HConstants.ROOT_TABLE_NAME);
    } else {
      metaTable = new HTable(HConstants.META_TABLE_NAME);
    }
  }

  @Override
  protected void processScanItem(String serverName, HRegionInfo info)
    throws IOException {
    // only snapshot of offline tables could be created by the master
    if (isEnabled(info)) {
      LOG.debug("Region still enabled: " + info.toString());
      throw new TableNotDisabledException(tableName);
    }
  }
  
  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
    throws IOException {
    for (HRegionInfo i: unservedRegions) {
      if (!Bytes.equals(this.tableName, i.getTableDesc().getName())) {
        // Don't backup regions that are not from our table.
        continue;
      }
      
      // backup the region files, including metadata and HFiles
      backupRegion(i);
    }
  }
  
  private void backupRegion(final HRegionInfo info) throws IOException {
    Path regionDir = HRegion.getRegionDir(master.getRootDir(), info);
    Path backupDir = new Path(snapshotDir, info.getEncodedName());
    // backup meta
    checkRegioninfoOnFilesystem(backupDir, info);
    
    // back HFiles
    FileSystem fs = master.getFileSystem();
    for (HColumnDescriptor c : info.getTableDesc().getFamilies()) {
      Path familyDir = new Path(regionDir, c.getNameAsString());
      Path refDir = new Path(backupDir, c.getNameAsString());
      FileStatus[] hfiles = fs.listStatus(familyDir);
      for (FileStatus file : hfiles) {
        FSUtils.createFileReference(fs, file.getPath(), refDir);
        
        // update reference count for these HFiles
        updateReferenceCount(info, file);
      }
    }
  }
  
  /*
   * Write out the info file under the backup directory
   */
  private void checkRegioninfoOnFilesystem(final Path backupDir, final HRegionInfo info) throws IOException {
    
  }
  
  
  private void updateReferenceCount(final HRegionInfo info, final FileStatus file) throws IOException {
    byte[] row = Bytes.add(HConstants.SNAPSHOT_META_PREFIX, info.getRegionName());
    byte[] family = Bytes.toBytes("snapshot");  // shoud be moved to constants
    byte[] qulifier = Bytes.toBytes(file.getPath().getName());
    metaTable.incrementColumnValue(row, family, qulifier, 1);
  }
}
