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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HSnapshotDescriptor;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Instantiated to create a snapshot by the master
 */
public class TableSnapshot extends TableOperation {
  private final Log LOG = LogFactory.getLog(this.getClass());
  
  protected final HSnapshotDescriptor snapshot;
  protected final FileSystem fs;
  protected final Path snapshotDir;
  
  protected final HTable metaTable;
  
  TableSnapshot(final HMaster master, final HSnapshotDescriptor snapshot) throws IOException {
    super(master, snapshot.getTableName());
    
    this.snapshot = snapshot;
    this.fs = master.getFileSystem();
    
    this.snapshotDir = HSnapshotDescriptor.getSnapshotDir(master.getRootDir(),
        snapshot.getSnapshotName());
  
    if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
      // we need update the ROOT table here
      metaTable = new HTable(HConstants.ROOT_TABLE_NAME);
    } else {
      metaTable = new HTable(HConstants.META_TABLE_NAME);
    }
  }

  @Override
  protected void processScanItem(String serverName, HRegionInfo info)
    throws IOException {
    // table must be offline
    if (isEnabled(info)) {
      LOG.debug("Region still enabled: " + info.toString());
      throw new TableNotDisabledException(tableName);
    }
  }
  
  @Override
  protected void postProcessMeta(MetaRegion m, HRegionInterface server)
    throws IOException {
    for (HRegionInfo i: unservedRegions) {
      // Don't backup regions that are not from our table.
      if (!Bytes.equals(this.tableName, i.getTableDesc().getName())) {
        continue;
      }
      // Skipping split parent region
      if (i.isOffline() && i.isSplit()) {
        continue;
      }
      
      backupRegion(i);
    }
  }
  
  /*
   * Backup the region files, including metadata and HFiles
   */
  private void backupRegion(final HRegionInfo info) throws IOException {
    Path srcRegionDir = HRegion.getRegionDir(master.getRootDir(), info);
    Path dstRegionDir = new Path(snapshotDir, info.getEncodedName());
    // 1. backup meta
    checkRegioninfoOnFilesystem(dstRegionDir, info);
    
    FileSystem fs = master.getFileSystem();
    for (HColumnDescriptor c : info.getTableDesc().getFamilies()) {
      Path srcFamilyDir = new Path(srcRegionDir, c.getNameAsString());
      Path dstFamilyDir = new Path(dstRegionDir, c.getNameAsString());
      
      FileStatus[] hfiles = fs.listStatus(srcFamilyDir);
      for (FileStatus file : hfiles) {
        Path dstFile = null;
        // 2. back HFiles
        if (StoreFile.isReference(file.getPath())) {
          // copy the file directly if it is already a half reference file after split
          dstFile = new Path(dstFamilyDir, file.getPath().getName());
          FileUtil.copy(fs, file.getPath(), fs, dstFile, false, 
              master.getConfiguration());
        } else {
          dstFile = FSUtils.createFileReference(fs, file.getPath(), dstFamilyDir);
        }
        // 3. update reference count for backup HFile
        updateReferenceCountInMeta(info, file.getPath(), dstFile);
      }
    }
    
    
  }
  
  /*
   * Write out the region info file under the passed dstDir directory
   */
  private void checkRegioninfoOnFilesystem(final Path dstDir, final HRegionInfo info)
    throws IOException {
      Path regioninfo = new Path(dstDir, HRegion.REGIONINFO_FILE);
      FSDataOutputStream out = this.fs.create(regioninfo, true);
      try {
        info.write(out);
        out.write('\n');
        out.write('\n');
        out.write(Bytes.toBytes(info.toString()));
      } finally {
        out.close();
      }
  }
  
  /*
   * Update the reference count for the backup file by one
   */
  private void updateReferenceCountInMeta(final HRegionInfo info, Path backupFile, 
      Path refFile) throws IOException {
    try {
      // reference count information is not stored in the original meta row for this 
      // region but in a separate row whose row key is prefixed by ".SNAPSHOT."
      // This can be seen as a virtual table ".SNAPSHOT."
      byte[] row = Bytes.add(HConstants.SNAPSHOT_ROW_PREFIX, info.getRegionName());
      metaTable.incrementColumnValue(row, HConstants.SNAPSHOT_FAMILY, 
          Bytes.toBytes(backupFile.getName()), 1);
    } catch (IOException e) {
      LOG.debug("Failed to update reference count for " + backupFile);
      // delete reference file if updating reference count in META fails
      fs.delete(refFile, true);
      throw e;
    }
  }
}
