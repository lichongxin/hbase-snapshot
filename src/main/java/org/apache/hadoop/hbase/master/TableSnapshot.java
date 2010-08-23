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
import org.apache.hadoop.hbase.SnapshotDescriptor;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Instantiated to create snapshot for table which is offline.
 * Since this table is offline and not served by any region server,
 * this kind of snapshot is totally driven by this class in master.
 */
public class TableSnapshot extends TableOperation {
  private final Log LOG = LogFactory.getLog(this.getClass());

  protected final SnapshotDescriptor snapshot;
  protected final FileSystem fs;
  // directory which holds this snapshot
  protected final Path snapshotDir;

  protected final HTable metaTable;

  TableSnapshot(final HMaster master, final SnapshotDescriptor snapshot)
    throws IOException {
    super(master, snapshot.getTableName());

    this.snapshot = snapshot;
    this.fs = master.getFileSystem();

    this.snapshotDir = SnapshotDescriptor.getSnapshotDir(master.getRootDir(),
        snapshot.getSnapshotName());
    if (!fs.mkdirs(snapshotDir)) {
      LOG.warn("Could not create snapshot directory: " + snapshotDir);
      throw new IOException("Could not create snapshot directory: "
          + snapshotDir);
    }

    if (Bytes.equals(tableName, HConstants.META_TABLE_NAME)) {
      // If the snapshot table is meta table, we need update the ROOT table here
      metaTable = new HTable(master.getConfiguration(),
          HConstants.ROOT_TABLE_NAME);
    } else {
      metaTable = new HTable(master.getConfiguration(),
          HConstants.META_TABLE_NAME);
    }

    // dump snapshot info under snapshot dir
    SnapshotDescriptor.write(snapshot, snapshotDir, fs);
  }

  @Override
  protected void processScanItem(String serverName, HRegionInfo info)
    throws IOException {
    // table must be offline
    if (isEnabled(info)) {
      LOG.info("Region still enabled: " + info.toString());
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

      regionSnapshot(i);
    }
  }

  /*
   * Create snapshot for a single region, i.e. dump the region meta and
   * create reference files for HFiles of this region.
   */
  private void regionSnapshot(final HRegionInfo info) throws IOException {
    Path srcRegionDir = HRegion.getRegionDir(master.getRootDir(), info);
    Path dstRegionDir = new Path(snapshotDir, info.getEncodedName());
    // 1. dump the region meta
    HRegion.checkRegioninfoOnFilesystem(fs, info, dstRegionDir);

    FileSystem fs = master.getFileSystem();
    for (HColumnDescriptor c : info.getTableDesc().getFamilies()) {
      Path srcFamilyDir = new Path(srcRegionDir, c.getNameAsString());
      Path dstFamilyDir = new Path(dstRegionDir, c.getNameAsString());

      FileStatus[] hfiles = fs.listStatus(srcFamilyDir);
      for (FileStatus file : hfiles) {
        // 2. create "reference" to this HFile
        Path dstFile = StoreFile.createReference(file.getPath(),
            dstFamilyDir, fs, master.getConfiguration());
        // 3. update reference count for this HFile
        HRegion.updateRefCountInMeta(info, metaTable, file.getPath(),
            dstFile, fs);
      }
    }
  }
}
