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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Instantiated to delete an existing snapshot.
 */
public class DeleteSnapshot extends SnapshotOperation {
  private static final Log LOG = LogFactory.getLog(DeleteSnapshot.class);

  DeleteSnapshot(HMaster master, byte[] snapshotName) throws IOException {
    super(master, snapshotName);
  }

  @Override
  protected void beforeProcess() throws IOException {
    // do nothing
  }

  @Override
  protected void afterProcess() throws IOException {
    // delete the whole directory for this snapshot
    if (!fs.delete(snapshotDir, true)) {
      LOG.error("Failed to delete snapshot dir: " + snapshotDir);
      throw new IOException("Failed to delete snapshot dir: " + snapshotDir);
    }
  }

  @Override
  protected void processRegion(Path regionDir) throws IOException {
    // for each reference file under snapshot dir, decrease the reference
    // count by 1 in meta and then delete it from the file system
    Path srcTableDir =
      HTableDescriptor.getTableDir(master.getRootDir(), hsd.getTableName());
    try {
      // read region info
      Path infoFile = new Path(regionDir, HRegion.REGIONINFO_FILE);
      FSDataInputStream in = fs.open(infoFile);
      HRegionInfo info = null;
      try {
        info = new HRegionInfo();
        info.readFields(in);
      } finally {
        in.close();
      }

      for (HColumnDescriptor family : info.getTableDesc().getColumnFamilies()) {
        Path familyDir = new Path(regionDir, family.getNameAsString());
        Path srcFamilyDir = Store.getStoreHomedir(srcTableDir,
            info.getEncodedName(), family.getName());
        FileStatus[] hfiles = fs.listStatus(familyDir);
        if (hfiles != null) {
          for (FileStatus refFile : hfiles) {
            Path srcFile =
              new Path(srcFamilyDir, refFile.getPath().getName());
            // the qualifier is the path of the src file
            meta.incrementColumnValue(
                info.getReferenceMetaRow(), HConstants.SNAPSHOT_FAMILY,
                Bytes.toBytes(FSUtils.getPath(srcFile)), -1);
            if (!fs.delete(refFile.getPath(), false)) {
              LOG.warn("Failed to delete snapshot reference file: " + refFile);
            }
          }
        }
      }
      if (!fs.delete(regionDir, true)) {
        LOG.warn("Failed to delete snapshot region dir: " + regionDir);
      }
      LOG.debug("Deleting snapshot region: " + regionDir);
    } catch (IOException e) {
      LOG.warn("Failed to delete snapshot region: " + regionDir,
          RemoteExceptionHandler.checkIOException(e));
      // don't re-throw the exception here
      // the whole snapshot directory would be deleted in method
      // afterProcess anyway and reference count in META would be
      // synchronized in MetaScanner.
    }
  }
}
