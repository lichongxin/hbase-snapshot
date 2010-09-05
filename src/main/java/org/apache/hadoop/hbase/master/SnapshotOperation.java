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
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SnapshotDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Abstract base class for snapshot operations.
 */
public abstract class SnapshotOperation {
  public static final Pattern REGION_NAME_PATTERN = Pattern.compile("^(\\w+)$");

  protected HMaster master;
  protected FileSystem fs;

  protected SnapshotDescriptor hsd;
  protected Path snapshotDir;

  // regions' directories of the snapshot
  protected Set<Path> regions = new TreeSet<Path>();

  protected HTable meta;

  SnapshotOperation(final HMaster master, final byte [] snapshotName)
  throws IOException {
    this.master = master;
    this.fs = master.getFileSystem();
    this.snapshotDir =
      SnapshotDescriptor.getSnapshotDir(master.getRootDir(), snapshotName);
    if (!fs.exists(snapshotDir)) {
      throw new IOException("snapshot does not exist");
    }

    // read snapshot info
    Path snapshotInfo =
      new Path(snapshotDir, SnapshotDescriptor.SNAPSHOTINFO_FILE);
    FSDataInputStream in = fs.open(snapshotInfo);
    try {
      hsd = new SnapshotDescriptor();
      hsd.readFields(in);
    } finally {
      in.close();
    }

    if (Bytes.equals(hsd.getTableName(), HConstants.META_TABLE_NAME)) {
      this.meta = new HTable(master.getConfiguration(),
          HConstants.ROOT_TABLE_NAME);
    } else {
      this.meta = new HTable(master.getConfiguration(),
          HConstants.META_TABLE_NAME);
    }

    // list snapshot regions
    fs.listStatus(snapshotDir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          Matcher m = REGION_NAME_PATTERN.matcher(p.getName());
          result = fs.getFileStatus(p).isDir() && m.matches();
          if (result) {
            regions.add(p);
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
        return result;
      }
    });
  }

  /**
   * Called before any snapshot region is processed
   */
  protected abstract void beforeProcess() throws IOException;

  /**
   * Called after any snapshot region is processed
   */
  protected abstract void afterProcess() throws IOException;

  void process() throws IOException{
    beforeProcess();
    // process each region of the snapshot
    for (Path regionDir : regions) {
      processRegion(regionDir);
    }
    afterProcess();
  }

  protected abstract void processRegion(Path regionDir) throws IOException;

  /** @return snapshot descriptor for the current operation */
  public SnapshotDescriptor getSnapshotDescriptor() {
    return this.hsd;
  }
}
