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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HSnapshotDescriptor;
import org.apache.hadoop.hbase.master.SnapshotMonitor.SnapshotStatus;

/**
 * Thread that creates the snapshot on region server.
 */
public class SnapshotThread extends Thread {
  private static final Log LOG = LogFactory.getLog(SnapshotThread.class);

  private final HSnapshotDescriptor snapshot;
  private final HRegionServer server;

  public SnapshotThread(final HSnapshotDescriptor snapshot, final HRegionServer server) {
    this.snapshot = snapshot;
    this.server = server;
  }

  @Override
  public void run(){
    try {
      LOG.debug("Start snapshot thread for: " + snapshot);
      server.performSnapshot(snapshot);
    } catch (Throwable e) {
      LOG.info("Failed to perform snapshot: " + snapshot + " on RS " +
          server.getServerInfo().getServerName(), e);

      /*
       * remove the RS node under ready/finish status, there are two situations when
       * a exception occurs:
       * 1. if ready or finish node has been created under corresponding directory
       * this will trigger the master to abort the whole snapshot process and then
       * master will do the clean up work.
       * 2. if exception occurs before this RS is ready for snapshot, then nothing
       * will be created or deleted. Master will abort the snapshot when timeout.
       */
      server.getZooKeeperWrapper().removeRSForSnapshot(
          server.getServerInfo().getServerName(), SnapshotStatus.RS_READY);
      server.getZooKeeperWrapper().removeRSForSnapshot(
          server.getServerInfo().getServerName(), SnapshotStatus.RS_FINISH);
    }
  }

  /**
   * Get current snapshot descriptor for this thread
   *
   * @return
   */
  public HSnapshotDescriptor getCurrentSnapshot() {
    return snapshot;
  }
}
