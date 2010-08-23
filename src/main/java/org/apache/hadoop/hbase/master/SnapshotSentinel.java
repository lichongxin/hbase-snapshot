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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.SnapshotDescriptor;

/**
 * Tracking the global status of a snapshot over its lifetime. Each snapshot
 * is associated with a SnapshotSentinel. It is created at the start of snapshot
 * and is destroyed when the snapshot reaches one of the end status:
 * <code>GlobalSnapshotStatus.ALL_RS_FINISHED<code> or
 * <code>GlobalSnapshotStatus.ABORTED<code>.
 */
public class SnapshotSentinel {
  private static final Log LOG = LogFactory.getLog(SnapshotSentinel.class);

  private final int maxRetries;
  private SnapshotDescriptor hsd;

  /*
   * active servers when the snapshot is started. The table for snapshot
   * is served by these servers so snapshot is finished only if finished
   * on all these region servers.
   */
  private Set<String> activeServers;

  // current status of snapshot
  private volatile GlobalSnapshotStatus status;
  private Object statusNotifier = new Object();

  SnapshotSentinel(final SnapshotDescriptor hsd, final HMaster master)
  throws IOException {
    this.maxRetries = master.getConfiguration().getInt(
        "hbase.master.snapshot.maxretries", 5);
    this.hsd = hsd;
    this.status = GlobalSnapshotStatus.INIT;
    this.activeServers = new HashSet<String>();
    activeServers.addAll(
        master.getServerManager().getServersToServerInfo().keySet());

    // create the base directory for this snapshot
    Path snapshotDir = SnapshotDescriptor.getSnapshotDir(master.getRootDir(),
        hsd.getSnapshotName());
    if (!master.getFileSystem().mkdirs(snapshotDir)) {
      LOG.warn("Could not create snapshot directory: " + snapshotDir);
      throw new IOException("Could not create snapshot directory: "
          + snapshotDir);
    }

    // dump snapshot info
    SnapshotDescriptor.write(hsd, snapshotDir, master.getFileSystem());
  }

  /**
   * Verify if all <code>activeServers</code> are included in
   * <code>rsList</code>.
   *
   * @param rsList region server that are ready or finished on ZK
   * @return true if all activeServers are included in rsList
   */
  boolean containAllRS(List<String> rsList) {
    Set<String> rsSet = new TreeSet<String>();
    rsSet.addAll(rsList);

    if (rsSet.size() >= activeServers.size()) {
      for(String serverName : activeServers) {
        if (!rsSet.contains(serverName)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /** @return status of the snapshot */
  GlobalSnapshotStatus getStatus() {
    return this.status;
  }

  void setStatus(GlobalSnapshotStatus status) {
    this.status = status;
  }

  /** @return the snapshot descriptor for this snapshot */
  SnapshotDescriptor getSnapshot() {
    return this.hsd;
  }

  /**
   * Wait until the snapshot is finished. Timeout after maxRetries * 3000 ms.
   * Increase maxRetries if snapshot always timeout.
   *
   * @throws IOException
   *           if timeout or snapshot is aborted or waiting is interrupted
   */
  void waitToFinish() throws IOException {
    try {
      int retries = 0;
      synchronized (statusNotifier) {
        while (!status.equals(GlobalSnapshotStatus.ALL_RS_FINISHED) &&
            !status.equals(GlobalSnapshotStatus.ABORTED) &&
            retries < maxRetries) {
          LOG.debug("Waiting snapshot: " + hsd + " to finish, retries: "
              + retries);
          statusNotifier.wait(3000);
          retries++;
        }
      }
      // retries exhausted, waiting timeout
      if (retries == maxRetries) {
        LOG.info("Retries exhausted, snapshot timeout: " + hsd);
        throw new IOException("Snapshot: " + hsd + " timeout");
      } else if (status.equals(GlobalSnapshotStatus.ABORTED)) {
        LOG.info("Snapshot is aborted: " + hsd);
        throw new IOException("Snapshot is aborted: " + hsd);
      }
    } catch (InterruptedException e) {
      LOG.debug("Master thread is interrupted for snapshot: " + hsd, e);
      throw new IOException(e);
    }
  }

  /**
   * Wait until snapshot is aborted. Timeout after maxRetries * 3000 ms.
   *
   * @throws IOException if timeout or waiting is interrupted
   */
  void waitToAbort() throws IOException {
    try {
      int retries = 0;
      synchronized (statusNotifier) {
        while (!status.equals(GlobalSnapshotStatus.ABORTED) &&
            retries < maxRetries) {
          LOG.debug("Waiting snapshot: " + hsd + " to abort, retries: "
              + retries);
          statusNotifier.wait(3000);
          retries++;
        }
      }
      // retries exhausted, waiting timeout
      if (retries == maxRetries) {
        LOG.info("Retries exhausted, aborting snapshot timeout: " + hsd);
        throw new IOException("Snapshot: " + hsd + " timeout for aborting");
      }
    } catch (InterruptedException e) {
      LOG.debug("Master thread is interrupted for snapshot: " + hsd, e);
      throw new IOException(e);
    }
  }

  void notifyWaiting() {
    synchronized(statusNotifier) {
      statusNotifier.notify();
    }
  }

  @Override
  public String toString() {
    return hsd.toString();
  }

  /**
   * Status of snapshot over the cluster. The end status of snapshot
   * is <code>ALL_RS_FINISHED</code> or <code>ABORTED</code>.
   */
  public static enum GlobalSnapshotStatus{
    INIT,              // snapshot is just started over the cluster
    ALL_RS_READY,      // all RS are ready for snapshot
    ALL_RS_FINISHED,   // all RS have finished the snapshot
    ABORTING,          // abort the snapshot over the cluster
    ABORTED,           // snapshot is successfully aborted on all RS
  }
}
