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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HSnapshotDescriptor;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * This class monitors the whole process of a snapshot for the master
 */
public class SnapshotMonitor implements Watcher {
  private static final Log LOG = LogFactory.getLog(SnapshotMonitor.class);

  private HSnapshotDescriptor snapshot;

  private HMaster master;
  private final ZooKeeperWrapper zkWrapper;

  /*
   * active servers when the snapshot is initialized.
   * the table for snapshot is served by these servers so
   * snapshot is finished only if all these servers finish.
   */
  private Set<String> activeServers;

  // current status of snapshot
  SnapshotStatus status;
  private Integer mutex = new Integer(0);

  SnapshotMonitor(final Configuration conf, final HMaster master) {
    this.master = master;
    this.zkWrapper = ZooKeeperWrapper.getInstance(conf, HMaster.class.getName());
    
    String snapshotZNode = zkWrapper.getSnapshotRootZNode();
    // If the snapshot ZNode exists, clean up the znodes under snapshot dir
    if(master.isClusterStartup() && zkWrapper.exists(snapshotZNode, false)) {
      zkWrapper.cleanupSnapshotZNode();
    }
    // create snapshot node, ready node and finish node
    // then set watches on them
    zkWrapper.createZNodeIfNotExists(snapshotZNode);
  }

  /**
   * @return true if there is a snapshot in process, false otherwise
   * @throws IOException
   */
  public boolean isInProcess() throws IOException {
    byte[] hsd = zkWrapper.readZNode(zkWrapper.getSnapshotRootZNode(), null);
    if (hsd != null && hsd.length > 0) {
      return true;
    } else if (status == SnapshotStatus.M_ABORTING) {
      // abort snapshot but aborting has not finished
      return true;
    }
    return false;
  }

  /**
   * Start monitoring a snapshot
   *
   * @param hsd
   * @return
   * @throws IOException 
   */
  public void start(final HSnapshotDescriptor hsd) throws IOException {
    LOG.debug("Start monitoring snapshot: " + hsd);
    this.snapshot = hsd;
    this.status = SnapshotStatus.M_INIT;
    this.activeServers = new HashSet<String>();
    activeServers.addAll(master.getServerManager().getServersToServerInfo().keySet());

    // create the snapshot directory
    Path snapshotDir = HSnapshotDescriptor.getSnapshotDir(master.getRootDir(),
        hsd.getSnapshotName());
    if (master.getFileSystem().exists(snapshotDir)) {
      LOG.warn("Snapshot " + hsd.getSnapshotNameAsString() + " already exists.");
      throw new IOException("Snapshot already exists");
    }
    master.getFileSystem().mkdirs(snapshotDir);
    zkWrapper.registerListener(this);
  }

  @Override
  public void process(WatchedEvent event) {
    String path = event.getPath();
    EventType type = event.getType();
    LOG.debug("ZK-EVENT-PROCESS: Got zkEvent " + type +
        " state:" + event.getState() + " path:" + path);

    // check if the path is under the SNAPSHOT directory that we care about
    if (path == null
        || !path.startsWith(zkWrapper.getSnapshotRootZNode())) {
       return;
    }
    // Handle the ignored events
    if(type.equals(EventType.None)) {
      return;
    }

    try {
      // snapshot is started on ZK
      if (type.equals(EventType.NodeDataChanged) &&
          path.equals(zkWrapper.getSnapshotRootZNode())) {
        byte[] data = zkWrapper.readZNode(zkWrapper.getSnapshotRootZNode(), null);
        if (data.length != 0) {
          this.status = SnapshotStatus.M_INIT;
          LOG.info("Snapshot is started on ZK: " + snapshot);
        }
      }
      // check if all RS are ready every time when one RS node
      // is added to ready directory
      else if (type.equals(EventType.NodeChildrenChanged) &&
          path.equals(zkWrapper.getSnapshotReadyZNode())) {
        Set<String> readyRS = new TreeSet<String>();
        readyRS.addAll(zkWrapper.listZnodes(zkWrapper.getSnapshotReadyZNode()));

        if (readyRS.size() >= activeServers.size()) {
          status = SnapshotStatus.M_ALLREADY;
          // all active servers for this table should be ready
          for(String serverName : activeServers) {
            if (!readyRS.contains(serverName)) {
              // not ready yet, roll back to previous status and keep watching
              status = SnapshotStatus.M_INIT;
              break;
            }
          }

          if (status.equals(SnapshotStatus.M_ALLREADY)) {
            LOG.info("All RS are ready for snapshot: " + snapshot);
          }
        }
      }
      // check if all RS have finished every time when one RS node
      // is added to finish directory
      else if (type.equals(EventType.NodeChildrenChanged) &&
          path.equals(zkWrapper.getSnapshotFinishZNode())) {
        Set<String> finishRS = new TreeSet<String>();
        finishRS.addAll(zkWrapper.listZnodes(zkWrapper.getSnapshotFinishZNode()));
        
        if (finishRS.size() >= activeServers.size()) {
          status = SnapshotStatus.M_ALLFINISH;
          // all active servers for this table should finish
          for(String serverName : activeServers) {
            if (!finishRS.contains(serverName)) {
              // not finished yet, roll back to previous status and keep watching
              status = SnapshotStatus.M_ALLREADY;
              break;
            }
          }

          if (status.equals(SnapshotStatus.M_ALLFINISH)) {
            LOG.info("All RS have finished for snapshot: " + snapshot);
          }
        }
      }
      // Two situations when a znode is deleted:
      // 1. if current status is normal(not aborting) and one RS node is
      // deleted from "ready" directory then abort the whole snapshot on ZK
      // 2. if all RS have removed the nodes under ready directory, then
      // the snapshot is aborted
      else if (type.equals(EventType.NodeDeleted) &&
          path.startsWith(zkWrapper.getSnapshotReadyZNode())) {
        List<String> readyRS = zkWrapper.listZnodes(zkWrapper.getSnapshotReadyZNode());
        if (readyRS == null || readyRS.size() == 0) {
          // snapshot has been aborted on all region servers
          // then clean up the snapshot on file system
          master.deleteSnapshot(snapshot.getSnapshotName());
          status = SnapshotStatus.M_ABORTED;
          LOG.info("Snapshot is aborted: " + snapshot);
        } else if (!status.equals(SnapshotStatus.M_ABORTING)) {
          // ask all RS to abort on ZK
          status = SnapshotStatus.M_ABORTING;
          zkWrapper.abortSnapshot();
          String server = path.substring(path.lastIndexOf(
              ZooKeeperWrapper.ZNODE_PATH_SEPARATOR));
          LOG.info("Aborting snapshot: " + snapshot + " because of RS: " + server);
        }
      }
      else {
        // ignore other events?
      }

      // notify the snapshot is finished or aborted
      if (status.equals(SnapshotStatus.M_ALLFINISH) ||
          status.equals(SnapshotStatus.M_ABORTED)) {
        mutex.notify();
      }
    } catch (IOException e) {
      LOG.error("Could not process event from ZooKeeper", e);
    }
  }

  /**
   * Wait for the snapshot to be finished or aborted or timeout
   * after 3000 * 3 ms
   *
   * @return snapshot status when 
   * @throws IOException 
   */
  public SnapshotStatus waitToFinish() throws IOException {
    try {
      int retries = 0;
      synchronized(mutex) {
        while (status != SnapshotStatus.M_ALLFINISH &&
            status != SnapshotStatus.M_ABORTED &&
            retries < 3) {
          LOG.debug("Wait for snapshot to finish: " + snapshot
              + ", retries: " + retries);
          mutex.wait(3000);
          retries++;
        }

        // retries exhausted, snapshot timeout, so abort the snapshot
        if (retries == 3) {
          LOG.info("Retries exhausted, snapshot timeout: " + snapshot);
          abortSnapshot();
        }
      }
    } catch (InterruptedException e) {
      LOG.debug("Master thread is interrupted for snapshot: " + snapshot, e);
    } finally {
      // stop monitoring the snapshot, clean up the snapshot directory
      // and reset the snapshot root znode data
      zkWrapper.unregisterListener(this);
      zkWrapper.cleanupSnapshotZNode();
      zkWrapper.writeZNode(zkWrapper.getSnapshotRootZNode(),
          HConstants.EMPTY_BYTE_ARRAY, -1, false);
    }

    return status;
  }

  /*
   * Abort the snapshot if it is timeout
   */
  private void abortSnapshot() throws InterruptedException, IOException {
    zkWrapper.abortSnapshot();
    int retries = 0;
    synchronized(mutex) {
      while (status != SnapshotStatus.M_ABORTED &&
          retries < 3) {
        LOG.debug("Wait for snapshot to abort: " + snapshot
            + ", retries: " + retries);
        mutex.wait(3000);
        retries++;
      }

      // retries exhausted, aborting timeout
      if (retries == 3) {
        LOG.info("Retries exhausted, aborting timeout: " + snapshot);
        throw new IOException("Failed to abort snapshot");
      }
    }
  }

  /**
   * Status of snapshot for the master and region server
   */
  public static enum SnapshotStatus{
    /*
     * Snapshot status for the master
     */
    M_INIT,          // snapshot is set on ZK
    M_ALLREADY,      // all RS are ready for snapshot
    M_ALLFINISH,     // all RS have finished the snapshot
    M_ABORTING,      // abort the snapshot on ZK
    M_ABORTED,       // snapshot is successfully aborted

    /*
     * Snapshot status for a single region server
     */
    RS_READY,       // RS is ready for snapshot
    RS_FINISH;      // RS has finished the snapshot
  }
}
