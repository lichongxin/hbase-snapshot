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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SnapshotDescriptor;
import org.apache.hadoop.hbase.master.SnapshotSentinel.GlobalSnapshotStatus;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * This class monitors the whole process of snapshots via ZooKeeper.
 * There is only one SnapshotMonitor for the master.
 *
 * <p>Start monitoring a snapshot by calling method monitor() before the
 * snapshot is started across the cluster via ZooKeeper. SnapshotMonitor
 * would stop monitoring this snapshot only if it is finished or aborted.
 *
 * <p>Note: There could be only one snapshot being processed and monitored
 * at a time over the cluster. Start monitoring a snapshot only when the
 * previous one reaches an end status.
 */
public class SnapshotMonitor implements Watcher {
  private static final Log LOG = LogFactory.getLog(SnapshotMonitor.class);

  private final HMaster master;
  private final ZooKeeperWrapper zkWrapper;

  // the snapshot being monitored
  private SnapshotSentinel sentinel;

  SnapshotMonitor(final HMaster master) {
    this.master = master;
    this.zkWrapper = master.getZooKeeperWrapper();
    this.sentinel = null;

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
   */
  public boolean isInProcess() {
    return sentinel != null;
  }

  /**
   * Start monitoring a snapshot. Call this method before a snapshot is
   * started across the cluster via ZooKeeper.
   *
   * @param hsd snapshot to monitor
   * @return SnapshotTracker for this snapshot
   * @throws IOException if there another snapshot in process
   */
  public SnapshotSentinel monitor(final SnapshotDescriptor hsd)
  throws IOException {
    // only one snapshot could be monitored at a time
    if (isInProcess()) {
      LOG.warn("Another snapshot is still in process: "
          + hsd.getSnapshotNameAsString());
      throw new IOException("Snapshot in process: "
          + hsd.getSnapshotNameAsString());
    }
    LOG.debug("Start monitoring snapshot: " + hsd);
    this.sentinel = new SnapshotSentinel(hsd, master);
    zkWrapper.registerListener(this);
    return this.sentinel;
  }

  /**
   * @return SnapshotTracker for current snapshot
   */
  public SnapshotSentinel getCurrentSnapshotTracker() {
    return this.sentinel;
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
      // NodeDataChanged for snapshot root node, could be:
      // 1. Start snapshot if the data is not empty
      // 2. Abort snapshot if the data is set to empty
      if (type.equals(EventType.NodeDataChanged) &&
          path.equals(zkWrapper.getSnapshotRootZNode())) {
        byte[] data = zkWrapper.readZNode(zkWrapper.getSnapshotRootZNode(), null);
        if (data.length != 0) {
          LOG.info("Starting snapshot on ZK: " + sentinel);
        } else {
          LOG.info("Aborting snapshot on ZK: " + sentinel);
          List<String> readyRS =
            zkWrapper.listZnodes(zkWrapper.getSnapshotReadyZNode());
          if (readyRS == null || readyRS.isEmpty()) {
            sentinel.setStatus(GlobalSnapshotStatus.ABORTED);
            LOG.info("Snapshot is aborted: " + sentinel);
          } else {
            sentinel.setStatus(GlobalSnapshotStatus.ABORTING);
          }
        }
      }
      // check if all RS are ready every time when one RS node
      // is added to ready directory
      else if (type.equals(EventType.NodeChildrenChanged) &&
          path.equals(zkWrapper.getSnapshotReadyZNode())) {
        List<String> readyRS =
          zkWrapper.listZnodes(zkWrapper.getSnapshotReadyZNode());
        if (sentinel.containAllRS(readyRS) && 
            sentinel.getStatus().equals(GlobalSnapshotStatus.INIT)) {
          sentinel.setStatus(GlobalSnapshotStatus.ALL_RS_READY);
          LOG.info("All RS are ready for snapshot: " + sentinel);
        }
      }
      // check if all RS have finished every time when one RS node
      // is added to finish directory
      else if (type.equals(EventType.NodeChildrenChanged) &&
          path.equals(zkWrapper.getSnapshotFinishZNode())) {
        List<String> finishRS =
          zkWrapper.listZnodes(zkWrapper.getSnapshotFinishZNode());
        if (sentinel.containAllRS(finishRS) && 
            sentinel.getStatus().equals(GlobalSnapshotStatus.ALL_RS_READY)) {
          sentinel.setStatus(GlobalSnapshotStatus.ALL_RS_FINISHED);
          LOG.info("All RS are finished for snapshot: " + sentinel);
        }
      }
      // Two situations when a znode is deleted:
      // 1. if current status is normal(not aborting) and one RS node is
      // deleted from "ready" directory then abort the whole snapshot on ZK
      // 2. if all RS have removed the nodes under ready directory, then
      // the snapshot is aborted
      else if (type.equals(EventType.NodeDeleted) &&
          path.startsWith(zkWrapper.getSnapshotReadyZNode())) {
        List<String> readyRS =
          zkWrapper.listZnodes(zkWrapper.getSnapshotReadyZNode());
        if (readyRS == null || readyRS.isEmpty()) { // Case 2
          sentinel.setStatus(GlobalSnapshotStatus.ABORTED);
          LOG.info("Snapshot is aborted: " + sentinel);
        } else if (!sentinel.getStatus().equals(
            GlobalSnapshotStatus.ABORTING)) { // Case 1
          zkWrapper.abortSnapshot();
          String server = path.substring(path.lastIndexOf(
              ZooKeeperWrapper.ZNODE_PATH_SEPARATOR));
          LOG.info("Aborting snapshot: " + sentinel + " because of RS: " + server);
        }
      }
      else {
        // ignore other events
        LOG.debug("Ignoring ZK Event: " + type + " state:"
            + event.getState() + " path:" + path);
      }
    } catch (IOException e) {
      LOG.error("Could not process event from ZooKeeper", e);
    } finally {
      // if snapshot has been finished or aborted, reset the monitor
      if (sentinel.getStatus().equals(GlobalSnapshotStatus.ALL_RS_FINISHED)
          || sentinel.getStatus().equals(GlobalSnapshotStatus.ABORTED)) {
        sentinel.notifyWaiting();
        reset();
      }
    }
  }

  /*
   * stop monitoring the snapshot and clean up the snapshot directory on ZK
   */
  private void reset() {
    zkWrapper.cleanupSnapshotZNode();
    zkWrapper.unregisterListener(this);
    this.sentinel = null;
  }
}
