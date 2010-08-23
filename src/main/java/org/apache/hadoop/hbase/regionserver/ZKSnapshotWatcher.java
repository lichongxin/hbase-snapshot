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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.SnapshotDescriptor;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * Watches the snapshot znode, and handles the events that create snapshot
 * or abort snapshot on the region server.
 *
 * Note: there could be only one snapshot in process at any time. If a snapshot
 * has been started on this region server, only event to abort this snapshot
 * would be accepted by this class.
 */
public class ZKSnapshotWatcher implements Watcher {
  private static final Log LOG = LogFactory.getLog(ZKSnapshotWatcher.class);

  private HRegionServer server;
  private ZooKeeperWrapper zkWrapper;

  // thread that creates the snapshot
  Snapshotter snapshotThread;

  /**
   * Start watching the snapshot start/abort request on snapshot znode.
   *
   * @param conf Configuration
   * @param server
   * @return ZKSnapshotWatcher instance which is just started
   */
  public static ZKSnapshotWatcher start(Configuration conf, HRegionServer server) {
    LOG.debug("Starting ZKSnapshotWatcher");
    return new ZKSnapshotWatcher(conf, server);
  }

  ZKSnapshotWatcher(Configuration conf, HRegionServer server) {
    this.zkWrapper = ZooKeeperWrapper.getInstance(conf,
        server.getServerInfo().getServerName());
    this.server = server;

    String snapshotNode = zkWrapper.getSnapshotRootZNode();
    zkWrapper.createZNodeIfNotExists(snapshotNode);
    // set a watch on snapshot root znode
    zkWrapper.watchZNode(snapshotNode);
    zkWrapper.registerListener(this);
  }

  @Override
  public synchronized void process(WatchedEvent event) {
    EventType type = event.getType();
    LOG.debug("Got ZooKeeper event, state: " + event.getState() + ", type: "
        + event.getType() + ", path: " + event.getPath());

    // check if the path is the snapshot directory that we care about
    if (event.getPath() == null
        || !event.getPath().equals(zkWrapper.getSnapshotRootZNode())) {
       return;
    }

    // ignore other events except NodeDataChanged event for
    // snapshot root directory
    if (!type.equals(EventType.NodeDataChanged)) {
      return;
    }

    try {
      byte[] data =
        zkWrapper.readZNode(zkWrapper.getSnapshotRootZNode(), null);
      /*
       * if data in snapshot root znode is not empty,
       * create snapshot based on the data in the node
       */
      if (data.length != 0) {
        SnapshotDescriptor snapshot = (SnapshotDescriptor) Writables
          .getWritable(data, new SnapshotDescriptor());
        LOG.debug("Create snapshot on RS: " + snapshot + ", RS=" +
            server.getServerInfo().getServerName());

        handleSnapshotStart(snapshot);
      }
      /*
       * if data in snapshot root znode is set empty,
       * abort current snapshot
       */
      else {
        if (snapshotThread != null) {
          SnapshotDescriptor snapshot = snapshotThread.getCurrentSnapshot();
          LOG.debug("Aborting snapshot on RS: " + snapshot + ", RS=" +
              server.getServerInfo().getServerName());
        }
        handleSnapshotAbort();
      }
    } catch (IOException e) {
      LOG.error("Could not process event from ZooKeeper", e);
    }
  }

  /*
   * Perform snapshot in a separate thread
   */
  private void handleSnapshotStart(final SnapshotDescriptor snapshot) {
    // if there is a snapshot thread is still running, don't start another
    if (snapshotThread != null && snapshotThread.isAlive()) {
      LOG.warn("Another snapshot is still in process.");
      return;
    }
    snapshotThread = new Snapshotter(snapshot, server);
    snapshotThread.start();
  }

  /*
   * Abort current snapshot then remove the RS node under ready and finish
   * directory. The master will do the clean up work on file system.
   */
  private void handleSnapshotAbort() {
    if (snapshotThread != null) {
      try {
        snapshotThread.join();
      } catch (InterruptedException e) {
        // TODO call region server abortSnapshot?
        LOG.info("Snapshot thread is interrupted");
      }
    }
    snapshotThread = null;

    // remove RS znodes under ready and finish directory to notify the master
    // snapshot has been aborted on this RS
    zkWrapper.removeRSForSnapshot(server.getServerInfo().getServerName(),
        RSSnapshotStatus.READY);
    zkWrapper.removeRSForSnapshot(server.getServerInfo().getServerName(),
        RSSnapshotStatus.FINISH);
  }

  /**
   * Snapshot status on a region server
   */
  public static enum RSSnapshotStatus {
    READY,       // RS is ready for snapshot
    FINISH;      // RS has finished the snapshot
  }
}
