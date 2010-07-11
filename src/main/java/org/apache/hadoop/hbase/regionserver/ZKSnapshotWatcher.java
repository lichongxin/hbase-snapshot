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
import org.apache.hadoop.hbase.HSnapshotDescriptor;
import org.apache.hadoop.hbase.master.SnapshotMonitor.SnapshotStatus;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * Watches the snapshot znode, and handles the events that create snapshot
 * or abort snapshot on the region server
 */
public class ZKSnapshotWatcher implements Watcher {
  private static final Log LOG = LogFactory.getLog(ZKSnapshotWatcher.class); 

  private HRegionServer server;
  private ZooKeeperWrapper zkWrapper;
  
  // thread that creates the snapshot
  private SnapshotThread snapshotThread;
  
  /**
   * Start watching the snapshot start/abort request on snapshot znode.
   * 
   * @param conf
   * @param server
   */
  public static void start(Configuration conf, HRegionServer server) {
    new ZKSnapshotWatcher(conf, server);
    LOG.debug("Started ZKSnapshot Watcher");
  }
  
  ZKSnapshotWatcher(Configuration conf, HRegionServer server) {
    this.zkWrapper = ZooKeeperWrapper.getInstance(conf, server.getClass().getName());
    this.server = server;
    
    String snapshotZNode = zkWrapper.getSnapshotRootZNode();
    
    if (zkWrapper.exists(snapshotZNode, false)) {
      zkWrapper.cleanupSnapshotZNode();
    }
    zkWrapper.createZNodeIfNotExists(zkWrapper.getSnapshotRootZNode());
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
    
    // ignore other events except NodeDataChanged event for snapshot root directory
    if (!type.equals(EventType.NodeDataChanged)) {
      return;
    }
    
    try {
      byte[] data = zkWrapper.readZNode(zkWrapper.getSnapshotRootZNode(), null);
      
      /*
       * if data in snapshot root znode is not empty, create snapshot based on
       * the data in the node
       */
      if (data.length != 0) {
        HSnapshotDescriptor snapshot = (HSnapshotDescriptor) Writables
          .getWritable(data, new HSnapshotDescriptor());
        LOG.debug("Create snapshot on RS: " + snapshot + ", RS=" + 
            server.getServerInfo().getServerName());
        
        handleSnapshotStart(snapshot);
      }
      /*
       * if data in snapshot root znode is set empty, abort current snapshot
       */
      else {
        HSnapshotDescriptor snapshot = snapshotThread.getCurrentSnapshot();
        LOG.debug("Abort snapshot on RS: " + snapshot + ", RS=" + 
            server.getServerInfo().getServerName());
        
        handleSnapshotAbort();
      }
      
    } catch (IOException e) {
      LOG.error("Could not process event from ZooKeeper", e);
    }

  }

  /*
   * Perform snapshot in a separate thread
   */
  private void handleSnapshotStart(final HSnapshotDescriptor snapshot) {
    snapshotThread = new SnapshotThread(snapshot, server);
    snapshotThread.start();
  }
  
  /*
   * Abort current snapshot by interrupting the snapshot thread.
   * If the thread has terminated, there is nothing to do, and 
   * master will do the clean up work in ZK and HDFS.
   */
  private void handleSnapshotAbort() {
    if (snapshotThread != null && snapshotThread.isAlive()) {
      snapshotThread.interrupt();
    } 
    snapshotThread = null;
    
    // remove RS znodes under ready/finish to notify the master
    // snapshot has been aborted on this RS
    zkWrapper.removeRSForSnapshot(server.getServerInfo().getServerName(), 
        SnapshotStatus.RS_READY);
    zkWrapper.removeRSForSnapshot(server.getServerInfo().getServerName(), 
        SnapshotStatus.RS_FINISH);
  }
}
