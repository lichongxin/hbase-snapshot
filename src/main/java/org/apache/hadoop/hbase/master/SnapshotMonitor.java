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
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
  
  private final HSnapshotDescriptor snapshot;
  
  private HMaster master;
  private final ZooKeeperWrapper zkWrapper;
  /*
   *  active servers when the snapshot is initialized.
   *  the table for snapshot is served by these active servers
   */
  private Set<String> activeServers;
  
  // current status of snapshot
  private SnapshotStatus status;
  private Integer mutex;
  
  /**
   * Start a snapshot monitor for the passed snapshot. Each snapshot is 
   * associated with a SnapshotMonitor instance.
   * 
   * @param conf
   * @param master
   * @param snapshot
   * @return
   */
  public static SnapshotMonitor start(final Configuration conf, final HMaster master,
      final HSnapshotDescriptor snapshot) {
    LOG.debug("Start monitoring snapshot: " + snapshot);
    return new SnapshotMonitor(conf, master, snapshot);
  }
  
  SnapshotMonitor(final Configuration conf, final HMaster master, 
      final HSnapshotDescriptor snapshot) {
    this.master = master;
    this.snapshot = snapshot;
    this.zkWrapper = ZooKeeperWrapper.getInstance(conf, HMaster.class.getName());
    this.activeServers = master.getServerManager().getServersToServerInfo().keySet();
    
    String snapshotZNode = zkWrapper.getSnapshotRootZNode();
    // If the snapshot ZNode exists, clean up the znodes for snapshot
    if(master.isClusterStartup() && zkWrapper.exists(snapshotZNode, false)) {
      zkWrapper.cleanupSnapshotZNode();
    }
    // create snapshot node and set a watch on it
    zkWrapper.createZNodeIfNotExists(snapshotZNode);
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
      // check if all RS are ready every time one RS node
      // is added to ready directory 
      else if (type.equals(EventType.NodeChildrenChanged) &&
          path.equals(zkWrapper.getSnapshotReadyZNode())) {
        Set<String> readyRS = new TreeSet<String>();
        readyRS.addAll(zkWrapper.listZnodes(zkWrapper.getSnapshotReadyZNode()));
        
        if (readyRS.size() >= activeServers.size()) {
          status = SnapshotStatus.M_ALLREADY;
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
      // check if all RS have finished every time one RS node 
      // is added to finish directory 
      else if (type.equals(EventType.NodeChildrenChanged) &&
          path.equals(zkWrapper.getSnapshotFinishZNode())) {
        Set<String> finishRS = new TreeSet<String>();
        finishRS.addAll(zkWrapper.listZnodes(zkWrapper.getSnapshotFinishZNode()));
        
        if (finishRS.size() >= activeServers.size()) {
          status = SnapshotStatus.M_ALLFINISH;
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
      // 2. if current status is aborting and all RS have removed
      // the nodes under ready directory, then the snapshot is aborted
      else if (type.equals(EventType.NodeDeleted) && 
          path.startsWith(zkWrapper.getSnapshotReadyZNode()) &&
          path.length() > zkWrapper.getSnapshotReadyZNode().length()) {
        if (status.equals(SnapshotStatus.M_ABORTING)) {
          List<String> readyRS = zkWrapper.listZnodes(zkWrapper.getSnapshotReadyZNode());
          if (readyRS == null || readyRS.size() == 0) {
            master.abortSnapshot(snapshot.getSnapshotName(), snapshot.getTableName());
            status = SnapshotStatus.M_ABORTED;
            LOG.info("Snapshot is aborted: " + snapshot);
          }
        } else {
          // ask all RS to abort on ZK
          status = SnapshotStatus.M_ABORTING;
          zkWrapper.abortSnapshot();
          String server = path.substring(path.lastIndexOf(
              ZooKeeperWrapper.ZNODE_PATH_SEPARATOR));
          LOG.info("Aborting snapshot: " + snapshot + " because of RS: " + server);
        }
      } else {
        // ignore other events?
      }
      
      // stop the monitor if this snapshot is finished or aborted
      if (status.equals(SnapshotStatus.M_ALLFINISH) ||
          status.equals(SnapshotStatus.M_ABORTED)) {
        zkWrapper.unregisterListener(this);
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
   */
  public SnapshotStatus waitToFinish() {
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
        
        // retries exhausted, snapshot overtime, stop monitoring the snapshot
        if (retries == 3) {
          LOG.info("Retries exhausted, snapshot overtime: " + snapshot);
          zkWrapper.unregisterListener(this);
        }
      }
    } catch (InterruptedException e) {
      LOG.debug("Master thread is interrupted for snapshot: " + snapshot, e);
      zkWrapper.unregisterListener(this);
    }
    
    return status;
  }
  
  public static enum SnapshotStatus{
    /*
     * Snapshot status for the master
     */
    M_INIT         (0),     // snapshot is set on ZK
    M_ALLREADY     (1),     // all RS are ready for snapshot
    M_ALLFINISH    (2),     // all RS have finished the snapshot
    M_ABORTING     (3),     // abort the snapshot on ZK
    M_ABORTED      (-1),    // snapshot is successfully aborted
    
    /*
     * Snapshot status for a single region server
     */
    RS_READY       (11),    // RS is ready for snapshot
    RS_FINISH      (12);    // RS has finished the snapshot
    
    private final byte value;
    
    private SnapshotStatus(int intValue) {
      this.value = (byte)intValue;
    }
    
    public byte getByteValue() {
      return value;
    }
    
    public static SnapshotStatus fromByte(byte value) {
      switch(value) {
        case -1 : return M_ABORTED;
        case 0 : return M_INIT;
        case 1 : return M_ALLREADY;
        case 2 : return M_ALLFINISH;
        case 3 : return M_ABORTING;
        case  11 : return RS_READY;
        case  12 : return RS_FINISH;

        default:
          throw new RuntimeException("Invalid byte value for SnapshotStatus");
      }
    }
  }
}
