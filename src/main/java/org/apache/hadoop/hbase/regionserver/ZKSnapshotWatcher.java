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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.handler.SnapshotHandler;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Watches the snapshot znode for the region server, and handles the events
 * which create snapshot or abort snapshot on the region server
 */
public class ZKSnapshotWatcher implements Watcher {
  private static final Log LOG = LogFactory.getLog(ZKSnapshotWatcher.class); 

  private HRegionServer server;
  private ZooKeeperWrapper zkWrapper;
  
  // whether snapshot creation thread is running
  private boolean isRunning;
  private SnapshotHandler snapshotThread;
  
  ZKSnapshotWatcher(Configuration conf, HRegionServer server) {
    zkWrapper = ZooKeeperWrapper.getInstance(conf, server.getClass().getName());
    this.server = server;
    
   
    // Set a watch on Zookeeper's snapshot node if it exists.
    zkWrapper.registerListener(this);
  }
  
  @Override
  public synchronized void process(WatchedEvent event) {
    // TODO Auto-generated method stub
  }

  private void handleSnapshotStart(final byte[] snapshotName, final byte[] tableName) {
    
  }
  
  private void handleSnapshotAbort(final byte[] snapshotName) {
    
  }
}
