package org.apache.hadoop.hbase.master;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class SnapshotMonitor implements Watcher {

  @Override
  public void process(WatchedEvent arg0) {
    // TODO Auto-generated method stub

  }
  
  public static enum SnapshotStatus{
    ABORT       (0),
    READY       (1),
    FINISH      (2);
    
    private final byte value;
    
    private SnapshotStatus(int intValue) {
      this.value = (byte)intValue;
    }
    
    public byte getByteValue() {
      return value;
    }
    
    public static SnapshotStatus fromByte(byte value) {
      switch(value) {
        case  0: return ABORT;
        case  1 : return READY;
        case  2 : return FINISH;

        default:
          throw new RuntimeException("Invalid byte value for SnapshotStatus");
      }
    }
    
    @Override
    public String toString() {
      switch(value) {
      case 1:
        return "ready";
      case 2:
        return "finish";
        
      default:
        throw new RuntimeException("Invalid byte value for SnapshotStatus");
      }
    }
  }

}
