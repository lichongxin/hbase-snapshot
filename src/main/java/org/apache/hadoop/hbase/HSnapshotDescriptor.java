package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class HSnapshotDescriptor implements Writable {
  public static final String SNAPSHOTINFO_FILE = ".snapshotinfo";
  private byte[] snapshotName;
  private byte[] tableName;
  private long creationTime;
  
  /**
   * Default constructor is only used for deserialization
   */
  public HSnapshotDescriptor() {
    snapshotName = null;
    tableName = null;
    creationTime = 0L;
  }
  
  /**
   * Construct a HSnapshotDescriptor whose creationTime is current time
   * 
   * @param snapshotName
   * @param tableName
   */
  public HSnapshotDescriptor(final byte[] snapshotName, final byte[] tableName) {
    this(snapshotName, tableName, new Date().getTime());
  }
  
  public HSnapshotDescriptor(final byte[] snapshotName, final byte[] tableName, final long creationTime) {
    this.snapshotName = snapshotName;
    this.tableName = tableName;
    this.creationTime = creationTime;
  }
  
  public byte[] getSnapshotName() {
    return snapshotName;
  }
  
  public String getSnapshotNameAsString() {
    return Bytes.toString(snapshotName);
  }
  
  public byte[] getTableName() {
    return tableName;
  }
  
  public String getTableNameAsString() {
    return Bytes.toString(tableName);
  }

  public long getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(long creationTime) {
    this.creationTime = creationTime;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
   this.snapshotName = Bytes.readByteArray(in);
   this.creationTime = in.readLong();
   this.tableName = Bytes.readByteArray(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, snapshotName);
    out.writeLong(creationTime);
    Bytes.writeByteArray(out, tableName);
  }

//  /**
//   * Dump the snapshot meta information into DataOutput 
//   * 
//   * @param out 
//   * @throws IOException
//   */
//  public void dumpSnapshotInfo(DataOutput out) throws IOException {
//    Bytes.writeByteArray(out, snapshotName);
//    out.writeLong(creationTime);
//    
//    HTable table = new HTable(tableName);
//    HTableDescriptor tableDesc = table.getTableDescriptor();
//    tableDesc.write(out);
//  }
  
  @Override
  public String toString() {
    return "snapshotName=" + getSnapshotNameAsString() + ", tableName=" +
      getTableNameAsString() + ", creationTime=" + getCreationTime(); 
  }
  
  public static Path getSnapshotRootDir(final Path rootDir) {
    return new Path(rootDir, HConstants.SNAPSHOT_DIR);
  }
  
  public static Path getSnapshotDir(final Path rootDir, final byte [] snapshotName) {
    return new Path(new Path(rootDir, HConstants.SNAPSHOT_DIR), 
        Bytes.toString(snapshotName));
  }
}
