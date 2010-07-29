package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * HSnapshotDescriptor contains the basic information for a snapshot,
 * including snapshot name, table name and the creation time.
 */
public class HSnapshotDescriptor implements Writable {
  /**
   * The file contains the snapshot basic information and it
   * is under the directory of a snapshot. 
   */
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

  /**
   * @param snapshotName
   * @param tableName
   * @param creationTime
   */
  public HSnapshotDescriptor(final byte[] snapshotName, final byte[] tableName, 
      final long creationTime) {
    this.snapshotName = snapshotName;
    this.tableName = tableName;
    this.creationTime = creationTime;
  }

  /** @return name of snapshot */
  public byte[] getSnapshotName() {
    return snapshotName;
  }

  /** @return name of snapshot as String */
  public String getSnapshotNameAsString() {
    return Bytes.toString(snapshotName);
  }

  /** @return name of table */
  public byte[] getTableName() {
    return tableName;
  }

  /** @return name of table as String */
  public String getTableNameAsString() {
    return Bytes.toString(tableName);
  }

  /** @return creation time of the snapshot */
  public long getCreationTime() {
    return creationTime;
  }

  /**
   * @param creationTime
   */
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

  @Override
  public String toString() {
    return "snapshotName=" + getSnapshotNameAsString() + ", tableName=" +
      getTableNameAsString() + ", creationTime=" + getCreationTime();
  }

  /**
   * Get the snapshot root directory. All the snapshots are kept under this 
   * directory, e.g. /hbase/.snapshot 
   *
   * @param rootDir hbase root directory
   * @return
   */
  public static Path getSnapshotRootDir(final Path rootDir) {
    return new Path(rootDir, HConstants.SNAPSHOT_DIR);
  }

  /**
   * Get the directory for a specified snapshot. This directory is a
   * sub-directory of snapshot root directory and all the data files
   * for a snapshot are kept under this directory.
   *
   * @param rootDir hbase root directory
   * @param snapshotName
   * @return
   */
  public static Path getSnapshotDir(final Path rootDir, final byte [] snapshotName) {
    return new Path(new Path(rootDir, HConstants.SNAPSHOT_DIR),
        Bytes.toString(snapshotName));
  }
}
