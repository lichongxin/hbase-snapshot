/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.Writable;

/**
 * A reference to the another store file or top or bottom half of a store file.
 * The file referenced lives under a different table or a different region.
 * References are made at table snapshot or region split time.
 *
 * <p>For references made at table snapshot, they refer to the store files of
 * that table. For references made at region split, they work with a special
 * half store file type which is used reading the referred to file.
 *
 * <p>References know how to write out the reference format in the file system
 * and are whats juggled when references are mixed in with direct store files.
 *
 * <p>References to store files located over in some other region look like
 * this in the file system
 * <code>1278437856009925445.tablename</code>:
 * i.e. an id followed by the table name of the referenced table.
 * <code>1278437856009925445.3323223323</code>:
 * i.e. an id followed by hash of the referenced region.
 * Note, a region is itself not splitable if it has instances of store file
 * references.  References are cleaned up by compactions.
 */
public class Reference implements Writable {
  private byte [] splitkey;
  private Range range;

  /**
   * For split HStoreFiles, it specifies if the file covers the lower half or
   * the upper half of the key range
   */
  public static enum Range {
    /** StoreFile contains lower half of key range */
    BOTTOM    (0),
    /** StoreFile contains upper half of key range */
    TOP       (1),
    /** StoreFile contains the entire key range */
    ENTIRE    (2);

    private final byte value;

    private Range(final int intValue) {
      this.value = (byte)intValue;
    }

    public byte getByteValue() {
      return value;
    }

    public static Range fromByte(byte value) {
      switch(value) {
      case 0:
        return Range.BOTTOM;
      case 1:
        return Range.TOP;
      case 2:
        return Range.ENTIRE;

      default:
          throw new RuntimeException("Invalid byte value for Reference.Range");
      }
    }
  }

  /**
   * Constructor
   * @param splitRow This is row we are splitting around.
   * @param fr
   */
  public Reference(final byte [] splitRow, final Range fr) {
    this.splitkey = splitRow == null?
      null: KeyValue.createFirstOnRow(splitRow).getKey();
    this.range = fr;
  }

  /**
   * Used by serializations.
   */
  public Reference() {
    this(null, Range.BOTTOM);
  }

  /**
   *
   * @return Range
   */
  public Range getFileRange() {
    return this.range;
  }

  /**
   * @return splitKey
   */
  public byte [] getSplitKey() {
    return splitkey;
  }

  /**
   * @return true if the range is Range.ENTIRE
   */
  public boolean isEntireRange() {
    return range.equals(Range.ENTIRE);
  }

  /**
   * @return true if the range is Range.TOP
   */
  public boolean isTopFileRange() {
    return range.equals(Range.TOP);
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "" + this.range;
  }

  // Make it serializable.

  public void write(DataOutput out) throws IOException {
    // Write the byte value of the range
    out.write(range.getByteValue());
    Bytes.writeByteArray(out, this.splitkey);
  }

  public void readFields(DataInput in) throws IOException {
    this.range = Range.fromByte(in.readByte());
    this.splitkey = Bytes.readByteArray(in);
  }

  /**
   * Write this reference into the given path.
   * @param fs FileSystem
   * @param p reference file path
   * @return path to the reference file
   * @throws IOException
   */
  public Path write(final FileSystem fs, final Path p)
  throws IOException {
    FSUtils.create(fs, p);
    FSDataOutputStream out = fs.create(p);
    try {
      write(out);
    } finally {
      out.close();
    }
    return p;
  }

  /**
   * Read a Reference from FileSystem.
   * @param fs
   * @param p
   * @return New Reference made from passed <code>p</code>
   * @throws IOException
   */
  public static Reference read(final FileSystem fs, final Path p)
  throws IOException {
    FSDataInputStream in = fs.open(p);
    try {
      Reference r = new Reference();
      r.readFields(in);
      return r;
    } finally {
      in.close();
    }
  }

  /**
   * Create a reference file for <code>srcFile</code> under the passed
   * <code>dstDir</code>
   *
   * @param fs FileSystem
   * @param srcFile file to be referred
   * @param dstDir directory where reference file is created
   * @return path to the reference file
   * @throws IOException if creating reference file fails
   */
  public static Path createReferenceFile(final FileSystem fs,
      final Path srcFile, final Path dstDir) throws IOException {
    // A reference to the entire store file.
    Reference r = new Reference(null, Range.ENTIRE);

    String parentTableName =
      srcFile.getParent().getParent().getParent().getName();
    // Write reference with same file id only with the other table name
    // as suffix.
    Path p = new Path(dstDir, srcFile.getName() + "." + parentTableName);
    return r.write(fs, p);
  }
}