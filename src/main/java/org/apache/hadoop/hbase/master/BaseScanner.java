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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.SnapshotDescriptor;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.ipc.RemoteException;


/**
 * Base HRegion scanner class. Holds utilty common to <code>ROOT</code> and
 * <code>META</code> HRegion scanners.
 *
 * <p>How do we know if all regions are assigned? After the initial scan of
 * the <code>ROOT</code> and <code>META</code> regions, all regions known at
 * that time will have been or are in the process of being assigned.</p>
 *
 * <p>When a region is split the region server notifies the master of the
 * split and the new regions are assigned. But suppose the master loses the
 * split message? We need to periodically rescan the <code>ROOT</code> and
 * <code>META</code> regions.
 *    <ul>
 *    <li>If we rescan, any regions that are new but not assigned will have
 *    no server info. Any regions that are not being served by the same
 *    server will get re-assigned.</li>
 *
 *    <li>Thus a periodic rescan of the root region will find any new
 *    <code>META</code> regions where we missed the <code>META</code> split
 *    message or we failed to detect a server death and consequently need to
 *    assign the region to a new server.</li>
 *
 *    <li>if we keep track of all the known <code>META</code> regions, then
 *    we can rescan them periodically. If we do this then we can detect any
 *    regions for which we missed a region split message.</li>
 *    </ul>
 *
 * Thus just keeping track of all the <code>META</code> regions permits
 * periodic rescanning which will detect unassigned regions (new or
 * otherwise) without the need to keep track of every region.</p>
 *
 * <p>So the <code>ROOT</code> region scanner needs to wake up:
 * <ol>
 * <li>when the master receives notification that the <code>ROOT</code>
 * region has been opened.</li>
 * <li>periodically after the first scan</li>
 * </ol>
 *
 * The <code>META</code>  scanner needs to wake up:
 * <ol>
 * <li>when a <code>META</code> region comes on line</li>
 * </li>periodically to rescan the online <code>META</code> regions</li>
 * </ol>
 *
 * <p>A <code>META</code> region is not 'online' until it has been scanned
 * once.
 */
abstract class BaseScanner extends Chore {
  static final Log LOG = LogFactory.getLog(BaseScanner.class.getName());
  // These are names of new columns in a meta region offlined parent row.  They
  // are added by the metascanner after we verify that split daughter made it
  // in.  Their value is 'true' if present.
  private static final byte[] SPLITA_CHECKED =
    Bytes.toBytes(Bytes.toString(HConstants.SPLITA_QUALIFIER) + "_checked");
  private static final byte[] SPLITB_CHECKED =
    Bytes.toBytes(Bytes.toString(HConstants.SPLITB_QUALIFIER) + "_checked");
  // Make the 'true' Writable once only.
  private static byte[] TRUE_WRITABLE_AS_BYTES;
  // used to generate random accessing time for reference meta row which
  // has not been accessed before
  private final static Random rand = new Random();
  static {
    try {
      TRUE_WRITABLE_AS_BYTES = Writables.getBytes(new BooleanWritable(true));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  private final boolean rootRegion;
  protected final HMaster master;

  protected boolean initialScanComplete;
  private int maxCheckedRegions;

  protected abstract boolean initialScan();
  protected abstract void maintenanceScan();

  // will use this variable to synchronize and make sure we aren't interrupted
  // mid-scan
  final Object scannerLock = new Object();

  BaseScanner(final HMaster master, final boolean rootRegion,
      final AtomicBoolean stop) {
    super("Scanner for " + (rootRegion ? "-ROOT-":".META.") + " table",
        master.getConfiguration().
        getInt("hbase.master.meta.thread.rescanfrequency", 60 * 1000), stop);
    this.maxCheckedRegions = master.getConfiguration().
        getInt("hbase.master.meta.thread.maxcheckedregions", 5);
    this.rootRegion = rootRegion;
    this.master = master;
    this.initialScanComplete = false;
  }

  /** @return true if initial scan completed successfully */
  public boolean isInitialScanComplete() {
    return initialScanComplete;
  }

  @Override
  protected boolean initialChore() {
    return initialScan();
  }

  @Override
  protected void chore() {
    maintenanceScan();
  }

  /**
   * @param region Region to scan
   * @throws IOException
   */
  protected void scanRegion(final MetaRegion region) throws IOException {
    HRegionInterface regionServer = null;
    long scannerId = -1L;
    LOG.info(Thread.currentThread().getName() + " scanning meta region " +
      region.toString());

    // Array to hold list of split parents found.  Scan adds to list.  After
    // scan we go check if parents can be removed and that their daughters
    // are in place.
    NavigableMap<HRegionInfo, Result> splitParents =
      new TreeMap<HRegionInfo, Result>();
    NavigableMap<Long, Result> referenceRows = new TreeMap<Long, Result>();
    List<byte []> emptyRows = new ArrayList<byte []>();
    int rows = 0;
    try {
      regionServer =
        this.master.getServerConnection().getHRegionConnection(region.getServer());
      Scan s = new Scan().addFamily(HConstants.CATALOG_FAMILY).addFamily(
          HConstants.SNAPSHOT_FAMILY);
      // Make this scan do a row at a time otherwise, data can be stale.
      s.setCaching(1);
      scannerId = regionServer.openScanner(region.getRegionName(), s);
      while (true) {
        Result values = regionServer.next(scannerId);
        if (values == null || values.size() == 0) {
          break;
        }
        HRegionInfo info = master.getHRegionInfo(values.getRow(), values);
        if (info == null) {
          if (Bytes.startsWith(values.getRow(), HConstants.SNAPSHOT_ROW_PREFIX)) {
            // this row contains reference count information, add it to a map
            // which is sorted by the last checking time
            byte[] lastCheckTime = values.getValue(
                HConstants.CATALOG_FAMILY, HConstants.LASTCHECKTIME_QUALIFIER);
            if (lastCheckTime != null) {
              referenceRows.put(Bytes.toLong(lastCheckTime), values);
            } else {
              // use random negative value if the reference information
              // of a region has not been checked before
              referenceRows.put(-rand.nextLong(), values);
            }
          } else {
            emptyRows.add(values.getRow());
          }
          continue;
        }



        String serverAddress = getServerAddress(values);
        long startCode = getStartCode(values);

        // Note Region has been assigned.
        checkAssigned(regionServer, region, info, serverAddress, startCode, true);
        if (isSplitParent(info)) {
          splitParents.put(info, values);
        }
        rows += 1;
      }
      if (rootRegion) {
        this.master.getRegionManager().setNumMetaRegions(rows);
      }
    } catch (IOException e) {
      if (e instanceof RemoteException) {
        e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
        if (e instanceof UnknownScannerException) {
          // Reset scannerId so we do not try closing a scanner the other side
          // has lost account of: prevents duplicated stack trace out of the
          // below close in the finally.
          scannerId = -1L;
        }
      }
      throw e;
    } finally {
      try {
        if (scannerId != -1L && regionServer != null) {
          regionServer.close(scannerId);
        }
      } catch (IOException e) {
        LOG.error("Closing scanner",
            RemoteExceptionHandler.checkIOException(e));
      }
    }

    // Scan is finished.

    // First clean up any meta region rows which had null HRegionInfos
    if (emptyRows.size() > 0) {
      LOG.warn("Found " + emptyRows.size() + " rows with empty HRegionInfo " +
        "while scanning meta region " + Bytes.toString(region.getRegionName()));
      this.master.deleteEmptyMetaRows(regionServer, region.getRegionName(),
          emptyRows);
    }

    // Take a look at split parents to see if any we can clean up any and to
    // make sure that daughter regions are in place.
    if (splitParents.size() > 0) {
      for (Map.Entry<HRegionInfo, Result> e : splitParents.entrySet()) {
        HRegionInfo hri = e.getKey();
        cleanupAndVerifySplits(region.getRegionName(), regionServer,
          hri, e.getValue());
      }
    }

    // checking the reference count information, this will synchronize the
    // reference count in META with the number of reference files for snapshots
    int checkedRegions = 0;
    for (Map.Entry<Long, Result> row : referenceRows.entrySet()) {
      checkReferences(region.getRegionName(), regionServer,
          row.getValue(), master.getArchiveDir());
      checkedRegions++;
      // checking references is expensive, only check regions which
      // have not been checked for the longest time
      if (checkedRegions >= maxCheckedRegions) {
        break;
      }
    }

    LOG.info(Thread.currentThread().getName() + " scan of " + rows +
      " row(s) of meta region " + region.toString() + " complete");
  }

  /*
   * @param r
   * @return Empty String or server address found in <code>r</code>
   */
  static String getServerAddress(final Result r) {
    final byte[] val = r.getValue(HConstants.CATALOG_FAMILY,
        HConstants.SERVER_QUALIFIER);
    return val == null || val.length <= 0 ? "" : Bytes.toString(val);
  }

  /*
   * @param r
   * @return Return 0L or server startcode found in <code>r</code>
   */
  static long getStartCode(final Result r) {
    final byte[] val = r.getValue(HConstants.CATALOG_FAMILY,
        HConstants.STARTCODE_QUALIFIER);
    return val == null || val.length <= 0 ? 0L : Bytes.toLong(val);
  }

  /*
   * @param info Region to check.
   * @return True if this is a split parent.
   */
  private boolean isSplitParent(final HRegionInfo info) {
    if (!info.isSplit()) {
      return false;
    }
    if (!info.isOffline()) {
      LOG.warn("Region is split but not offline: " +
        info.getRegionNameAsString());
    }
    return true;
  }

  /*
   * If daughters no longer hold reference to the parents, delete the parent.
   * If the parent is lone without daughter splits AND there are references in
   * the filesystem, then a daughters was not added to .META. -- must have been
   * a crash before their addition.  Add them here.
   * @param metaRegionName Meta region name: e.g. .META.,,1
   * @param server HRegionInterface of meta server to talk to
   * @param parent HRegionInfo of split offlined parent
   * @param rowContent Content of <code>parent</code> row in
   * <code>metaRegionName</code>
   * @return True if we removed <code>parent</code> from meta table and from
   * the filesystem.
   * @throws IOException
   */
  private boolean cleanupAndVerifySplits(final byte [] metaRegionName,
    final HRegionInterface srvr, final HRegionInfo parent,
    Result rowContent)
  throws IOException {
    boolean result = false;
    // Run checks on each daughter split.
    boolean hasReferencesA = checkDaughter(metaRegionName, srvr,
      parent, rowContent, HConstants.SPLITA_QUALIFIER);
    boolean hasReferencesB = checkDaughter(metaRegionName, srvr,
        parent, rowContent, HConstants.SPLITB_QUALIFIER);
    if (!hasReferencesA && !hasReferencesB) {
      LOG.info("Deleting region " + parent.getRegionNameAsString() +
        " because daughter splits no longer hold references");
      HRegion.deleteRegion(this.master.getFileSystem(), this.master.getRootDir(),
          this.master.getArchiveDir(), parent);
      HRegion.removeRegionFromMETA(srvr, metaRegionName,
        parent.getRegionName());
      result = true;
    }
    return result;
  }


  /*
   * See if the passed daughter has references in the filesystem to the parent
   * and if not, remove the note of daughter region in the parent row: its
   * column info:splitA or info:splitB.  Also make sure that daughter row is
   * present in the .META. and mark the parent row when confirmed so we don't
   * keep checking.  The mark will be info:splitA_checked and its value will be
   * a true BooleanWritable.
   * @param metaRegionName
   * @param srvr
   * @param parent
   * @param rowContent
   * @param qualifier
   * @return True if this daughter still has references to the parent.
   * @throws IOException
   */
  private boolean checkDaughter(final byte [] metaRegionName,
    final HRegionInterface srvr, final HRegionInfo parent,
    final Result rowContent, final byte [] qualifier)
  throws IOException {
    HRegionInfo hri = getDaughterRegionInfo(rowContent, qualifier);
    boolean references = hasReferences(metaRegionName, srvr, parent, rowContent,
        hri, qualifier);
    // Return if no references.
    if (!references) return references;
    if (!verifyDaughterRowPresent(rowContent, qualifier, srvr, metaRegionName,
        hri, parent)) {
      // If we got here, then the parent row does not yet have the
      // "daughter row verified present" marker present. Add it.
      addDaughterRowChecked(metaRegionName, srvr, parent.getRegionName(), hri,
        qualifier);
    }
    return references;
  }

  /*
   * Check the daughter of parent is present in meta table.  If not there,
   * add it.
   * @param rowContent
   * @param daughter
   * @param srvr
   * @param metaRegionName
   * @param daughterHRI
   * @throws IOException
   * @return True, if parent row has marker for "daughter row verified present"
   * else, false (and will do fixup adding daughter if daughter not present).
   */
  private boolean verifyDaughterRowPresent(final Result rowContent,
      final byte [] daughter, final HRegionInterface srvr,
      final byte [] metaRegionName,
      final HRegionInfo daughterHRI, final HRegionInfo parent)
  throws IOException {
    // See if the 'checked' column is in parent. If so, we're done.
    boolean present = getDaughterRowChecked(rowContent, daughter);
    if (present) return present;
    // Parent is not carrying the splitA_checked/splitB_checked so this must
    // be the first time through here checking splitA/splitB are in metatable.
    byte [] daughterRowKey = daughterHRI.getRegionName();
    Get g = new Get(daughterRowKey);
    g.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
    Result r = srvr.get(metaRegionName, g);
    if (r == null || r.isEmpty()) {
      // Daughter row not present.  Fixup kicks in.  Insert it.
      LOG.warn("Fixup broke split: Add missing split daughter to meta," +
       " daughter=" + daughterHRI.toString() + ", parent=" + parent.toString());
      Put p = new Put(daughterRowKey);
      p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(daughterHRI));
      srvr.put(metaRegionName, p);
    }
    return present;
  }

  /*
   * Add to parent a marker that we verified the daughter exists.
   * @param metaRegionName
   * @param srvr
   * @param parent
   * @param split
   * @param daughter
   * @throws IOException
   */
  private void addDaughterRowChecked(final byte [] metaRegionName,
    final HRegionInterface srvr, final byte [] parent,
    final HRegionInfo split, final byte [] daughter)
  throws IOException {
    Put p = new Put(parent);
    p.add(HConstants.CATALOG_FAMILY, getNameOfVerifiedDaughterColumn(daughter),
      TRUE_WRITABLE_AS_BYTES);
    srvr.put(metaRegionName, p);
  }

  /*
   * @param rowContent
   * @param which
   * @return True if the daughter row has already been verified present in
   * metatable.
   * @throws IOException
   */
  private boolean getDaughterRowChecked(final Result rowContent,
    final byte[] which)
  throws IOException {
    final byte[] b = rowContent.getValue(HConstants.CATALOG_FAMILY,
      getNameOfVerifiedDaughterColumn(which));
    BooleanWritable bw = null;
    if (b != null && b.length > 0) {
      bw = (BooleanWritable)Writables.getWritable(b, new BooleanWritable());
    }
    return bw == null? false: bw.get();
  }

  /*
   * @param daughter
   * @return Returns splitA_checked or splitB_checked dependent on what
   * <code>daughter</code> is.
   */
  private static byte [] getNameOfVerifiedDaughterColumn(final byte [] daughter) {
    return (Bytes.equals(HConstants.SPLITA_QUALIFIER, daughter)
            ? SPLITA_CHECKED : SPLITB_CHECKED);
  }

  /*
   * Get daughter HRegionInfo out of parent info:splitA/info:splitB columns.
   * @param rowContent
   * @param which Whether "info:splitA" or "info:splitB" column
   * @return Deserialized content of the info:splitA or info:splitB as a
   * HRegionInfo
   * @throws IOException
   */
  private HRegionInfo getDaughterRegionInfo(final Result rowContent,
    final byte [] which)
  throws IOException {
    return Writables.getHRegionInfoOrNull(
        rowContent.getValue(HConstants.CATALOG_FAMILY, which));
  }

  /*
   * Remove mention of daughter from parent row.
   * parent row.
   * @param metaRegionName
   * @param srvr
   * @param parent
   * @param split
   * @param qualifier
   * @throws IOException
   */
  private void removeDaughterFromParent(final byte [] metaRegionName,
    final HRegionInterface srvr, final HRegionInfo parent,
    final HRegionInfo split, final byte [] qualifier)
  throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(split.getRegionNameAsString() +
        " no longer has references to " + parent.getRegionNameAsString());
    }
    Delete delete = new Delete(parent.getRegionName());
    delete.deleteColumns(HConstants.CATALOG_FAMILY, qualifier);
    srvr.delete(metaRegionName, delete);
  }

  /*
   * Checks if a daughter region -- either splitA or splitB -- still holds
   * references to parent.  If not, removes reference to the split from
   * the parent meta region row so we don't check it any more.
   * @param metaRegionName Name of meta region to look in.
   * @param srvr Where region resides.
   * @param parent Parent region name.
   * @param rowContent Keyed content of the parent row in meta region.
   * @param split Which column family.
   * @param qualifier Which of the daughters to look at, splitA or splitB.
   * @return True if still has references to parent.
   * @throws IOException
   */
  private boolean hasReferences(final byte [] metaRegionName,
    final HRegionInterface srvr, final HRegionInfo parent,
    Result rowContent, final HRegionInfo split, byte [] qualifier)
  throws IOException {
    boolean result = false;
    if (split == null) {
      return result;
    }
    Path tabledir =
      new Path(this.master.getRootDir(), split.getTableDesc().getNameAsString());
    for (HColumnDescriptor family: split.getTableDesc().getFamilies()) {
      Path p = Store.getStoreHomedir(tabledir, split.getEncodedName(),
        family.getName());
      if (!this.master.getFileSystem().exists(p)) continue;
      // Look for reference files.  Call listStatus with an anonymous
      // instance of PathFilter.
      FileStatus [] ps =
        this.master.getFileSystem().listStatus(p, new PathFilter () {
            public boolean accept(Path path) {
              return StoreFile.isReference(path);
            }
          }
      );

      if (ps != null && ps.length > 0) {
        result = true;
        break;
      }
    }
    if (!result) {
      removeDaughterFromParent(metaRegionName, srvr, parent, split, qualifier);
    }
    return result;
  }

  /*
   * Check the reference count information for HFiles. Synchronize the reference
   * count information in META and the number of reference files for snapshots.
   *
   * If the reference count information in META is inconsistent with the number
   * of reference files, update the META with the number of reference files.
   * Files which are not referred by any reference files would be removed from
   * META and archive dir.
   *
   * @param metaRegionName Name of meta region to look in.
   * @param srvr Where region resides.
   * @param values Values of the reference meta row.
   * @param archiveDir directory for archived files
   * @throws IOException
   */
  private void checkReferences(final byte [] metaRegionName,
      final HRegionInterface srvr, final Result values,
      final Path archiveDir) {
    byte[] regionName = HRegionInfo.parseReferenceMetaRow(values.getRow());
    String encodedRegionName = HRegionInfo.encodeRegionName(regionName);
    try {
      Map<String, Long> referenceFiles =
        getReferenceFilesForRegion(encodedRegionName);

      // TODO lock reference meta row for this region?
      Map<byte[], byte[]> referenceInMeta =
        values.getFamilyMap(HConstants.SNAPSHOT_FAMILY);
      for (Map.Entry<byte[], byte[]> e : referenceInMeta.entrySet()) {
        Path srcfile = new Path(Bytes.toString(e.getKey()));
        if (!referenceFiles.containsKey(srcfile.getName())) {
          // there is not reference file for this src file, remove it
          removeArchivedFile(srcfile, archiveDir, values.getRow(),
              metaRegionName, srvr);
        } else {
          long referenceCount = Bytes.toLong(e.getValue());
          long numOfRefFiles = referenceFiles.get(srcfile.getName());
          if (referenceCount != numOfRefFiles) {
            // reference count in META is inconsistent with the number of
            // reference files, update meta
            Put put = new Put(values.getRow());
            put.add(HConstants.SNAPSHOT_FAMILY, e.getKey(),
                Bytes.toBytes(numOfRefFiles));
            srvr.put(metaRegionName, put);
          }
        }
      }
      // update the last checking time
      Put put = new Put(values.getRow());
      put.add(HConstants.CATALOG_FAMILY, HConstants.LASTCHECKTIME_QUALIFIER,
          Bytes.toBytes(System.currentTimeMillis()));
      srvr.put(metaRegionName, put);
    } catch (IOException e) {
      LOG.warn("Failed to check references for region "
          + Bytes.toStringBinary(regionName), e);
    }
  }

  /*
   * Get all the reference files under snapshots directory for a region
   *
   * @param encodedRegionName directory name of the desired region
   * @return a map whose key is the file name in the this region and
   *         value is the number of reference files for this region file
   * @throws IOException
   */
  private Map<String, Long> getReferenceFilesForRegion(
      final String encodedRegionName) throws IOException {
    Map<String, Long> referenceFiles = new TreeMap<String, Long>();
    final FileSystem fs = master.getFileSystem();
    Path snapshotRoot =
      SnapshotDescriptor.getSnapshotRootDir(master.getRootDir());
    FileStatus[] snapshots =
      fs.listStatus(snapshotRoot, new FSUtils.DirFilter(fs));
    for (FileStatus snapshot : snapshots) {
      // get regions whose directory name is the same as the passed
      // encodedRegionName, actually there would be at most one
      // such region for each snapshot
      FileStatus[] regions = fs.listStatus(snapshot.getPath(),
          new PathFilter() {
            @Override
            public boolean accept(Path p) {
              try {
                if (fs.getFileStatus(p).isDir() &&
                    p.getName().equals(encodedRegionName)) {
                  return true;
                }
              } catch (IOException e) {
                LOG.warn("Failed to get file status for file: " + p);
              }
              return false;
            }
          });
      for (FileStatus regionDir : regions) {
        // recursively get all reference files under this region dir
        List<Path> files =
          FSUtils.listAllFiles(fs, regionDir.getPath(), true);
        for (Path file : files) {
          if (!StoreFile.isReference(file)) {
            // ignore non-reference files
            continue;
          }
          // reference file name is suffixed with the table name or region name
          String srcfile = file.getName().substring(0,
              file.getName().indexOf("."));
          Long count = referenceFiles.get(srcfile);
          if (count == null) {
            referenceFiles.put(srcfile, 1L);
          } else {
            referenceFiles.put(srcfile, count + 1);
          }
        }
      }
    }
    return referenceFiles;
  }

  /*
   * Remove archived file from both file system and META
   *
   * @param srcFile source file of the archived file to be removed
   * @param archiveDir directory for archived files
   * @param row row key of the reference meta row
   * @param metaRegionName
   * @param srvr
   * @throws IOException
   */
  private void removeArchivedFile(final Path srcFile, final Path archiveDir,
      final byte[] row, final byte [] metaRegionName,
      final HRegionInterface srvr) throws IOException {
    // 1. delete the archived file
    Path archivedFile = FSUtils.getHFileArchivePath(srcFile, archiveDir);
    LOG.info("Deleting archived file: " + archivedFile);
    // ignoring if it doesn't exist
    if (master.getFileSystem().exists(archivedFile)) {
      if (!master.getFileSystem().delete(archivedFile, true)) {
        throw new IOException("Failed to delete archived file: " + archivedFile);
      }
    }

    LOG.info("Removing archived file from META: " + srcFile);
    // 2. delete the file reference information in META
    Delete delete = new Delete(row);
    delete.deleteColumn(HConstants.SNAPSHOT_FAMILY,
        Bytes.toBytes(FSUtils.getPath(srcFile)));
    srvr.delete(metaRegionName, delete);
  }

  /*
   * Check the passed region is assigned.  If not, add to unassigned.
   * @param regionServer
   * @param meta
   * @param info
   * @param hostnameAndPort hostname ':' port as it comes out of .META.
   * @param startCode
   * @param checkTwice should we check twice before adding a region
   * to unassigned pool.              
   * @throws IOException
   */
  protected void checkAssigned(final HRegionInterface regionServer,
    final MetaRegion meta, HRegionInfo info,
    final String hostnameAndPort, final long startCode, boolean checkTwice)
  throws IOException {
    boolean tryAgain = false;
    String serverName = null;
    String sa = hostnameAndPort;
    long sc = startCode;
    if (sa == null || sa.length() <= 0) {
      // Scans are sloppy.  They cache a row internally so may have data that
      // is a little stale.  Make sure that for sure this serverAddress is null.
      // We are trying to avoid double-assignments.  See hbase-1784.
      Get g = new Get(info.getRegionName());
      g.addFamily(HConstants.CATALOG_FAMILY);
      Result r = regionServer.get(meta.getRegionName(), g);
      if (r != null && !r.isEmpty()) {
        sa = getServerAddress(r);
        sc = getStartCode(r);
        info = master.getHRegionInfo(r.getRow(), r);
      }
    }
    if (sa != null && sa.length() > 0) {
      serverName = HServerInfo.getServerName(sa, sc);
    }
    HServerInfo storedInfo = null;
    synchronized (this.master.getRegionManager()) {
      /* We don't assign regions that are offline, in transition or were on
       * a dead server. Regions that were on a dead server will get reassigned
       * by ProcessServerShutdown
       */
      if (info == null || info.isOffline() ||
        this.master.getRegionManager().regionIsInTransition(info.getRegionNameAsString()) ||
          (serverName != null && this.master.getServerManager().isDead(serverName))) {
        return;
      }
      if (serverName != null) {
        storedInfo = this.master.getServerManager().getServerInfo(serverName);
      }

      // If we can't find the HServerInfo, then add it to the list of
      //  unassigned regions.
      if (storedInfo == null) {
        if (checkTwice) {
          tryAgain = true;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Current assignment of " + info.getRegionNameAsString() +
              " is not valid; " + " serverAddress=" + sa +
              ", startCode=" + sc + " unknown.");
          }
          // Now get the region assigned
          this.master.getRegionManager().setUnassigned(info, true);
        }
      }
    }
    if (tryAgain) {
      // The current assignment is invalid. But we need to try again.
      if (LOG.isDebugEnabled()) {
        LOG.debug("Current assignment of " + info.getRegionNameAsString() +
          " is not valid; " + " serverAddress=" + sa +
          ", startCode=" + sc + " unknown; checking once more!");
      }
      // passing null for hostNameAndPort will force the function
      // to reget the assignment from META and protect against
      // double assignment race conditions (HBASE-2755).
      checkAssigned(regionServer, meta, info, null, 0, false);
    }
  }

  /**
   * Interrupt thread regardless of what it's doing
   */
  public void interruptAndStop() {
    synchronized(scannerLock){
      if (isAlive()) {
        super.interrupt();
        LOG.info("Interrupted");
      }
    }
  }
}
