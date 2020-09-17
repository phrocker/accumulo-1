/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.tablet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.accumulo.tserver.InMemoryMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class NativeCompactor extends Compactor {

  private static final Logger log = LoggerFactory.getLogger(NativeCompactor.class);

  public long nativePtr;

  private static AtomicBoolean loadedNativeLibraries = new AtomicBoolean(false);

  // Load native library
  static {
    // Check in directories set by JVM system property
    List<File> directories = new ArrayList<>();
    String accumuloNativeLibDirs = System.getProperty("accumulo.native.lib.path");
    if (accumuloNativeLibDirs != null) {
      for (String libDir : accumuloNativeLibDirs.split(":")) {
        directories.add(new File(libDir));
      }
    }
    // Attempt to load from these directories, using standard names
    loadNativeLib(directories);

    // Check LD_LIBRARY_PATH (DYLD_LIBRARY_PATH on Mac)
    if (!isLoaded()) {
      log.error("Tried and failed to load Accumulo native library from {}", accumuloNativeLibDirs);
      String ldLibraryPath = System.getProperty("java.library.path");

      try {
        System.loadLibrary("nativecompactions");
        loadedNativeLibraries.set(true);
        log.info("Loaded pysharkbite shared library from {}", ldLibraryPath);
      } catch (Exception | UnsatisfiedLinkError e) {
        log.error("Tried and failed to load Accumulo native library from {}", ldLibraryPath, e);
      }
    }

    // Exit if native libraries could not be loaded
  }

  /**
   * If native libraries are not loaded, the specified search path will be used to attempt to load
   * them. Directories will be searched by using the system-specific library naming conventions. A
   * path directly to a file can also be provided. Loading will continue until the search path is
   * exhausted, or until the native libraries are found and successfully loaded, whichever occurs
   * first.
   *
   * @param searchPath
   *          a list of files and directories to search
   */
  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "search paths provided by admin")
  public static void loadNativeLib(List<File> searchPath) {
    if (!isLoaded()) {
      List<String> names = getValidLibraryNames();
      List<File> tryList = new ArrayList<>(searchPath.size() * names.size());

      for (File p : searchPath)
        if (p.exists() && p.isDirectory())
          for (String name : names)
            tryList.add(new File(p, name));
        else
          tryList.add(p);

      for (File f : tryList) {
        if (f.getAbsolutePath().contains("nativecompactions"))
          loadNativeLib(f);
        else {
          System.err.println("Could not accept " + f.getAbsolutePath());
        }
      }
    }
  }

  /**
   * Check if native libraries are loaded.
   *
   * @return true if they are loaded; false otherwise
   */
  public static boolean isLoaded() {
    return loadedNativeLibraries.get();
  }

  private static List<String> getValidLibraryNames() {
    ArrayList<String> names = new ArrayList<>(3);

    String libname = System.mapLibraryName("nativecompactions");
    names.add(libname);

    int dot = libname.lastIndexOf(".");
    String prefix = dot < 0 ? libname : libname.substring(0, dot);

    // additional supported Mac extensions
    if ("Mac OS X".equals(System.getProperty("os.name")))
      for (String ext : new String[] {".dylib", ".jnilib"})
        if (!libname.endsWith(ext))
          names.add(prefix + ext);

    return names;
  }

  private static boolean loadNativeLib(File libFile) {f
    log.debug("Trying to load native map library {}", libFile);
    if (libFile.exists() && libFile.isFile()) {
      try {
        System.load(libFile.getAbsolutePath());
        loadedNativeLibraries.set(true);
        log.info("Loaded native map shared library {}", libFile);
        return true;
      } catch (Exception | UnsatisfiedLinkError e) {
        log.error("Tried and failed to load native map library " + libFile, e);
      }
    } else {
      log.debug("Native map library {} not found or is not a file.", libFile);
    }
    return false;
  }

  public NativeCompactor(ServerContext context, Tablet tablet,
      Map<StoredTabletFile,DataFileValue> files, InMemoryMap imm, TabletFile outputFile,
      boolean propogateDeletes, CompactionEnv env, List<IteratorSetting> iterators, int reason,
      AccumuloConfiguration tableConfiguation) {
    super(context, tablet, files, imm, outputFile, propogateDeletes, env, iterators, reason,
        tableConfiguation);
  }

  native void callCompact(String outputFile, List<String> files, LongAdder resulting,
      LongAdder total, LongAdder filesize);

  @Override
  public CompactionStats call() throws IOException, CompactionCanceledException {

    FileSKVWriter mfw = null;

    CompactionStats majCStats = new CompactionStats();

    boolean remove = runningCompactions.add(this);

    clearStats();

    String oldThreadName = Thread.currentThread().getName();
    String newThreadName = "MajC compacting " + extent + " started "
        + dateFormatter.format(new Date()) + " file: " + outputFile;
    Thread.currentThread().setName(newThreadName);
    thread = Thread.currentThread();
    try {

      FileOperations fileFactory = FileOperations.getInstance();
      FileSystem ns = this.fs.getFileSystemByPath(outputFile.getPath());

      Map<String,Set<ByteSequence>> lGroups = getLocalityGroups(acuTableConf);

      long t1 = System.currentTimeMillis();

      HashSet<ByteSequence> allColumnFamilies = new HashSet<>();

      setLocalityGroup("");
      long filesize = compactLocalityGroup(null, allColumnFamilies, false, outputFile, majCStats);

      long t2 = System.currentTimeMillis();

      log.trace(String.format(
          "Compaction %s %,d read | %,d written | %,6d entries/sec"
              + " | %,6.3f secs | %,12d bytes | %9.3f byte/sec",
          extent, majCStats.getEntriesRead(), majCStats.getEntriesWritten(),
          (int) (majCStats.getEntriesRead() / ((t2 - t1) / 1000.0)), (t2 - t1) / 1000.0, filesize,
          filesize / ((t2 - t1) / 1000.0)));

      majCStats.setFileSize(filesize);
      mfw = null;
      return majCStats;

    } catch (IOException | RuntimeException e) {
      log.error("{}", e.getMessage(), e);
      throw e;
    } finally {
      Thread.currentThread().setName(oldThreadName);
      if (remove) {
        thread = null;
        runningCompactions.remove(this);
      }

      if (mfw != null) {
        try {
          if (!fs.deleteRecursively(outputFile.getPath()))
            if (fs.exists(outputFile.getPath()))
              log.error("Unable to delete {}", outputFile);
        } catch (IOException | RuntimeException e) {
          log.warn("{}", e.getMessage(), e);
        }
      }
    }
  }

  protected List<String> getDataFiles(ArrayList<FileSKVIterator> readers) throws IOException {

    List<String> iters = new ArrayList<>(filesToCompact.size());

    for (TabletFile mapFile : filesToCompact.keySet()) {
      try {

        /**
         * Let Native compactions perform the bulk of this work
         */
        iters.add(mapFile.getPathStr());

      } catch (Throwable e) {

        ProblemReports.getInstance(context).report(
            new ProblemReport(extent.tableId(), ProblemType.FILE_READ, mapFile.getPathStr(), e));

        log.warn("Some problem opening map file {} {}", mapFile, e.getMessage(), e);
        // failed to open some map file... close the ones that were opened
        for (FileSKVIterator reader : readers) {
          try {
            reader.close();
          } catch (Throwable e2) {
            log.warn("Failed to close map file", e2);
          }
        }

        readers.clear();

        if (e instanceof IOException)
          throw (IOException) e;
        throw new IOException("Failed to open map data files", e);
      }
    }

    return iters;
  }

  private long compactLocalityGroup(String lgName, Set<ByteSequence> columnFamilies,
      boolean inclusive, TabletFile output, CompactionStats majCStats)
      throws IOException, CompactionCanceledException {
    ArrayList<FileSKVIterator> readers = new ArrayList<>(filesToCompact.size());
    LongAdder result = new LongAdder();
    LongAdder total = new LongAdder();
    LongAdder filesize = new LongAdder();
    try (TraceScope span = Trace.startSpan("compact")) {
      long entriesCompacted = 0;
      List<String> iters = getDataFiles(readers);
      callCompact(output.getPathStr(), iters, result, total, filesize);

    } finally {
      CompactionStats lgMajcStats = new CompactionStats(result.sum(), total.sum());
      majCStats.add(lgMajcStats);
    }

    return filesize.sum();
  }

}
