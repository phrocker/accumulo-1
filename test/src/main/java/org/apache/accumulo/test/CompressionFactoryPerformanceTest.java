/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.file.rfile.bcfile.Compression;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.accumulo.core.file.rfile.bcfile.codec.CompressorFactory;
import org.apache.accumulo.core.file.rfile.bcfile.codec.NonPooledFactory;
import org.apache.accumulo.core.file.rfile.bcfile.codec.pool.CodecPoolImpl;
import org.apache.accumulo.core.file.rfile.bcfile.codec.pool.CompressorPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Takes an argument for the desired test.
 *
 */
public class CompressionFactoryPerformanceTest {

  HashMap<Compression.Algorithm,Boolean> isSupported = new HashMap<>();

  private boolean isSupported(Algorithm al) {
    if (isSupported.get(al) != null && isSupported.get(al) == true) {
      return true;
    }
    return false;
  }

  protected void buildSupport() {
    // we can safely assert that GZ exists by virtue of it being the DefaultCodec
    isSupported.put(Compression.Algorithm.GZ, true);

    Configuration myConf = new Configuration();

    String extClazz = System.getProperty(Compression.Algorithm.CONF_LZO_CLASS);
    String clazz = (extClazz != null) ? extClazz : "org.apache.hadoop.io.compress.LzoCodec";
    try {
      CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

      isSupported.put(Compression.Algorithm.LZO, true);

    } catch (ClassNotFoundException e) {
      // that is okay
    }

    extClazz = System.getProperty(Compression.Algorithm.CONF_SNAPPY_CLASS);
    clazz = (extClazz != null) ? extClazz : "org.apache.hadoop.io.compress.SnappyCodec";
    try {
      CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

      Compression.Algorithm.SNAPPY.getCompressor();

      isSupported.put(Compression.Algorithm.SNAPPY, true);

    } catch (UnsatisfiedLinkError error) {
      // caused because the native libs aren't supported
    } catch (Exception e) {
      // not supported on this box
    }

  }

  public void testVanillaPoolsCreate() throws IOException {
    System.out.println("You are running testVanillaPoolsCreate()");
    int downStreamSize = 2 * 1024;
    StringBuilder builder = new StringBuilder();
    do {
      builder.append("DEADBEEF");
    } while (builder.length() < downStreamSize);

    long ts = 0;
    long hadoopCounter = 0;
    long commonsCounter = 0;

    CompressorFactory hadoopCodecPool = new CodecPoolImpl(DefaultConfiguration.getDefaultConfiguration());
    CompressorFactory commonsCodecPool = new CompressorPool(DefaultConfiguration.getDefaultConfiguration());
    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {
        // / test compressors

        ArrayList<Compressor> compressorList = new ArrayList<>(100001);
        ts = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
          compressorList.add(hadoopCodecPool.getCompressor(al));

        }
        hadoopCounter = System.currentTimeMillis() - ts;
        for (Compressor compressor : compressorList) {
          hadoopCodecPool.releaseCompressor(al, compressor);
        }
        hadoopCounter = System.currentTimeMillis() - ts;
        // using downStreamSize as the buffer size
        compressorList = new ArrayList<>(100001);

        ts = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
          compressorList.add(commonsCodecPool.getCompressor(al));
        }
        commonsCounter = System.currentTimeMillis() - ts;
        for (Compressor compressor : compressorList) {
          commonsCodecPool.releaseCompressor(al, compressor);
        }

        System.out.println(commonsCounter + " " + hadoopCounter + " (commonsCounter < hadoopCounter) ? " + (commonsCounter < hadoopCounter));
      }
    }

  }

  /**
   * Test vanilla pools to demonstrate differences when doing an immediate return.
   * 
   * This effectively tests a pool whose size is 1.
   * 
   * 
   * @throws IOException
   *           Error during compression or decompression.
   */
  public void testVanillaPoolsImmediateReturn() throws IOException {
    int downStreamSize = 2 * 1024;
    StringBuilder builder = new StringBuilder();
    do {
      builder.append("DEADBEEF");
    } while (builder.length() < downStreamSize);

    long ts = 0;
    long hadoopCounter = 0;
    long commonsCounter = 0;

    CompressorFactory hadoopCodecPool = new CodecPoolImpl(DefaultConfiguration.getDefaultConfiguration());
    CompressorFactory commonsCodecPool = new CompressorPool(DefaultConfiguration.getDefaultConfiguration());
    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {
        // / test compressors

        ts = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
          Compressor compressor = hadoopCodecPool.getCompressor(al);
          hadoopCodecPool.releaseCompressor(al, compressor);

        }
        hadoopCounter = System.currentTimeMillis() - ts;
        // using downStreamSize as the buffer size

        ts = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
          Compressor compressor = commonsCodecPool.getCompressor(al);
          commonsCodecPool.releaseCompressor(al, compressor);
        }
        commonsCounter = System.currentTimeMillis() - ts;

        System.out.println(commonsCounter + " " + hadoopCounter + " ? " + (commonsCounter < hadoopCounter));
      }
    }

  }

  /**
   * Tests comparing performance of each of the factory methods.
   * 
   * This particular test includes the non pooled factory. The expectation is that non pooling should be fastest.
   * 
   * 
   * @throws IOException
   *           Error during compression or decompression.
   */
  public void testPoolingVersusNonPoolingLowLimit() throws IOException {
    System.out.println("You are running testPoolingVersusNonPoolingLowLimit()");
    int downStreamSize = 2 * 1024;
    StringBuilder builder = new StringBuilder();
    do {
      builder.append("DEADBEEF");
    } while (builder.length() < downStreamSize);

    long ts = 0;
    long hadoopCounter = 0;
    long commonsCounter = 0;
    long nonCounter = 0;

    CompressorFactory hadoopCodecPool = new CodecPoolImpl(DefaultConfiguration.getDefaultConfiguration());
    CompressorPool commonsCodecPool = new CompressorPool(DefaultConfiguration.getDefaultConfiguration());
    commonsCodecPool.setMaxIdle(5);
    commonsCodecPool.setIdleSweepTime(-1);
    commonsCodecPool.setIdleStoreTime(-1);
    CompressorFactory nonPool = new NonPooledFactory(DefaultConfiguration.getDefaultConfiguration());

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {
        // / test compressors

        ArrayList<Compressor> compressorList = new ArrayList<>(100001);
        ts = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
          compressorList.add(hadoopCodecPool.getCompressor(al));

        }
        for (Compressor compressor : compressorList) {
          hadoopCodecPool.releaseCompressor(al, compressor);
        }
        hadoopCounter = System.currentTimeMillis() - ts;
        // using downStreamSize as the buffer size
        compressorList = new ArrayList<>(100001);

        ts = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
          compressorList.add(commonsCodecPool.getCompressor(al));
        }
        for (Compressor compressor : compressorList) {
          commonsCodecPool.releaseCompressor(al, compressor);
        }
        commonsCounter = System.currentTimeMillis() - ts;

        compressorList = new ArrayList<>(100001);
        ts = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
          compressorList.add(nonPool.getCompressor(al));
        }
        for (Compressor compressor : compressorList) {
          nonPool.releaseCompressor(al, compressor);
        }
        nonCounter = System.currentTimeMillis() - ts;

        System.out.println(commonsCounter + " " + hadoopCounter + " " + nonCounter + " (commonsCounter < hadoopCounter) ? " + (commonsCounter > hadoopCounter)
            + " && (nonCounter < hadoopCounter) ? " + (nonCounter < hadoopCounter));
      }
    }

  }

  /**
   * Tests comparing performance of each of the factory methods.
   * 
   * This particular test includes the non pooled factory. The expectation is that non pooling should be fastest.
   * 
   * 
   * @throws IOException
   *           Error during compression or decompression.
   */
  public void testPoolingVersusNonPooling() throws IOException {
    System.out.println("You are running testPoolingVersusNonPooling()");
    int downStreamSize = 2 * 1024;
    StringBuilder builder = new StringBuilder();
    do {
      builder.append("DEADBEEF");
    } while (builder.length() < downStreamSize);

    long ts = 0;
    long hadoopCounter = 0;
    long commonsCounter = 0;
    long nonCounter = 0;

    CompressorFactory hadoopCodecPool = new CodecPoolImpl(DefaultConfiguration.getDefaultConfiguration());
    CompressorPool commonsCodecPool = new CompressorPool(DefaultConfiguration.getDefaultConfiguration());
    commonsCodecPool.setMaxIdle(200000);
    commonsCodecPool.setIdleSweepTime(-1);
    commonsCodecPool.setIdleStoreTime(-1);
    CompressorFactory nonPool = new NonPooledFactory(DefaultConfiguration.getDefaultConfiguration());

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {
        // / test compressors

        ArrayList<Compressor> compressorList = new ArrayList<>(100001);
        ts = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
          compressorList.add(hadoopCodecPool.getCompressor(al));

        }
        for (Compressor compressor : compressorList) {
          hadoopCodecPool.releaseCompressor(al, compressor);
        }
        hadoopCounter = System.currentTimeMillis() - ts;
        // using downStreamSize as the buffer size
        compressorList = new ArrayList<>(100001);

        ts = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
          compressorList.add(commonsCodecPool.getCompressor(al));
        }
        for (Compressor compressor : compressorList) {
          commonsCodecPool.releaseCompressor(al, compressor);
        }
        commonsCounter = System.currentTimeMillis() - ts;

        compressorList = new ArrayList<>(100001);
        ts = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
          compressorList.add(nonPool.getCompressor(al));
        }
        for (Compressor compressor : compressorList) {
          nonPool.releaseCompressor(al, compressor);
        }
        nonCounter = System.currentTimeMillis() - ts;

        System.out.println(commonsCounter + " " + hadoopCounter + " " + nonCounter + " (commonsCounter < hadoopCounter) ? " + (commonsCounter < hadoopCounter)
            + " && (nonCounter < hadoopCounter) ? " + (nonCounter < hadoopCounter));
      }
    }

  }

  /**
   * Test vanilla pools to demonstrate differences when we prune the commons pool.
   * 
   * Calling Compression.setDataOutputBufferSize(0); causes us to use a buffer size equal to the input argument.
   * 
   * @throws IOException
   *           Error during compression or decompression.
   */

  /**
   * Example that shows the case where we are keeping our pools pruned. In this case the counter will include the cost to create and release compressors.
   * 
   * @throws IOException
   */
  public void testPoolsWhenPruneCommons() throws IOException {
    System.out.println("You are running testPoolsWhenPruneCommons()");
    int downStreamSize = 2 * 1024;
    StringBuilder builder = new StringBuilder();
    do {
      builder.append("DEADBEEF");
    } while (builder.length() < downStreamSize);

    long ts = 0;
    long hadoopCounter = 0;
    long commonsCounter = 0;

    Compression.setDataOutputBufferSize(1 * 1024);

    CompressorFactory hadoopCodecPool = new CodecPoolImpl(DefaultConfiguration.getDefaultConfiguration());
    CompressorPool commonsCodecPool = new CompressorPool(DefaultConfiguration.getDefaultConfiguration());

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {
        // / test compressors
        Compression.setDataOutputBufferSize(0);

        ArrayList<Compressor> compressorList = new ArrayList<>(100001);
        ts = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
          compressorList.add(hadoopCodecPool.getCompressor(al));

        }
        for (Compressor compressor : compressorList) {
          hadoopCodecPool.releaseCompressor(al, compressor);
        }
        hadoopCounter = System.currentTimeMillis() - ts;
        // using downStreamSize as the buffer size
        compressorList = new ArrayList<>(100001);

        ts = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
          compressorList.add(commonsCodecPool.getCompressor(al));
        }
        for (Compressor compressor : compressorList) {
          commonsCodecPool.releaseCompressor(al, compressor);
        }
        commonsCounter = System.currentTimeMillis() - ts;

        System.out.println(commonsCounter + " " + hadoopCounter + "  (commonsCounter < hadoopCounter) ? " + (commonsCounter < hadoopCounter));
      }
    }

  }

  /**
   * Example that shows the case where we are keeping our pools pruned. In this case the counter will include the cost to create and release compressors.
   * 
   * @throws IOException
   */
  public void testMeasureMemory() throws IOException {
    System.out.println("You are running testMeasureMemory()");

    CodecPoolImpl hadoopCodecPool = new CodecPoolImpl(DefaultConfiguration.getDefaultConfiguration());
    NonPooledFactory nonPooled = new NonPooledFactory(DefaultConfiguration.getDefaultConfiguration());
    CompressorPool compressorPool = new CompressorPool(DefaultConfiguration.getDefaultConfiguration());
    compressorPool.setMaxIdle(50000);
    Algorithm al = Algorithm.GZ;
    // / test compressors
    Runtime rt = Runtime.getRuntime();
    ArrayList<Compressor> compressorList = new ArrayList<>(100001);
    long usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
    for (int i = 0; i < 100000; i++) {
      compressorList.add(nonPooled.getCompressor(al));

    }
    for (Compressor compressor : compressorList) {
      nonPooled.releaseCompressor(al, compressor);
    }
    compressorList = new ArrayList<>(100001);
    
    long newused = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;
    long nonPooledDiff = newused - usedMB;

    // using downStreamSize as the buffer size
    compressorList = new ArrayList<>(100001);

    usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;

    for (int i = 0; i < 100000; i++) {
      compressorList.add(hadoopCodecPool.getCompressor(al));
    }
    for (Compressor compressor : compressorList) {
      hadoopCodecPool.releaseCompressor(al, compressor);
    }
    compressorList = new ArrayList<>(100001);
    
    newused = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;

    long pooledDiff = newused - usedMB;

    // using downStreamSize as the buffer size
    compressorList = new ArrayList<>(100001);

    usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;

    for (int i = 0; i < 100000; i++) {
      Compressor compressor = hadoopCodecPool.getCompressor(al);
      hadoopCodecPool.releaseCompressor(al, compressor);
    }
    /*
    Field f;
    try {
      f = CodecPool.class.getDeclaredField("compressorPool");
      f.setAccessible(true);// Abracadabra
      Map<Class<Compressor>,List<Compressor>> hadCompPool = (Map<Class<Compressor>,List<Compressor>>) f.get(null);
      List<Compressor> comps = hadCompPool.get(al.getCodec().getCompressorType());
      System.out.println("size is " + comps.size() );  
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }*/
    
    
    
    newused = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;

    long immediateFreePool = newused - usedMB;

    // using downStreamSize as the buffer size
    compressorList = new ArrayList<>(100001);

    usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;

    for (int i = 0; i < 100000; i++) {
      compressorList.add(compressorPool.getCompressor(al));
    }
    for (Compressor compressor : compressorList) {
      compressorPool.releaseCompressor(al, compressor);
    }
    
    
    compressorList = new ArrayList<>(100001);
    newused = (rt.totalMemory() - rt.freeMemory()) / 1024 / 1024;

    long compPool = newused - usedMB;

    System.out.println(immediateFreePool + " < " + nonPooledDiff + " < " + pooledDiff + " <  " + compPool);
    
    System.out.println(immediateFreePool + " < " + nonPooledDiff + " < " + pooledDiff + " " + compPool);

  }

  public static void main(String[] args) throws IOException {
    CompressionFactoryPerformanceTest test = new CompressionFactoryPerformanceTest();
    test.buildSupport();
    switch (args[0]) {
      case "testVanillaPoolsCreate":
        test.testVanillaPoolsCreate();
        break;
      case "testPoolingVersusNonPooling":
        test.testPoolingVersusNonPooling();
        break;
      case "testPoolingVersusNonPoolingLowLimit":
        test.testPoolingVersusNonPoolingLowLimit();
        break;
      case "testMeasureMemory":
        test.testMeasureMemory();
        break;
      case "testVanillaPoolsImmediateReturn":
        test.testPoolingVersusNonPooling();
        break;
      default:
        System.out.println("Please select a valid test");

    }
  }

}
