/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.accumulo.core.file.rfile.bcfile.codec.CompressorFactory;
import org.apache.accumulo.core.file.rfile.bcfile.codec.NonPooledFactory;
import org.apache.accumulo.core.file.rfile.bcfile.codec.pool.CodecPoolImpl;
import org.apache.accumulo.core.file.rfile.bcfile.codec.pool.CompressorPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CompressionTest {

  HashMap<Compression.Algorithm,Boolean> isSupported = new HashMap<>();

  private boolean isSupported(Algorithm al) {
    if (isSupported.get(al) != null && isSupported.get(al) == true) {
      return true;
    }
    return false;
  }

  public boolean isWithin(double pct, long x, long y) {
    double res =(((double)Math.abs(x - y) / (double)x) * 100); 
    System.out.println(res);
    return res <= pct;
  }

  @Before
  public void testSupport() {
    // we can safely assert that GZ exists by virtue of it being the DefaultCodec
    isSupported.put(Compression.Algorithm.GZ, true);

    Configuration myConf = new Configuration();

    String extClazz = System.getProperty(Compression.Algorithm.CONF_LZO_CLASS);
    String clazz = (extClazz != null) ? extClazz : "org.apache.hadoop.io.compress.LzoCodec";
    try {
      CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

      Assert.assertNotNull(codec);
      isSupported.put(Compression.Algorithm.LZO, true);

    } catch (ClassNotFoundException e) {
      // that is okay
    }

    extClazz = System.getProperty(Compression.Algorithm.CONF_SNAPPY_CLASS);
    clazz = (extClazz != null) ? extClazz : "org.apache.hadoop.io.compress.SnappyCodec";
    try {
      CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

      Assert.assertNotNull(codec);

      Compression.Algorithm.SNAPPY.getCompressor();

      isSupported.put(Compression.Algorithm.SNAPPY, true);

    } catch (UnsatisfiedLinkError error) {
      // caused because the native libs aren't supported
    } catch (Exception e) {
      // not supported on this box
    }

  }

  @Test
  public void testSingle() throws IOException {

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {

        // first call to issupported should be true
        Assert.assertTrue(al + " is not supported, but should be", al.isSupported());

        Assert.assertNotNull(al + " should have a non-null codec", al.getCodec());

        Assert.assertNotNull(al + " should have a non-null codec", al.getCodec());
      }
    }
  }

  @Test
  public void testSingleNoSideEffect() throws IOException {

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {

        Assert.assertTrue(al + " is not supported, but should be", al.isSupported());

        Assert.assertNotNull(al + " should have a non-null codec", al.getCodec());

        // assert that additional calls to create will not create
        // additional codecs

        Assert.assertNotEquals(al + " should have created a new codec, but did not", System.identityHashCode(al.getCodec()), al.createNewCodec(88 * 1024));
      }
    }
  }

  @Test(timeout = 60 * 1000)
  public void testManyStartNotNull() throws IOException, InterruptedException, ExecutionException {

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {

        // first call to issupported should be true
        Assert.assertTrue(al + " is not supported, but should be", al.isSupported());

        final CompressionCodec codec = al.getCodec();

        Assert.assertNotNull(al + " should not be null", codec);

        ExecutorService service = Executors.newFixedThreadPool(10);

        ArrayList<Future<Boolean>> results = new ArrayList<>();

        for (int i = 0; i < 30; i++) {
          results.add(service.submit(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
              Assert.assertNotNull(al + " should not be null", al.getCodec());
              return true;
            }

          }));
        }

        service.shutdown();

        Assert.assertNotNull(al + " should not be null", codec);

        while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          // wait
        }

        for (Future<Boolean> result : results) {
          Assert.assertTrue(al + " resulted in a failed call to getcodec within the thread pool", result.get());
        }
      }
    }

  }

  // don't start until we have created the codec
  @Test(timeout = 60 * 1000)
  public void testManyDontStartUntilThread() throws IOException, InterruptedException, ExecutionException {

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {

        // first call to issupported should be true
        Assert.assertTrue(al + " is not supported, but should be", al.isSupported());

        ExecutorService service = Executors.newFixedThreadPool(10);

        ArrayList<Future<Boolean>> results = new ArrayList<>();

        for (int i = 0; i < 30; i++) {

          results.add(service.submit(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
              Assert.assertNotNull(al + " should have a non-null codec", al.getCodec());
              return true;
            }

          }));
        }

        service.shutdown();

        while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          // wait
        }

        for (Future<Boolean> result : results) {
          Assert.assertTrue(al + " resulted in a failed call to getcodec within the thread pool", result.get());
        }
      }
    }

  }

  @Test(timeout = 60 * 1000)
  public void testThereCanBeOnlyOne() throws IOException, InterruptedException, ExecutionException {

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {

        // first call to issupported should be true
        Assert.assertTrue(al + " is not supported, but should be", al.isSupported());

        ExecutorService service = Executors.newFixedThreadPool(20);

        ArrayList<Callable<Boolean>> list = new ArrayList<>();

        ArrayList<Future<Boolean>> results = new ArrayList<>();

        // keep track of the system's identity hashcodes.
        final HashSet<Integer> testSet = new HashSet<>();

        for (int i = 0; i < 40; i++) {
          list.add(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
              CompressionCodec codec = al.getCodec();
              Assert.assertNotNull(al + " resulted in a non-null codec", codec);
              // add the identity hashcode to the set.
              synchronized (testSet) {
                testSet.add(System.identityHashCode(codec));
              }
              return true;
            }
          });
        }

        results.addAll(service.invokeAll(list));
        // ensure that we
        Assert.assertEquals(al + " created too many codecs", 1, testSet.size());
        service.shutdown();

        while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          // wait
        }

        for (Future<Boolean> result : results) {
          Assert.assertTrue(al + " resulted in a failed call to getcodec within the thread pool", result.get());
        }
      }
    }
  }

  @Test(timeout = 60 * 1000)
  public void testChangeFactory() throws IOException, InterruptedException, ExecutionException {

    CompressorFactory factory = new NonPooledFactory(DefaultConfiguration.getDefaultConfiguration());
    final byte[] testBytes = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06};
    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {

        // first call to issupported should be true
        Assert.assertTrue(al + " is not supported, but should be", al.isSupported());

        ExecutorService service = Executors.newFixedThreadPool(20);

        ArrayList<Callable<Boolean>> list = new ArrayList<>();

        ArrayList<Future<Boolean>> results = new ArrayList<>();

        final ArrayList<Compressor> compressors = new ArrayList<>();

        for (int i = 0; i < 42; i++) {
          list.add(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
              Compressor newCompressor = al.getCompressor();
              Assert.assertNotNull(al + " resulted in a non-null compressor", newCompressor);
              int compressed = newCompressor.compress(testBytes, 0, testBytes.length);
              Assert.assertEquals(compressed, newCompressor.getBytesWritten());
              synchronized (compressors) {
                compressors.add(newCompressor);
              }
              return true;
            }
          });
        }

        results.addAll(service.invokeAll(list));

        // ensure that we
        service.shutdown();

        while (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          // wait
        }

        for (Future<Boolean> result : results) {
          Assert.assertTrue(al + " resulted in a failed call to getcodec within the thread pool", result.get());
        }

        Assert.assertEquals("Should have 42 compressors", 42, compressors.size());

        factory = new CompressorPool(DefaultConfiguration.getDefaultConfiguration());

        Compression.setCompressionFactory(factory);

        for (Compressor compressor : compressors) {
          al.returnCompressor(compressor);
        }

        /**
         * Since we've changed pools we should get an exceptions that the deflator is closed
         */
        for (Compressor compressor : compressors) {
          try {
            compressor.getBytesWritten();
            Assert.fail("Expected an error when trying to access closed compressor");
          } catch (NullPointerException npe) {

          }
        }

      }
    }
  }

  /**
   * Test to show that changing the output buffer size has a dramatic impact.
   * 
   * Calling Compression.setDataOutputBufferSize(0); causes us to use a buffer size equal to the input argument.
   * 
   * @throws IOException
   *           Error during compression or decompression.
   */
  @Test
  public void testCompareCompressionSizes() throws IOException {
    int downStreamSize = 64 * 1024;
    StringBuilder builder = new StringBuilder();
    do {
      builder.append("DEADBEEF");
    } while (builder.length() < downStreamSize);

    final byte[] beefArray = builder.toString().getBytes();
    final byte[] testArray = new byte[beefArray.length];
    long ts = 0;
    long decomCounter = 0;

    long decomCounterChange = 0;

    Compression.setDataOutputBufferSize(1 * 1024);

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {
        for (int i = 0; i < 500; i++) {
          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

          Compressor compressor = al.getCompressor();
          Decompressor decom = al.getDecompressor();
          OutputStream outStream = al.createCompressionStream(outputStream, compressor, downStreamSize);
          outStream.write(beefArray);
          outStream.close();

          ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
          ts = System.currentTimeMillis();
          InputStream inStream = al.createDecompressionStream(inputStream, decom, downStreamSize);
          inStream.read(testArray);
          inStream.close();
          decomCounter += System.currentTimeMillis() - ts;
          Assert.assertArrayEquals(testArray, beefArray);

        }
        // using downStreamSize as the buffer size
        Compression.setDataOutputBufferSize(0);

        for (int i = 0; i < 500; i++) {
          ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

          Compressor compressor = al.getCompressor();
          Decompressor decom = al.getDecompressor();
          OutputStream outStream = al.createCompressionStream(outputStream, compressor, downStreamSize);
          outStream.write(beefArray);
          outStream.close();

          ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
          ts = System.currentTimeMillis();
          InputStream inStream = al.createDecompressionStream(inputStream, decom, downStreamSize);
          inStream.read(testArray);
          inStream.close();
          decomCounterChange += System.currentTimeMillis() - ts;
          // ensure the arrays are equal, otherwise we should fail the test.
          Assert.assertArrayEquals(testArray, beefArray);

        }

        Assert.assertTrue(decomCounterChange < decomCounter);
      }
    }

  }
}
