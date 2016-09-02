package org.apache.accumulo.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.file.rfile.bcfile.Compression;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.accumulo.core.file.rfile.bcfile.codec.CompressorFactory;
import org.apache.accumulo.core.file.rfile.bcfile.codec.NonPooledFactory;
import org.apache.accumulo.core.file.rfile.bcfile.codec.pool.CodecPoolImpl;
import org.apache.accumulo.core.file.rfile.bcfile.codec.pool.CompressorPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

public class CompressionPerformanceTest {

  HashMap<Compression.Algorithm,Boolean> isSupported = new HashMap<>();

  private boolean isSupported(Algorithm al) {
    if (isSupported.get(al) != null && isSupported.get(al) == true) {
      return true;
    }
    return false;
  }

  public boolean isWithin(double pct, long x, long y) {
    double res = (((double) Math.abs(x - y) / (double) x) * 100);
    System.out.println(res);
    return res <= pct;
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
        

        System.out.println(commonsCounter + " " + hadoopCounter + " ? " + (commonsCounter < hadoopCounter));
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
   * 
   * @throws IOException
   *           Error during compression or decompression.
   */
  public void testPoolingVersusNonPooling() throws IOException {
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
    CompressorFactory commonsCodecPool = new CompressorPool(DefaultConfiguration.getDefaultConfiguration());
    CompressorFactory nonPool = new NonPooledFactory(DefaultConfiguration.getDefaultConfiguration());

    for (final Algorithm al : Algorithm.values()) {
      if (isSupported(al)) {
        // / test compressors

        ArrayList<Compressor> compressorList = new ArrayList<>(50001);
        ts = System.currentTimeMillis();
        for (int i = 0; i < 50000; i++) {
          compressorList.add(hadoopCodecPool.getCompressor(al));

        }
        for (Compressor compressor : compressorList) {
          hadoopCodecPool.releaseCompressor(al, compressor);
        }
        hadoopCounter = System.currentTimeMillis() - ts;
        // using downStreamSize as the buffer size
        compressorList = new ArrayList<>(50001);

        ts = System.currentTimeMillis();
        for (int i = 0; i < 50000; i++) {
          compressorList.add(commonsCodecPool.getCompressor(al));
        }
        for (Compressor compressor : compressorList) {
          commonsCodecPool.releaseCompressor(al, compressor);
        }
        commonsCounter = System.currentTimeMillis() - ts;

        compressorList = new ArrayList<>(50001);
        ts = System.currentTimeMillis();
        for (int i = 0; i < 50000; i++) {
          compressorList.add(nonPool.getCompressor(al));
        }
        for (Compressor compressor : compressorList) {
          nonPool.releaseCompressor(al, compressor);
        }
        nonCounter = System.currentTimeMillis() - ts;

        System.out.println(commonsCounter + " " + hadoopCounter + " " + nonCounter + " " + isWithin(20, nonCounter, commonsCounter));
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

  public void testPoolsWhenPruneCommons() throws IOException {
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

        System.out.println(commonsCounter + " " + hadoopCounter + " ? " + (commonsCounter < hadoopCounter));
      }
    }

  }

  public static void main(String[] args) throws IOException {
    CompressionPerformanceTest test = new CompressionPerformanceTest();
    test.buildSupport();
    test.testVanillaPoolsCreate();
    test.testPoolingVersusNonPooling();
    test.testVanillaPoolsImmediateReturn();
  }

}
