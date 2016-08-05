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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.file.rfile.bcfile.codec.CompressorFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;

/**
 * Compression related stuff.
 */
public final class Compression {
  static final Log LOG = LogFactory.getLog(Compression.class);

  /**
   * Prevent the instantiation of class.
   */
  private Compression() {
    // nothing
  }

  static class FinishOnFlushCompressionStream extends FilterOutputStream {
    public FinishOnFlushCompressionStream(CompressionOutputStream cout) {
      super(cout);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      CompressionOutputStream cout = (CompressionOutputStream) out;
      cout.finish();
      cout.flush();
      cout.resetState();
    }
  }

  /** snappy codec **/
  public static final String COMPRESSION_SNAPPY = "snappy";
  /** compression: gzip */
  public static final String COMPRESSION_GZ = "gz";
  /** compression: lzo */
  public static final String COMPRESSION_LZO = "lzo";
  /** compression: none */
  public static final String COMPRESSION_NONE = "none";

  // data input buffer size to absorb small reads from application.
  private static final int DATA_IBUF_SIZE_DEFAULT = 1 * 1024;
  // data output buffer size to absorb small writes from application.
  private static final int DATA_OBUF_SIZE_DEFAULT = 4 * 1024;

  /**
   * Data input buffer size variable. Defaults is defined, statically, above.
   */
  private static volatile int dataInputBufferSize = DATA_IBUF_SIZE_DEFAULT;

  /**
   * Data output buffer size variable. Defaults is defined, statically, above.
   */
  private static volatile int dataOutputBufferSize = DATA_OBUF_SIZE_DEFAULT;

  /**
   * Compression algorithms. There is a static initializer, below the values defined in the enumeration, that calls the initializer of all defined codecs within
   * the Algorithm enum. This promotes a model of the following call graph of initialization by the static initializer, followed by calls to getCodec() and
   * createCompressionStream/DecompressionStream. In some cases, the compression and decompression call methods will include a different buffer size for the
   * stream. Note that if the compressed buffer size requested in these calls is zero, we will not set the buffer size for that algorithm. Instead, we will use
   * the default within the codec.
   *
   * The buffer size is configured in the Codec by way of a Hadoop Configuration reference. One approach may be to use the same Configuration object, but when
   * calls are made to createCompressionStream and DecompressionStream, with non default buffer sizes, the configuration object must be changed. In this case,
   * concurrent calls to createCompressionStream and DecompressionStream would mutate the configuration object beneath each other, requiring synchronization to
   * avoid undesirable activity via co-modification. To avoid synchronization entirely, we will create Codecs with their own Configuration object and cache them
   * for re-use. A default codec will be statically created, as mentioned above to ensure we always have a codec available at loader initialization.
   *
   * There is a Guava cache defined within Algorithm that allows us to cache Codecs for re-use. Since they will have their own configuration object and thus do
   * not need to be mutable, there is no concern for using them concurrently; however, the Guava cache exists to ensure a maximal size of the cache and
   * efficient and concurrent read/write access to the cache itself.
   *
   * To provide Algorithm specific details and to describe what is in code:
   *
   * LZO will always have the default LZO codec because the buffer size is never overridden within it.
   *
   * GZ will use the default GZ codec for the compression stream, but can potentially use a different codec instance for the decompression stream if the
   * requested buffer size does not match the default GZ buffer size of 32k.
   *
   * Snappy will use the default Snappy codec with the default buffer size of 64k for the compression stream, but will use a cached codec if the buffer size
   * differs from the default.
   */
  public static enum Algorithm {

    LZO(COMPRESSION_LZO) {
      /**
       * determines if we've checked the codec status. ensures we don't recreate the defualt codec
       */
      private final AtomicBoolean checked = new AtomicBoolean(false);
      private static final String defaultClazz = "org.apache.hadoop.io.compress.LzoCodec";
      private transient CompressionCodec codec = null;

      /**
       * Configuration option for LZO buffer size
       */
      private static final String BUFFER_SIZE_OPT = "io.compression.codec.lzo.buffersize";

      /**
       * Default buffer size
       */
      private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

      @Override
      public boolean isSupported() {
        return codec != null;
      }

      @Override
      public void initializeDefaultCodec() {
        if (!checked.get()) {
          checked.set(true);
          codec = createNewCodec(DEFAULT_BUFFER_SIZE);
        }
      }

      @Override
      CompressionCodec createNewCodec(int bufferSize) {
        String extClazz = (conf.get(CONF_LZO_CLASS) == null ? System.getProperty(CONF_LZO_CLASS) : null);
        String clazz = (extClazz != null) ? extClazz : defaultClazz;
        try {
          LOG.info("Trying to load Lzo codec class: " + clazz);
          Configuration myConf = new Configuration(conf);
          // only use the buffersize if > 0, otherwise we'll use
          // the default defined within the codec
          if (bufferSize > 0)
            myConf.setInt(BUFFER_SIZE_OPT, bufferSize);
          return (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);
        } catch (ClassNotFoundException e) {
          // that is okay
        }
        return null;
      }

      @Override
      public CompressionCodec getCodec() {
        return codec;
      }

      @Override
      public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor, int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException("LZO codec class not specified. Did you forget to set property " + CONF_LZO_CLASS + "?");
        }
        InputStream bis1 = null;
        if (downStreamBufferSize > 0) {
          bis1 = new BufferedInputStream(downStream, downStreamBufferSize);
        } else {
          bis1 = downStream;
        }
        CompressionInputStream cis = codec.createInputStream(bis1, decompressor);
        BufferedInputStream bis2 = new BufferedInputStream(cis, dataInputBufferSize == 0 ? downStreamBufferSize : dataInputBufferSize);
        return bis2;
      }

      @Override
      public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor, int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException("LZO codec class not specified. Did you forget to set property " + CONF_LZO_CLASS + "?");
        }
        OutputStream bos1 = null;
        if (downStreamBufferSize > 0) {
          bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
        } else {
          bos1 = downStream;
        }
        CompressionOutputStream cos = codec.createOutputStream(bos1, compressor);
        BufferedOutputStream bos2 = new BufferedOutputStream(new FinishOnFlushCompressionStream(cos), dataOutputBufferSize == 0 ? downStreamBufferSize
            : dataOutputBufferSize);
        return bos2;
      }

    },

    GZ(COMPRESSION_GZ) {

      private transient DefaultCodec codec = null;

      /**
       * Configuration option for gz buffer size
       */
      private static final String BUFFER_SIZE_OPT = "io.file.buffer.size";

      /**
       * Default buffer size
       */
      private static final int DEFAULT_BUFFER_SIZE = 32 * 1024;

      @Override
      public CompressionCodec getCodec() {
        return codec;
      }

      @Override
      public void initializeDefaultCodec() {
        codec = (DefaultCodec) createNewCodec(DEFAULT_BUFFER_SIZE);
      }

      /**
       * Create a new GZ codec
       *
       * @param bufferSize
       *          buffer size to for GZ
       * @return created codec
       */
      @Override
      protected CompressionCodec createNewCodec(final int bufferSize) {
        DefaultCodec myCodec = new DefaultCodec();
        Configuration myConf = new Configuration(conf);
        // only use the buffersize if > 0, otherwise we'll use
        // the default defined within the codec
        if (bufferSize > 0)
          myConf.setInt(BUFFER_SIZE_OPT, bufferSize);
        myCodec.setConf(myConf);
        return myCodec;
      }

      @Override
      public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor, int downStreamBufferSize) throws IOException {
        // Set the internal buffer size to read from down stream.
        CompressionCodec decomCodec = codec;
        // if we're not using the default, let's pull from the loading cache
        if (DEFAULT_BUFFER_SIZE != downStreamBufferSize) {
          Entry<Algorithm,Integer> sizeOpt = Maps.immutableEntry(GZ, downStreamBufferSize);
          try {
            decomCodec = codecCache.get(sizeOpt);
          } catch (ExecutionException e) {
            throw new IOException(e);
          }
        }
        CompressionInputStream cis = decomCodec.createInputStream(downStream, decompressor);
        BufferedInputStream bis2 = new BufferedInputStream(cis, dataInputBufferSize);
        return bis2;
      }

      @Override
      public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor, int downStreamBufferSize) throws IOException {
        OutputStream bos1 = null;
        if (downStreamBufferSize > 0) {
          bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
        } else {
          bos1 = downStream;
        }
        // always uses the default buffer size
        CompressionOutputStream cos = codec.createOutputStream(bos1, compressor);
        BufferedOutputStream bos2 = new BufferedOutputStream(new FinishOnFlushCompressionStream(cos), dataOutputBufferSize == 0 ? downStreamBufferSize
            : dataOutputBufferSize);
        return bos2;
      }

      @Override
      public boolean isSupported() {
        return true;
      }
    },

    NONE(COMPRESSION_NONE) {
      @Override
      public CompressionCodec getCodec() {
        return null;
      }

      @Override
      public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor, int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedInputStream(downStream, downStreamBufferSize);
        }
        return downStream;
      }

      @Override
      public void initializeDefaultCodec() {

      }

      @Override
      protected CompressionCodec createNewCodec(final int bufferSize) {
        return null;
      }

      @Override
      public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor, int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedOutputStream(downStream, downStreamBufferSize);
        }

        return downStream;
      }

      @Override
      public boolean isSupported() {
        return true;
      }
    },

    SNAPPY(COMPRESSION_SNAPPY) {
      // Use base type to avoid compile-time dependencies.
      private transient CompressionCodec snappyCodec = null;
      /**
       * determines if we've checked the codec status. ensures we don't recreate the defualt codec
       */
      private final AtomicBoolean checked = new AtomicBoolean(false);
      private static final String defaultClazz = "org.apache.hadoop.io.compress.SnappyCodec";

      /**
       * Buffer size option
       */
      private static final String BUFFER_SIZE_OPT = "io.compression.codec.snappy.buffersize";

      /**
       * Default buffer size value
       */
      private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

      @Override
      public CompressionCodec getCodec() {
        return snappyCodec;
      }

      @Override
      public void initializeDefaultCodec() {
        if (!checked.get()) {
          checked.set(true);
          snappyCodec = createNewCodec(DEFAULT_BUFFER_SIZE);
        }
      }

      /**
       * Creates a new snappy codec.
       *
       * @param bufferSize
       *          incoming buffer size
       * @return new codec or null, depending on if installed
       */
      @Override
      protected CompressionCodec createNewCodec(final int bufferSize) {

        String extClazz = (conf.get(CONF_SNAPPY_CLASS) == null ? System.getProperty(CONF_SNAPPY_CLASS) : null);
        String clazz = (extClazz != null) ? extClazz : defaultClazz;
        try {
          LOG.info("Trying to load snappy codec class: " + clazz);

          Configuration myConf = new Configuration(conf);
          // only use the buffersize if > 0, otherwise we'll use
          // the default defined within the codec
          if (bufferSize > 0)
            myConf.setInt(BUFFER_SIZE_OPT, bufferSize);

          return (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), myConf);

        } catch (ClassNotFoundException e) {
          // that is okay
        }

        return null;
      }

      @Override
      public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor, int downStreamBufferSize) throws IOException {

        if (!isSupported()) {
          throw new IOException("SNAPPY codec class not specified. Did you forget to set property " + CONF_SNAPPY_CLASS + "?");
        }
        OutputStream bos1 = null;
        if (downStreamBufferSize > 0) {
          bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
        } else {
          bos1 = downStream;
        }
        // use the default codec
        CompressionOutputStream cos = snappyCodec.createOutputStream(bos1, compressor);
        BufferedOutputStream bos2 = new BufferedOutputStream(new FinishOnFlushCompressionStream(cos), dataOutputBufferSize == 0 ? downStreamBufferSize
            : dataOutputBufferSize);
        return bos2;
      }

      @Override
      public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor, int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException("SNAPPY codec class not specified. Did you forget to set property " + CONF_SNAPPY_CLASS + "?");
        }

        CompressionCodec decomCodec = snappyCodec;
        // if we're not using the same buffer size, we'll pull the codec from the loading cache
        if (DEFAULT_BUFFER_SIZE != downStreamBufferSize) {
          Entry<Algorithm,Integer> sizeOpt = Maps.immutableEntry(SNAPPY, downStreamBufferSize);
          try {
            decomCodec = codecCache.get(sizeOpt);
          } catch (ExecutionException e) {
            throw new IOException(e);
          }
        }

        CompressionInputStream cis = decomCodec.createInputStream(downStream, decompressor);
        BufferedInputStream bis2 = new BufferedInputStream(cis, dataInputBufferSize == 0 ? downStreamBufferSize : dataInputBufferSize);
        return bis2;
      }

      @Override
      public boolean isSupported() {

        return snappyCodec != null;
      }
    };

    /**
     * The model defined by the static block, below, creates a singleton for each defined codec in the Algorithm enumeration. By creating the codecs, each call
     * to isSupported shall return true/false depending on if the codec singleton is defined. The static initializer, below, will ensure this occurs when the
     * Enumeration is loaded. Furthermore, calls to getCodec will return the singleton, whether it is null or not.
     *
     * Calls to createCompressionStream and createDecompressionStream may return a different codec than getCodec, if the incoming downStreamBufferSize is
     * different than the default. In such a case, we will place the resulting codec into the codecCache, defined below, to ensure we have cache codecs.
     *
     * Since codecs are immutable, there is no concern about concurrent access to the CompressionCodec objects within the guava cache.
     */
    static {
      conf = new Configuration();
      for (final Algorithm al : Algorithm.values()) {
        al.initializeDefaultCodec();
      }
    }

    /**
     * Guava cache to have a limited factory pattern defined in the Algorithm enum.
     */
    private static LoadingCache<Entry<Algorithm,Integer>,CompressionCodec> codecCache = CacheBuilder.newBuilder().maximumSize(25)
        .build(new CacheLoader<Entry<Algorithm,Integer>,CompressionCodec>() {
          @Override
          public CompressionCodec load(Entry<Algorithm,Integer> key) {
            return key.getKey().createNewCodec(key.getValue());
          }
        });

    // We require that all compression related settings are configured
    // statically in the Configuration object.
    protected static final Configuration conf;
    private final String compressName;

    public static final String CONF_LZO_CLASS = "io.compression.codec.lzo.class";
    public static final String CONF_SNAPPY_CLASS = "io.compression.codec.snappy.class";

    Algorithm(String name) {
      this.compressName = name;
    }

    public abstract CompressionCodec getCodec();

    /**
     * function to create the default codec object.
     */
    abstract void initializeDefaultCodec();

    /**
     * Shared function to create new codec objects. It is expected that if buffersize is invalid, a codec will be created with the default buffer size
     *
     * @param bufferSize
     *          configured buffer size.
     * @return new codec
     */
    abstract CompressionCodec createNewCodec(int bufferSize);

    public abstract InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor, int downStreamBufferSize) throws IOException;

    public abstract OutputStream createCompressionStream(OutputStream downStream, Compressor compressor, int downStreamBufferSize) throws IOException;

    public abstract boolean isSupported();

    public Compressor getCompressor() throws IOException {
      return compressorFactory.getCompressor(this);
    }

    public void returnCompressor(Compressor compressor) {
      compressorFactory.releaseCompressor(this, compressor);
    }

    public Decompressor getDecompressor() throws IOException {
      return compressorFactory.getDecompressor(this);
    }

    public void returnDecompressor(Decompressor decompressor) {
      compressorFactory.releaseDecompressor(this, decompressor);
    }

    public String getName() {
      return compressName;
    }
  }

  /**
   * Default implementation will create new compressors.
   */
  private static CompressorFactory compressorFactory = new CompressorFactory(null);

  /**
   * Allow the compressor factory to be set within this Instance.
   *
   * @param compFactory
   *          incoming compressor factory to be used by all Algorithms
   */
  public static synchronized void setCompressionFactory(final CompressorFactory compFactory) {
    Preconditions.checkNotNull(compFactory, "Compressor Factory cannot be null");
    if (null != compressorFactory) {
      compressorFactory.close();
    }

    compressorFactory = compFactory;
  }

  /**
   * Adjusts the input buffer size
   *
   * @param inputBufferSize
   *          configured input buffer size
   */
  public static synchronized void setDataInputBufferSize(final int inputBufferSize) {
    dataInputBufferSize = inputBufferSize;
  }

  /**
   * Adjusts the output buffer size
   *
   * @param dataOutputBufferSizeOpt
   *          configured output buffer size
   */
  public static synchronized void setDataOutputBufferSize(final int dataOutputBufferSizeOpt) {
    dataOutputBufferSize = dataOutputBufferSizeOpt;
  }

  static Algorithm getCompressionAlgorithmByName(String compressName) {
    Algorithm[] algos = Algorithm.class.getEnumConstants();

    for (Algorithm a : algos) {
      if (a.getName().equals(compressName)) {
        return a;
      }
    }

    throw new IllegalArgumentException("Unsupported compression algorithm name: " + compressName);
  }

  public static String[] getSupportedAlgorithms() {
    Algorithm[] algos = Algorithm.class.getEnumConstants();

    ArrayList<String> ret = new ArrayList<String>();
    for (Algorithm a : algos) {
      if (a.isSupported()) {
        ret.add(a.getName());
      }
    }
    return ret.toArray(new String[ret.size()]);
  }
}
