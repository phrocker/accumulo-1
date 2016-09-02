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
package org.apache.accumulo.core.file.rfile.bcfile.codec;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * Compressor Factory is an abstract class which creates compressors based on the supplied algorithm. Extensions may allow for alternative factory methods, such
 * as object pooling.
 */
public abstract class CompressorFactory {

  public CompressorFactory(AccumuloConfiguration acuConf) {
    initialize(acuConf);
  }

  /**
   * Provides the caller a compressor object.
   *
   * @param compressionAlgorithm
   *          compressor's algorithm.
   * @return compressor.
   * @throws IOException
   *           I/O Exception during factory implementation
   */
  public abstract Compressor getCompressor(Algorithm compressionAlgorithm) throws IOException;

  /**
   * Method to release a compressor. This implementation will call end on the compressor.
   * 
   * Implementations that
   *
   * @param algorithm
   *          Supplied compressor's Algorithm.
   * @param compressor
   *          Compressor object
   */
  public abstract boolean releaseCompressor(Algorithm algorithm, Compressor compressor);

  /**
   * Method to release the decompressor. This implementation will call end on the decompressor.
   *
   * @param algorithm
   *          Supplied decompressor's Algorithm.
   * @param decompressor
   *          decompressor object.
   */
  public abstract boolean releaseDecompressor(Algorithm algorithm, Decompressor decompressor);

  /**
   * Provides the caller a decompressor object.
   *
   * @param compressionAlgorithm
   *          decompressor's algorithm.
   * @return decompressor.
   */
  public abstract Decompressor getDecompressor(Algorithm compressionAlgorithm) throws IOException;

  /**
   * Implementations may choose to have a close call implemented.
   */
  public abstract void close();

  /**
   * Initializes the pool
   * 
   * @param acuConf
   *          accumulo configuration
   */
  protected void initialize(final AccumuloConfiguration acuConf) {
    // no initialization necessary
  }

  /**
   * Provides the capability to update the compression factory
   *
   * @param acuConf
   *          accumulo configuration
   */
  public void update(final AccumuloConfiguration acuConf) {

  }

}
