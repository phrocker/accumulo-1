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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class NonPooledFactory extends CompressorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CompressorFactory.class);

  public NonPooledFactory(AccumuloConfiguration acuConf) {
    super(acuConf);
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
  @Override
  public Compressor getCompressor(Algorithm compressionAlgorithm) throws IOException {
    if (compressionAlgorithm != null) {
      return compressionAlgorithm.getCodec().createCompressor();
    }
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean releaseCompressor(Algorithm algorithm, Compressor compressor) {
    Preconditions.checkNotNull(algorithm, "Algorithm cannot be null");
    Preconditions.checkNotNull(compressor, "Compressor should not be null");
    compressor.end();
    return true;
  }

  /**
   * Method to release the decompressor. This implementation will call end on the decompressor.
   *
   * @param algorithm
   *          Supplied decompressor's Algorithm.
   * @param decompressor
   *          decompressor object.
   */
  @Override
  public boolean releaseDecompressor(Algorithm algorithm, Decompressor decompressor) {
    Preconditions.checkNotNull(algorithm, "Algorithm cannot be null");
    Preconditions.checkNotNull(decompressor, "Decompressor should not be null");
    decompressor.end();
    return true;
  }

  /**
   * Provides the caller a decompressor object.
   *
   * @param compressionAlgorithm
   *          decompressor's algorithm.
   * @return decompressor.
   */
  @Override
  public Decompressor getDecompressor(Algorithm compressionAlgorithm) throws IOException {
    if (compressionAlgorithm != null) {
      return compressionAlgorithm.getCodec().createDecompressor();
    }
    return null;
  }

  @Override
  public void close() {
    // no close necessary

  }

}
