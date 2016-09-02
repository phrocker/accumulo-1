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
package org.apache.accumulo.core.file.rfile.bcfile.codec.pool;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.accumulo.core.file.rfile.bcfile.codec.CompressorFactory;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Compressor factory extension that enables object pooling using Hadoop's Codec Pool
 *
 */
public class CodecPoolImpl extends CompressorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CodecPoolImpl.class);

  /**
   * Compressor pool constructor
   * 
   * @param acuConf
   *          accumulo configuration
   */
  public CodecPoolImpl(AccumuloConfiguration acuConf) {

    super(acuConf);
  }

  @Override
  public Compressor getCompressor(Algorithm compressionAlgorithm) throws IOException {
    Preconditions.checkNotNull(compressionAlgorithm, "Algorithm cannot be null");
    Compressor compressor = CodecPool.getCompressor(compressionAlgorithm.getCodec());
    if (null != compressor) {
      if (compressor.finished()) {
        LOG.warn("Compressor obtained from CodecPool already finished()");
      } else {
        LOG.warn("Got a compressor: {}", compressor.hashCode());
      }

      compressor.reset();
    }
    return compressor;

  }

  @Override
  public boolean releaseCompressor(Algorithm compressionAlgorithm, Compressor compressor) {
    Preconditions.checkNotNull(compressionAlgorithm, "Algorithm cannot be null");
    Preconditions.checkNotNull(compressor, "Compressor should not be null");
    CodecPool.returnCompressor(compressor);
    return true;

  }

  @Override
  public boolean releaseDecompressor(Algorithm compressionAlgorithm, Decompressor decompressor) {
    Preconditions.checkNotNull(compressionAlgorithm, "Algorithm cannot be null");
    Preconditions.checkNotNull(decompressor, "Deompressor should not be null");
    CodecPool.returnDecompressor(decompressor);
    return true;
  }

  @Override
  public Decompressor getDecompressor(Algorithm compressionAlgorithm) {
    Preconditions.checkNotNull(compressionAlgorithm, "Algorithm cannot be null");
    Decompressor decompressor = CodecPool.getDecompressor(compressionAlgorithm.getCodec());
    if (null != decompressor) {
      if (decompressor.finished()) {
        LOG.warn("Decompressor obtained from CodecPool already finished()");
      } else {
        LOG.warn("Got a decompressor: {}", decompressor.hashCode());
      }

      decompressor.reset();
    }
    return decompressor;
  }

  @Override
  public void close() {
    // no state needed. 

  }

}
