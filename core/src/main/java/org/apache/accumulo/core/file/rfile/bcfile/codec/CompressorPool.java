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
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Compressor factory extension that enables object pooling using Commons Pool. The design will have a keyed compressor pool and decompressor pool. The key of
 * which will be the Algorithm itself.
 *
 */
public class CompressorPool extends CompressorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CompressorObjectFactory.class);

  /**
   * Compressor pool.
   */
  GenericKeyedObjectPool<Algorithm,Compressor> compressorPool;

  /**
   * Decompressor pool
   */
  GenericKeyedObjectPool<Algorithm,Decompressor> decompressorPool;

  public CompressorPool(AccumuloConfiguration acuConf) {

    super(acuConf);

    compressorPool = new GenericKeyedObjectPool<Algorithm,Compressor>(new CompressorObjectFactory());
    // ensure that the pool grows when needed
    compressorPool.setBlockWhenExhausted(false);

    decompressorPool = new GenericKeyedObjectPool<Algorithm,Decompressor>(new DecompressorObjectFactory());
    // ensure that the pool grows when needed.
    decompressorPool.setBlockWhenExhausted(false);

    // perform the initial update.
    update(acuConf);

  }

  public void setMaxIdle(final int size) {
    // check that we are changing the value.
    // this will avoid synchronization within the pool
    if (size != compressorPool.getMaxIdlePerKey())
      compressorPool.setMaxIdlePerKey(size);
    if (size != decompressorPool.getMaxIdlePerKey())
      decompressorPool.setMaxIdlePerKey(size);
  }

  @Override
  public Compressor getCompressor(Algorithm compressionAlgorithm) throws IOException {
    Preconditions.checkNotNull(compressionAlgorithm, "Algorithm cannot be null");
    try {
      return compressorPool.borrowObject(compressionAlgorithm);
    } catch (Exception e) {
      // could not borrow the object, therefore we will attempt to create it
      // this will most likely result in an exception when returning,
      // so end will be called instead
      LOG.warn("Could not borrow compressor; creating instead. Error message {}", e.getMessage(), e);
      return compressionAlgorithm.getCodec().createCompressor();
    }
  }

  @Override
  public boolean releaseCompressor(Algorithm compressionAlgorithm, Compressor compressor) {
    Preconditions.checkNotNull(compressionAlgorithm, "Algorithm cannot be null");
    Preconditions.checkNotNull(compressor, "Compressor should not be null");
    try {
      compressorPool.returnObject(compressionAlgorithm, compressor);
      return true;
    } catch (Exception e) {
      LOG.warn("Could not return compressor. Closing instead. Error message {}", e.getMessage(), e);
      compressor.end();
      return true;
    }

  }

  @Override
  public boolean releaseDecompressor(Algorithm compressionAlgorithm, Decompressor decompressor) {
    Preconditions.checkNotNull(compressionAlgorithm, "Algorithm cannot be null");
    Preconditions.checkNotNull(decompressor, "Deompressor should not be null");
    try {
      decompressorPool.returnObject(compressionAlgorithm, decompressor);
      return true;
    } catch (Exception e) {
      LOG.warn("Could not return decompressor. Closing instead. Error message {}",e.getMessage(), e);
      decompressor.end();
      return true;
    }

  }

  @Override
  public Decompressor getDecompressor(Algorithm compressionAlgorithm) {
    Preconditions.checkNotNull(compressionAlgorithm, "Algorithm cannot be null");
    try {
      return decompressorPool.borrowObject(compressionAlgorithm);
    } catch (Exception e) {
      LOG.warn("Could not borrow decompressor; creating instead. Error message {}", e.getMessage(), e);
      // could not borrow the object, therefore we will attempt to create it
      // this will most likely result in an exception when returning so an end will occur
      return compressionAlgorithm.getCodec().createDecompressor();
    }
  }

  /**
   * Closes both pools, which will clear and evict the respective compressor/decompressors. {@inheritDoc}
   */
  @Override
  public void close() {
    try {
      compressorPool.close();
    } catch (Exception e) {
      LOG.error("Error while closing compressor pool. Error message {}",e.getMessage(),e);
    }
    try {
      decompressorPool.close();
    } catch (Exception e) {
      LOG.error("Error while closing decompressor pool. Error message {}",e.getMessage(),e);
    }

  }

  /**
   * Updates the maximum number of idle objects allowed, the sweep time, and the minimum time before eviction is used {@inheritDoc}
   */
  @Override
  public void update(final AccumuloConfiguration acuConf) {
    try {
      final int poolMaxIdle = acuConf.getCount(Property.TSERV_COMPRESSOR_POOL_IDLE);
      setMaxIdle(poolMaxIdle);

      final long idleSweepTimeMs = acuConf.getTimeInMillis(Property.TSERV_COMPRESSOR_POOL_IDLE_SWEEP_TIME);

      setIdleSweepTime(idleSweepTimeMs);
      final long idleStoreTimeMs = acuConf.getTimeInMillis(Property.TSERV_COMPRESSOR_POOL_IDLE_STORE_TIME);
      setIdleStoreTime(idleStoreTimeMs);

    } catch (Exception e) {
      LOG.error("Invalid compressor pool configuration", e);
    }
  }

  /**
   * Sets the minimum amount of time may pass before a (de)compressor may be evicted.
   *
   * @param idleStoreTimeMs
   *          minimum time in ms before a (de)compressor is considered for eviction.
   */
  public void setIdleStoreTime(final long idleStoreTimeMs) {

    if (idleStoreTimeMs > 0) {
      // if > 0, then we check that we aren't setting it to the same value
      // we used previously. If so, we call the setter, from which a thread
      // will be launched.
      if (compressorPool.getMinEvictableIdleTimeMillis() != idleStoreTimeMs) {

        compressorPool.setMinEvictableIdleTimeMillis(idleStoreTimeMs);
      }

      if (decompressorPool.getMinEvictableIdleTimeMillis() != idleStoreTimeMs) {
        decompressorPool.setMinEvictableIdleTimeMillis(idleStoreTimeMs);
      }
    } else {
      if (compressorPool.getMinEvictableIdleTimeMillis() > 0) {
        compressorPool.setMinEvictableIdleTimeMillis(-1);
      }

      if (decompressorPool.getMinEvictableIdleTimeMillis() > 0) {
        decompressorPool.setMinEvictableIdleTimeMillis(-1);
      }
    }
  }

  /**
   * Sets the idle sweep time if &gt; 0.
   *
   * @param idleSweepTimeMs
   *          idle sweep time.
   */
  public void setIdleSweepTime(final long idleSweepTimeMs) {
    if (idleSweepTimeMs > 0) {
      // if > 0, then we check that we aren't setting it to the same value
      // we used previously. If so, we call the setter, from which a thread
      // will be launched.
      if (compressorPool.getTimeBetweenEvictionRunsMillis() != idleSweepTimeMs) {

        compressorPool.setTimeBetweenEvictionRunsMillis(idleSweepTimeMs);
      }

      if (decompressorPool.getTimeBetweenEvictionRunsMillis() != idleSweepTimeMs) {
        decompressorPool.setTimeBetweenEvictionRunsMillis(idleSweepTimeMs);
      }
    } else {
      if (compressorPool.getTimeBetweenEvictionRunsMillis() > 0) {
        compressorPool.setTimeBetweenEvictionRunsMillis(-1);
      }

      if (decompressorPool.getTimeBetweenEvictionRunsMillis() > 0) {
        decompressorPool.setTimeBetweenEvictionRunsMillis(-1);
      }

    }

  }
}
