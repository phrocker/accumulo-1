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

import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.io.compress.Compressor;

/**
 * Factory pattern used to create compressors within CompressorPool
 *
 */
public class CompressorObjectFactory implements KeyedPooledObjectFactory<Algorithm,Compressor> {

  @Override
  public PooledObject<Compressor> makeObject(Algorithm key) throws Exception {
    return new DefaultPooledObject<Compressor>(key.getCodec().createCompressor());
  }

  @Override
  public void destroyObject(Algorithm key, PooledObject<Compressor> compressor) throws Exception {
    compressor.getObject().end();
  }

  @Override
  public boolean validateObject(Algorithm key, PooledObject<Compressor> compressor) {
    return compressor.getObject().finished();
  }

  @Override
  public void activateObject(Algorithm key, PooledObject<Compressor> compressor) throws Exception {
    compressor.getObject().reset();
  }

  @Override
  public void passivateObject(Algorithm key, PooledObject<Compressor> compressor) throws Exception {
    compressor.getObject().finish();
  }

}
