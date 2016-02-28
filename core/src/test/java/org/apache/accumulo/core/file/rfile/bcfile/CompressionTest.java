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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.file.rfile.bcfile.Compression.Algorithm;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class CompressionTest {

  @Test
  public void testSingleGZ() throws IOException {
    Assert.assertNotNull(Compression.Algorithm.GZ.getCodec());
  }

  @Test
  public void testSingleLZO() throws IOException {
    if (Algorithm.LZO.isSupported())
      Assert.assertNotNull(Compression.Algorithm.LZO.getCodec());
  }

  @Test
  public void testSingleSnappy() throws IOException {
    if (Algorithm.SNAPPY.isSupported())
      Assert.assertNotNull(Algorithm.SNAPPY.getCodec());
  }

  // don't start until we have created the codec
  @Test
  public void testThereCanBeOnlyOneGZ() throws IOException, InterruptedException {

    ExecutorService service = Executors.newFixedThreadPool(20);

    List<Callable<Boolean>> list = Lists.newArrayList();

    // keep track of the system's identity hashcodes.
    final Set<Integer> testSet = Collections.synchronizedSet(new HashSet<Integer>());

    for (int i = 0; i < 40; i++) {
      list.add(new Callable<Boolean>() {

        @Override
        public Boolean call() throws Exception {
          CompressionCodec codec = Compression.Algorithm.GZ.getCodec();
          Assert.assertNotNull(codec);
          // add the identity hashcode to the set.
          testSet.add(System.identityHashCode(codec));
          return true;
        }
      });
    }

    service.invokeAll(list);
    service.shutdown();

    try {
      service.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      Assert.fail("Unit test did not complete on time");
    }
    // ensure that we
    for (Integer hash : testSet) {
      System.out.println(" gz Missed " + hash);
    }
    Assert.assertEquals(1, testSet.size());

  }

  @Test
  public void testManyStartNotNullGZ() throws IOException {
    final CompressionCodec codec = Algorithm.GZ.getCodec();

    ExecutorService service = Executors.newFixedThreadPool(10);

    for (int i = 0; i < 30; i++) {
      service.submit(new Callable<Boolean>()

      {

        @Override
        public Boolean call() throws Exception {
          Assert.assertNotNull(Compression.Algorithm.GZ.getCodec());
          return true;
        }

      });
    }

    service.shutdown();

    try {
      service.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      Assert.fail("Unit test did not complete on time");
    }

    Assert.assertNotNull(codec);

  }

  // Create an LZO compressor test
  @Test
  public void testThereCanBeOnlyOneLZO() throws IOException, InterruptedException {

    if (Algorithm.LZO.isSupported()) {
      ExecutorService service = Executors.newFixedThreadPool(20);

      List<Callable<Boolean>> list = Lists.newArrayList();

      // keep track of the system's identity hashcodes.
      final Set<Integer> testSet = Collections.synchronizedSet(new HashSet<Integer>());

      for (int i = 0; i < 40; i++) {
        list.add(new Callable<Boolean>() {

          @Override
          public Boolean call() throws Exception {
            CompressionCodec codec = Compression.Algorithm.LZO.getCodec();
            Assert.assertNotNull(codec);
            // add the identity hashcode to the set.
            testSet.add(System.identityHashCode(codec));
            return true;
          }
        });
      }

      service.invokeAll(list);
      // ensure that we
      // ensure that we
      service.shutdown();

      try {
        service.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        Assert.fail("Unit test did not complete on time");
      }
      for (Integer hash : testSet) {
        System.out.println("Missed " + hash);
      }
      Assert.assertEquals(1, testSet.size());

    }

  }

  // Create a snappy compression test
  @Test
  public void testThereCanBeOnlyOneSnappy() throws IOException, InterruptedException {

    if (Algorithm.SNAPPY.isSupported()) {
      ExecutorService service = Executors.newFixedThreadPool(20);

      List<Callable<Boolean>> list = Lists.newArrayList();

      // keep track of the system's identity hashcodes.

      final Set<Integer> testSet = Collections.synchronizedSet(new HashSet<Integer>());

      for (int i = 0; i < 40; i++) {
        list.add(new Callable<Boolean>() {

          @Override
          public Boolean call() throws Exception {
            CompressionCodec codec = Compression.Algorithm.SNAPPY.getCodec();
            Assert.assertNotNull(codec);
            // add the identity hashcode to the set.
            testSet.add(System.identityHashCode(codec));
            return true;
          }
        });
      }

      service.invokeAll(list);
      service.shutdown();

      try {
        service.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        Assert.fail("Unit test did not complete on time");
      }
      for (Integer hash : testSet) {
        System.out.println("Missed snappy" + hash);
      }
      Assert.assertEquals(1, testSet.size());

    }

  }
}
