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
package org.apache.accumulo.cluster;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;

/**
 * Configuration class for an {@link AccumuloCluster}
 *
 * (Experimental, not guaranteed to stay backwards compatible)
 *
 * @since 1.6.0
 */
public interface AccumuloConfig {

  /**
   * Calling this method is optional. If not set, it defaults to two.
   *
   * @param numTservers
   *          the number of tablet servers that mini accumulo cluster should start
   */
  public AccumuloConfig setNumTservers(int numTservers);

  /**
   * Calling this method is optional. If not set, defaults to 'miniInstance'
   */
  public AccumuloConfig setInstanceName(String instanceName);

  /**
   * Calling this method is optional. If not set, it defaults to an empty map.
   *
   * @param siteConfig
   *          key/values that you normally put in accumulo-site.xml can be put here.
   */
  public AccumuloConfig setSiteConfig(Map<String,String> siteConfig);

  /**
   * Calling this method is optional. A random port is generated by default
   *
   * @param zooKeeperPort
   *          A valid (and unused) port to use for the zookeeper
   */
  public AccumuloConfig setZooKeeperPort(int zooKeeperPort);

  /**
   * Sets the amount of memory to use in the master process. Calling this method is optional. Default memory is 128M
   *
   * @param serverType
   *          the type of server to apply the memory settings
   * @param memory
   *          amount of memory to set
   *
   * @param memoryUnit
   *          the units for which to apply with the memory size
   */
  public AccumuloConfig setMemory(ServerType serverType, long memory, MemoryUnit memoryUnit);

  /**
   * Sets the default memory size to use. This value is also used when a ServerType has not been configured explicitly. Calling this method is optional. Default
   * memory is 128M
   *
   * @param memory
   *          amount of memory to set
   *
   * @param memoryUnit
   *          the units for which to apply with the memory size
   */
  public AccumuloConfig setDefaultMemory(long memory, MemoryUnit memoryUnit);

  /**
   * @return a copy of the site config
   */
  public Map<String,String> getSiteConfig();

  /**
   * @return The configured ZooKeeper port
   */
  public int getZooKeeperPort();

  /**
   * @return name of configured instance
   */
  public String getInstanceName();

  /**
   * @param serverType
   *          get configuration for this server type
   *
   * @return memory configured in bytes, returns default if this server type is not configured
   */
  public long getMemory(ServerType serverType);

  /**
   * @return memory configured in bytes
   */
  public long getDefaultMemory();

  /**
   * @return the root password of this cluster configuration
   */
  public String getRootPassword();

  /**
   * @return the number of tservers configured for this cluster
   */
  public int getNumTservers();

  /**
   * @return the paths to use for loading native libraries
   */
  public String[] getNativeLibPaths();

  /**
   * Sets the path for processes to use for loading native libraries
   *
   * @param nativePathItems
   *          the nativePathItems to set
   */
  public AccumuloConfig setNativeLibPaths(String... nativePathItems);

  /**
   * Build the appropriate {@link AccumuloCluster} from this configuration
   *
   * @return An {@link AccumuloCluster}
   */
  public AccumuloCluster build() throws IOException;
}