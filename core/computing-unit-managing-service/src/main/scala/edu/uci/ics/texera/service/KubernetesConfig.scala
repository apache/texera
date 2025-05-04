/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.texera.service

import com.typesafe.config.{Config, ConfigFactory}

object KubernetesConfig {

  private val conf: Config = ConfigFactory.parseResources("kubernetes.conf").resolve()

  // Access the Kubernetes settings with environment variable fallback
  val computeUnitServiceName: String = conf.getString("kubernetes.compute-unit-service-name")
  val computeUnitPoolNamespace: String = conf.getString("kubernetes.compute-unit-pool-namespace")
  val computeUnitImageName: String = conf.getString("kubernetes.image-name")
  val computingUnitImagePullPolicy: String = conf.getString("kubernetes.image-pull-policy")

  val computeUnitPortNumber: Int = conf.getInt("kubernetes.port-num")

  val maxNumOfRunningComputingUnitsPerUser: Int =
    conf.getInt("kubernetes.max-num-of-running-computing-units-per-user")

  val cpuLimitOptions: List[String] =
    conf
      .getString("kubernetes.computing-unit-cpu-limit-options")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList

  val memoryLimitOptions: List[String] =
    conf
      .getString("kubernetes.computing-unit-memory-limit-options")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList

  val gpuLimitOptions: List[String] =
    conf
      .getString("kubernetes.computing-unit-gpu-limit-options")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toList

  // GPU resource key used directly in Kubernetes resource specifications
  val gpuResourceKey: String = conf.getString("kubernetes.computing-unit-gpu-resource-key")
}
