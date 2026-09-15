/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.common.config

import com.typesafe.config.{Config, ConfigFactory}

object CuratedImageConfig {

  private val conf: Config = ConfigFactory.parseResources("kubernetes.conf").resolve()

  val enabled: Boolean = conf.getBoolean("curated-images.enabled")

  val validationImage: String = conf.getString("curated-images.validation-image")
  val validationNamespace: String = conf.getString("curated-images.validation-namespace")
  val validationTimeoutSeconds: Int = conf.getInt("curated-images.validation-timeout-seconds")

  val validationCpuRequest: String = conf.getString("curated-images.validation-cpu-request")
  val validationMemoryRequest: String = conf.getString("curated-images.validation-memory-request")
  val validationCpuLimit: String = conf.getString("curated-images.validation-cpu-limit")
  val validationMemoryLimit: String = conf.getString("curated-images.validation-memory-limit")

  /**
    * What a computing unit runs, so an image that does not provide it cannot be one. The
    * computing-unit image declares it as its CMD; the validation checks for it before
    * the image can be used, so a wrong one fails in seconds and in front of the admin.
    */
  val requiredCommand: String = "computing-unit-master"

  /** Kubernetes object name for one check. Unique per attempt so retries never collide. */
  def validationJobName(iid: Int, attempt: Int): String = s"cu-image-check-$iid-$attempt"
}
