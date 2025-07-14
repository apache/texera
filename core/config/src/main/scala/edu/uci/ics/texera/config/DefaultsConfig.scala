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

package edu.uci.ics.texera.config

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueType}
import scala.jdk.CollectionConverters.CollectionHasAsScala

object DefaultsConfig {
  private val conf = ConfigFactory.parseResources("default.conf").resolve()
  val reinit: Boolean =
    conf.getBoolean("config-service.always-reset-configurations-to-default-values")

  val allDefaults: Map[String, String] = {
    conf
      .entrySet()
      .asScala
      .map { entry =>
        val shortKey = entry.getKey.split("\\.").last
        val value = entry.getValue.valueType() match {
          case ConfigValueType.STRING | ConfigValueType.NUMBER | ConfigValueType.BOOLEAN =>
            entry.getValue.unwrapped().toString
          case _ =>
            entry.getValue.render(ConfigRenderOptions.concise())
        }
        shortKey -> value
      }
      .toMap
  }
}
