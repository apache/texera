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

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueType}

import scala.jdk.CollectionConverters.CollectionHasAsScala

object DefaultsConfig {
  private val conf = ConfigFactory.parseResources("default.conf").resolve()
  val reinit: Boolean =
    conf.getBoolean("config-service.always-reset-configurations-to-default-values")

  // site_settings rows are keyed by the last path segment of a HOCON key, so
  // this is the single place that turns a config path into a row key.
  private def shortKey(fullKey: String): String = fullKey.split("\\.").last

  val allDefaults: Map[String, String] = {
    val entries = conf
      .entrySet()
      .asScala
      .toSeq
      .map { entry =>
        val value = entry.getValue.valueType() match {
          case ConfigValueType.STRING | ConfigValueType.NUMBER | ConfigValueType.BOOLEAN =>
            entry.getValue.unwrapped().toString
          case _ =>
            entry.getValue.render(ConfigRenderOptions.concise())
        }
        shortKey(entry.getKey) -> value
      }

    // Since the row key is only the last path segment, two entries under
    // different sections that share a last segment would silently collapse into
    // one site_settings row (and one whitelist entry). Fail fast at load time
    // rather than serving a wrong default or an unaddressable key.
    val collisions = entries.groupBy(_._1).collect { case (k, vs) if vs.size > 1 => k }
    require(
      collisions.isEmpty,
      s"default.conf declares settings whose last path segment collides: " +
        s"${collisions.toSeq.sorted.mkString(", ")}. site_settings keys are the last path " +
        "segment, so give these a unique leaf name."
    )

    entries.toMap
  }

  /**
    * Short keys (last path segment, matching the site_settings row keys) of every
    * default declared under the given top-level sections of default.conf. Lets
    * callers derive key groups (e.g. the user-visible gui/dataset settings) from
    * the file that already defines them, instead of maintaining a parallel list.
    */
  def keysUnderSections(sections: Set[String]): Set[String] =
    conf
      .entrySet()
      .asScala
      .collect {
        case entry if sections.contains(entry.getKey.takeWhile(_ != '.')) =>
          shortKey(entry.getKey)
      }
      .toSet
}
