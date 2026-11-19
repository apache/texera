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

import com.typesafe.config.{
  Config,
  ConfigFactory,
  ConfigRenderOptions,
  ConfigResolveOptions,
  ConfigValueType
}

import scala.jdk.CollectionConverters.CollectionHasAsScala

object DefaultsConfig {
  private val declared = ConfigFactory.parseResources("default.conf")
  private val conf = declared.resolve()
  val reinit: Boolean =
    conf.getBoolean("config-service.always-reset-configurations-to-default-values")

  // Typed rather than read off `allDefaults`: HOCON's parsing keeps the spellings
  // USER_SYS_INVITE_ONLY has always accepted (yes/no, on/off), and an unreadable value stops
  // config-service at startup, when it loads this object to seed site_settings. `seedValues`
  // stores the parsed value too, because every reader of the row expects true or false.
  private val InviteOnlyPath = "user-sys.invite_only"
  val inviteOnly: Boolean = conf.getBoolean(InviteOnlyPath)

  // site_settings rows are keyed by the last path segment of a HOCON key, so
  // this is the single place that turns a config path into a row key.
  private def shortKey(fullKey: String): String = fullKey.split("\\.").last

  // The leaves default.conf itself declares as booleans. Resolved without the environment, because
  // a `${?ENV}` override always arrives as a string: GUI_TABS_HUB_ENABLED=on would otherwise make
  // its leaf look like a string setting.
  private val booleanPaths: Set[String] =
    declared
      .resolve(ConfigResolveOptions.noSystem())
      .entrySet()
      .asScala
      .collect {
        case entry if entry.getValue.valueType() == ConfigValueType.BOOLEAN => entry.getKey
      }
      .toSet

  val allDefaults: Map[String, String] = seedValues(conf)

  /**
    * The site_settings rows that `source` seeds: every leaf under its short key, with each leaf that
    * default.conf declares as a boolean stored as true or false, the only spellings its readers
    * accept. A value HOCON cannot read as a boolean throws, which stops config-service at startup,
    * as `reinit` and `inviteOnly` already do. A parameter rather than `conf`, so a test can feed in
    * each spelling HOCON accepts without setting the environment.
    */
  private[config] def seedValues(source: Config): Map[String, String] = {
    val entries = source
      .entrySet()
      .asScala
      .toSeq
      .map { entry =>
        val path = entry.getKey
        val value =
          if (booleanPaths.contains(path)) source.getBoolean(path).toString
          else
            entry.getValue.valueType() match {
              case ConfigValueType.STRING | ConfigValueType.NUMBER | ConfigValueType.BOOLEAN =>
                entry.getValue.unwrapped().toString
              case _ =>
                entry.getValue.render(ConfigRenderOptions.concise())
            }
        shortKey(path) -> value
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
