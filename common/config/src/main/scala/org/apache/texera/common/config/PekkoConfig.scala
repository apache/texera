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

import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

object PekkoConfig {

  // Load configuration
  private val conf: Config = ConfigFactory.parseResources("cluster.conf").resolve()

  /**
    * Translate a logback level spelling into one pekko accepts.
    *
    * cluster.conf forwards ${?TEXERA_SERVICE_LOG_LEVEL} into pekko.loglevel, but that env
    * var's vocabulary belongs to logback: the same variable drives the logback root level
    * in logback.xml, and logback cannot translate on its side (it silently falls back to
    * DEBUG on names it does not know, such as WARNING). Pekko in turn only accepts OFF,
    * ERROR, WARNING, INFO and DEBUG, and prints a LoggerException on every ActorSystem
    * creation before falling back to ERROR when handed a logback-only spelling such as
    * WARN. Translating here lets the single env knob drive both systems.
    */
  private[config] def normalizePekkoLogLevel(level: String): String =
    level.toUpperCase match {
      case "WARN"          => "WARNING"
      case "TRACE" | "ALL" => "DEBUG"
      case other           => other
    }

  // Return the complete Pekko configuration with fallback to default application config
  def pekkoConfig: Config = {
    val resolved = conf.withFallback(ConfigFactory.defaultApplication()).resolve()
    resolved.withValue(
      "pekko.loglevel",
      ConfigValueFactory.fromAnyRef(normalizePekkoLogLevel(resolved.getString("pekko.loglevel")))
    )
  }
}
