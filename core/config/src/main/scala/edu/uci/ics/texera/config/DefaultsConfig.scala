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
import edu.uci.ics.amber.util.PathUtils
import java.nio.file.Files
import java.util.Base64
import scala.jdk.CollectionConverters.MapHasAsScala

object DefaultsConfig {
  private val conf = ConfigFactory.parseResources("defaults.conf").resolve()
  private val imageKeys = Set("logo", "mini-logo", "favicon")

  private def encodeImageAsBase64(path: String): String = {
    val asset = PathUtils.corePath.resolve(path)
    require(Files.exists(asset), s"Not found: $asset")
    val bytes = Files.readAllBytes(asset)
    s"data:image/png;base64,${Base64.getEncoder.encodeToString(bytes)}"
  }

  private def getValue(v: com.typesafe.config.ConfigValue): String =
    v.valueType() match {
      case ConfigValueType.STRING | ConfigValueType.NUMBER | ConfigValueType.BOOLEAN =>
        v.unwrapped().toString
      case _ => v.render(ConfigRenderOptions.concise())
    }

  val allDefaults: Map[String, String] = conf
    .root()
    .asScala
    .map {
      case (k, v) =>
        val raw = getValue(v)
        k -> (if (imageKeys.contains(k)) encodeImageAsBase64(raw) else raw)
    }
    .toMap
}
