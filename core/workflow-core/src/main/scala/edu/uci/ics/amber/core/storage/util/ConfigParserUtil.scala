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

package edu.uci.ics.amber.core.storage.util

object ConfigParserUtil {
  def parseSizeStringToBytes(size: String): Long = {
    val sizePattern = """(\d+)([KMG]B)""".r
    size match {
      case sizePattern(value, unit) =>
        val multiplier = unit match {
          case "KB" => 1024L
          case "MB" => 1024L * 1024
          case "GB" => 1024L * 1024 * 1024
        }
        value.toLong * multiplier
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid s3 multipart part-size format in StorageConfig.scala with value $size"
        )
    }
  }
}
