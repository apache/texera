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

package org.apache.texera.amber.util

object ImageFormatUtils {

  private val PngMagic = Array[Byte](0x89.toByte, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a)
  private val JpegMagic = Array[Byte](0xff.toByte, 0xd8.toByte, 0xff.toByte)
  private val Gif87Magic = "GIF87a".getBytes("US-ASCII")
  private val Gif89Magic = "GIF89a".getBytes("US-ASCII")
  private val RiffMagic = "RIFF".getBytes("US-ASCII")
  private val WebpMagic = "WEBP".getBytes("US-ASCII")

  def detectFormat(bytes: Array[Byte]): Option[String] = {
    if (startsWith(bytes, PngMagic)) Some("png")
    else if (startsWith(bytes, JpegMagic)) Some("jpeg")
    else if (startsWith(bytes, Gif87Magic) || startsWith(bytes, Gif89Magic)) Some("gif")
    else if (isWebp(bytes)) Some("webp")
    else None
  }

  def detectMimeType(bytes: Array[Byte]): Option[String] =
    detectFormat(bytes).map {
      case "png"  => "image/png"
      case "jpeg" => "image/jpeg"
      case "gif"  => "image/gif"
      case "webp" => "image/webp"
    }

  def extensionFormat(path: String): Option[String] = {
    val lower = path.toLowerCase
    val dot = lower.lastIndexOf('.')
    if (dot < 0) return None
    lower.substring(dot + 1) match {
      case "png"           => Some("png")
      case "jpg" | "jpeg"  => Some("jpeg")
      case "gif"           => Some("gif")
      case "webp"          => Some("webp")
      case _               => None
    }
  }

  private def isWebp(bytes: Array[Byte]): Boolean =
    bytes.length >= 12 &&
      startsWith(bytes, RiffMagic) &&
      startsWith(bytes.drop(8), WebpMagic)

  private def startsWith(bytes: Array[Byte], prefix: Array[Byte]): Boolean = {
    if (bytes.length < prefix.length) return false
    var index = 0
    while (index < prefix.length) {
      if (bytes(index) != prefix(index)) return false
      index += 1
    }
    true
  }
}
