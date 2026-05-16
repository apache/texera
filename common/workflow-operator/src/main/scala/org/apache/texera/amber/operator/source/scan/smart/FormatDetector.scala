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

package org.apache.texera.amber.operator.source.scan.smart

import org.apache.texera.amber.util.ImageFormatUtils

import java.nio.charset.Charset

object FormatDetector {

  // Magic bytes used by the formats we support.
  private val ParquetMagic: Array[Byte] = "PAR1".getBytes("US-ASCII")
  private val XlsxMagic: Array[Byte] = Array(0x50, 0x4b, 0x03, 0x04).map(_.toByte) // PK\x03\x04 ZIP container
  private val OleMagic: Array[Byte] = // legacy .xls (OLE2 compound document)
    Array(0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1).map(_.toByte)
  // Arrow IPC stream begins with "ARROW1\0\0", file format also starts with this signature.
  private val ArrowMagic: Array[Byte] = "ARROW1".getBytes("US-ASCII")

  /**
    * Cheap detection from a byte sample plus optional filename hint.
    * Order: magic bytes (most reliable) → extension → content sniff.
    */
  def detect(
      fileNameHint: Option[String],
      sample: Array[Byte],
      charset: Charset
  ): SmartFileFormat = {
    if (startsWith(sample, ParquetMagic)) return SmartFileFormat.PARQUET
    if (startsWith(sample, OleMagic)) return SmartFileFormat.EXCEL
    if (startsWith(sample, ArrowMagic)) return SmartFileFormat.ARROW
    if (ImageFormatUtils.detectFormat(sample).nonEmpty) return SmartFileFormat.IMAGE

    val extensionDetected = fileNameHint.flatMap(extensionFormat)
    if (startsWith(sample, XlsxMagic) && extensionDetected.contains(SmartFileFormat.EXCEL)) {
      return SmartFileFormat.EXCEL
    }

    extensionDetected.foreach(return _)

    sniffText(sample, charset)
  }

  /** Extension-based detection. Returns None if extension is unknown or absent. */
  def extensionFormat(fileName: String): Option[SmartFileFormat] = {
    val lower = fileName.toLowerCase
    val dot = lower.lastIndexOf('.')
    if (dot < 0) return None
    lower.substring(dot + 1) match {
      case "csv"                       => Some(SmartFileFormat.CSV)
      case "tsv" | "tab"               => Some(SmartFileFormat.TSV)
      case "json"                      => Some(SmartFileFormat.JSON)
      case "jsonl" | "ndjson"          => Some(SmartFileFormat.JSONL)
      case "arrow"                     => Some(SmartFileFormat.ARROW)
      case "parquet" | "pq"            => Some(SmartFileFormat.PARQUET)
      case "xlsx" | "xls" | "xlsm"     => Some(SmartFileFormat.EXCEL)
      case "png" | "jpg" | "jpeg" |
          "gif" | "webp"               => Some(SmartFileFormat.IMAGE)
      case "txt" | "log"               => Some(SmartFileFormat.TEXT)
      case _                           => None
    }
  }

  /**
    * Content-based sniffing for text formats when neither magic bytes nor extension
    * give a definitive answer. Heuristics:
    *   - first non-blank char `{` → JSON object → ambiguous JSON vs JSONL → look at how many
    *     `{` start at the beginning of a line
    *   - first non-blank char `[` → JSON array
    *   - lines with consistent tabs but few commas → TSV
    *   - otherwise → CSV (the most common case)
    */
  private def sniffText(sample: Array[Byte], charset: Charset): SmartFileFormat = {
    val text = new String(sample, charset)
    val trimmed = text.dropWhile(_.isWhitespace)
    if (trimmed.isEmpty) return SmartFileFormat.TEXT

    trimmed.head match {
      case '[' => return SmartFileFormat.JSON
      case '{' =>
        // Either a single JSON object, JSON array of objects pretty-printed, or JSONL.
        // JSONL: multiple lines each starting with `{`.
        val objectLineStarts = text.linesIterator
          .filter(_.nonEmpty)
          .count(line => line.headOption.contains('{'))
        return if (objectLineStarts >= 2) SmartFileFormat.JSONL else SmartFileFormat.JSON
      case _ =>
    }

    // Delimiter heuristic — only the first ~30 lines.
    val lines = text.linesIterator.take(30).filter(_.nonEmpty).toList
    if (lines.isEmpty) return SmartFileFormat.TEXT
    val tabHits = lines.count(_.contains('\t'))
    val commaHits = lines.count(_.contains(','))
    if (tabHits > 0 && tabHits >= commaHits) SmartFileFormat.TSV
    else if (commaHits > 0) SmartFileFormat.CSV
    else SmartFileFormat.TEXT
  }

  private def startsWith(sample: Array[Byte], prefix: Array[Byte]): Boolean = {
    if (sample.length < prefix.length) return false
    var i = 0
    while (i < prefix.length) {
      if (sample(i) != prefix(i)) return false
      i += 1
    }
    true
  }
}
