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

import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.texera.amber.core.tuple.{AttributeType, AttributeTypeUtils}

import java.io.StringReader

/** A guess at how a CSV-family file should be read. */
case class CSVDialect(delimiter: Char, hasHeader: Boolean)

/**
  * Heuristic CSV dialect detector. Given a text sample (first ~64 KB of the file),
  * it picks the delimiter that produces the most consistent column count across rows,
  * then decides whether the first row is a header.
  *
  * Not perfect — quoted multi-line values can confuse it on very short samples — but
  * good enough for the common cases the Smart File Source wants to cover.
  */
object CSVDialectSniffer {

  private val Candidates: Seq[Char] = Seq(',', '\t', ';', '|')

  /**
    * @param sampleText decoded text sample
    * @param preferred  an extension-based hint (`,` if `.csv`, `\t` if `.tsv`). When the
    *                   data is consistent with the preferred delimiter, we keep it even
    *                   if another delimiter would score marginally higher.
    */
  def sniff(sampleText: String, preferred: Option[Char] = None): CSVDialect = {
    val scored = Candidates.map(d => d -> scoreDelimiter(sampleText, d)).toMap

    val delimiter = preferred match {
      case Some(p) if scored.getOrElse(p, 0.0) >= 0.5 => p
      case _ =>
        scored
          .filter { case (_, score) => score > 0.0 }
          .toSeq
          .sortBy { case (_, score) => -score }
          .headOption
          .map(_._1)
          .getOrElse(',') // fall back to comma; downstream parsing will surface a real error
    }

    val hasHeader = detectHeader(sampleText, delimiter)
    CSVDialect(delimiter, hasHeader)
  }

  /**
    * A delimiter is "consistent" when the per-row column count is stable across rows.
    * Score is `(rows_with_modal_count - 1) / total_rows`, in [0, 1].
    */
  private def scoreDelimiter(sample: String, delimiter: Char): Double = {
    val rows = parseRows(sample, delimiter, headerExtraction = false, maxRows = 30)
    if (rows.size < 2) return 0.0
    val counts = rows.map(_.length).filter(_ > 0)
    if (counts.length < 2) return 0.0
    val modalCount = counts.groupBy(identity).view.mapValues(_.size).maxBy(_._2)._1
    if (modalCount < 2) return 0.0 // single-column "matches" don't tell us anything
    val agreeing = counts.count(_ == modalCount)
    (agreeing - 1).toDouble / rows.size
  }

  /**
    * Header detection: parse the first row, then parse subsequent rows; if at least one
    * column has a row-1 type of STRING but later rows are numeric/boolean/timestamp, the
    * first row is probably a header.
    */
  private def detectHeader(sample: String, delimiter: Char): Boolean = {
    val rows = parseRows(sample, delimiter, headerExtraction = false, maxRows = 30)
    if (rows.size < 2) return true // safer default — most CSVs have headers
    val firstRow = rows.head
    val laterRows = rows.tail
    val width = firstRow.length
    if (width == 0) return true

    val laterTypes: Array[AttributeType] = AttributeTypeUtils.inferSchemaFromRows(
      laterRows.iterator.map(r => r.padTo(width, "").take(width).asInstanceOf[Array[Any]])
    )

    val firstTypes = firstRow.map { v =>
      if (v == null || v.trim.isEmpty) AttributeType.STRING
      else AttributeTypeUtils.inferField(v)
    }

    val typedColumns = laterTypes.zipWithIndex.collect {
      case (t, i)
          if t != AttributeType.STRING && i < firstTypes.length
            && firstTypes(i) == AttributeType.STRING =>
        i
    }
    typedColumns.nonEmpty
  }

  private def parseRows(
      sample: String,
      delimiter: Char,
      headerExtraction: Boolean,
      maxRows: Int
  ): Array[Array[String]] = {
    val format = new CsvFormat()
    format.setDelimiter(delimiter)
    format.setLineSeparator("\n")
    format.setComment('\u0000')
    val settings = new CsvParserSettings()
    settings.setFormat(format)
    settings.setMaxCharsPerColumn(-1)
    settings.setHeaderExtractionEnabled(headerExtraction)
    settings.setNullValue("")
    val parser = new CsvParser(settings)
    val reader = new StringReader(sample)
    try {
      parser.beginParsing(reader)
      val buf = scala.collection.mutable.ArrayBuffer.empty[Array[String]]
      var count = 0
      var row = parser.parseNext()
      while (row != null && count < maxRows) {
        buf += row
        count += 1
        row = parser.parseNext()
      }
      parser.stopParsing()
      buf.toArray
    } finally reader.close()
  }
}
