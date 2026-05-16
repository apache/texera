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

import com.fasterxml.jackson.databind.JsonNode
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.poi.ss.usermodel.{Cell, CellType, DateUtil, Sheet, WorkbookFactory}
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.inferSchemaFromRows
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.operator.source.scan.FolderInputResolver
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.{JSONToMap, objectMapper}

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URI
import java.nio.charset.Charset
import java.nio.file.{Files, StandardOpenOption}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Using

/**
  * Overrides supplied by the user. Each `Some(...)` value short-circuits the corresponding
  * detection step; `None` means "let the inferencer decide".
  */
case class InferenceOverrides(
    format: Option[SmartFileFormat] = None,
    delimiter: Option[Char] = None,
    hasHeader: Option[Boolean] = None,
    sheetName: Option[String] = None,
    flatten: Option[Boolean] = None
)

/**
  * The full inference result. Carries the inferred schema along with the configuration
  * the runtime executor needs to read the file the same way the inferencer did.
  */
case class InferenceResult(
    format: SmartFileFormat,
    schema: Schema,
    csvDelimiter: Option[String] = None,
    csvHasHeader: Option[Boolean] = None,
    sheetName: Option[String] = None,
    availableSheetNames: List[String] = Nil,
    flatten: Option[Boolean] = None,
    isFolder: Boolean = false,
    fileCount: Int = 1
)

/**
  * The single source of truth for "look at this file and decide how to read it."
  * Both the operator descriptor (compile-time schema declaration) and the live
  * preview REST endpoint route through this object so their behavior is identical.
  */
object SmartFileInferencer {

  /** Bytes to read when sniffing format / delimiter / header. */
  private val SampleByteCount = 64 * 1024

  /** Rows to read when inferring types. Matches `ScanSourceOpDesc.INFER_READ_LIMIT`. */
  private val InferRowLimit = 100

  /** Cheap detection that only reads the header bytes. */
  def detect(uri: URI, encoding: Charset): SmartFileFormat = {
    val sample = readSampleBytes(uri)
    FormatDetector.detect(Some(uri.getPath), sample, encoding)
  }

  /** Full inference: format detection + schema. */
  def infer(uri: URI, encoding: Charset, overrides: InferenceOverrides): InferenceResult = {
    val input = FolderInputResolver.resolve(uri)
    if (input.isFolder) {
      inferFolder(uri, input.files.map(_.uri), encoding, overrides)
    } else {
      inferSingle(uri, encoding, overrides)
    }
  }

  private def inferFolder(
      folderUri: URI,
      files: List[URI],
      encoding: Charset,
      overrides: InferenceOverrides
  ): InferenceResult = {
    if (files.isEmpty) {
      throw new IllegalArgumentException(s"Folder $folderUri does not contain any readable files")
    }

    val inferred = files.map(file => inferSingle(file, encoding, overrides))
    val first = inferred.head
    val mismatchedFormat = inferred.find(_.format != first.format)
    if (mismatchedFormat.nonEmpty) {
      throw new IllegalArgumentException(
        s"Folder $folderUri must contain files with the same detected format"
      )
    }

    val expectedSchema = schemaSignature(first.schema)
    val mismatchedSchema = inferred.find(result => schemaSignature(result.schema) != expectedSchema)
    if (mismatchedSchema.nonEmpty) {
      throw new IllegalArgumentException(
        s"Folder $folderUri must contain files with the same inferred schema"
      )
    }

    first.copy(isFolder = true, fileCount = files.size)
  }

  private def inferSingle(uri: URI, encoding: Charset, overrides: InferenceOverrides): InferenceResult = {
    val format = overrides.format
      .filter(_ != SmartFileFormat.AUTO)
      .getOrElse {
        val sample = readSampleBytes(uri)
        FormatDetector.detect(Some(uri.getPath), sample, encoding)
      }

    format match {
      case SmartFileFormat.CSV | SmartFileFormat.TSV => inferCsv(uri, encoding, format, overrides)
      case SmartFileFormat.JSONL                     => inferJsonl(uri, encoding, overrides)
      case SmartFileFormat.JSON                      => inferJson(uri, encoding, overrides)
      case SmartFileFormat.ARROW                     => inferArrow(uri)
      case SmartFileFormat.PARQUET                   => inferParquet(uri)
      case SmartFileFormat.EXCEL                     => inferExcel(uri, overrides)
      case SmartFileFormat.IMAGE                     => inferImage()
      case SmartFileFormat.TEXT                      => inferText()
      case SmartFileFormat.AUTO =>
        throw new IllegalStateException("AUTO should have been resolved before dispatch")
    }
  }

  private def schemaSignature(schema: Schema): List[(String, AttributeType)] =
    schema.getAttributes.map(attribute => attribute.getName -> attribute.getType)

  // ---------------------------------------------------------------------------
  // CSV / TSV
  // ---------------------------------------------------------------------------

  private def inferCsv(
      uri: URI,
      encoding: Charset,
      format: SmartFileFormat,
      overrides: InferenceOverrides
  ): InferenceResult = {
    val sampleText = readSampleText(uri, encoding)
    val preferred = format match {
      case SmartFileFormat.TSV => Some('\t')
      case _                   => Some(',')
    }
    val sniffed = CSVDialectSniffer.sniff(sampleText, preferred)
    val delimiter = overrides.delimiter.getOrElse(sniffed.delimiter)
    val hasHeader = overrides.hasHeader.getOrElse(sniffed.hasHeader)
    val schema = inferCsvSchema(uri, encoding, delimiter, hasHeader)
    InferenceResult(
      format = format,
      schema = schema,
      csvDelimiter = Some(delimiter.toString),
      csvHasHeader = Some(hasHeader)
    )
  }

  private def inferCsvSchema(
      uri: URI,
      encoding: Charset,
      delimiter: Char,
      hasHeader: Boolean
  ): Schema = {
    val csvFormat = new CsvFormat()
    csvFormat.setDelimiter(delimiter)
    csvFormat.setLineSeparator("\n")
    csvFormat.setComment('\u0000')
    val settings = new CsvParserSettings()
    settings.setMaxCharsPerColumn(-1)
    settings.setFormat(csvFormat)
    settings.setHeaderExtractionEnabled(hasHeader)
    settings.setNullValue("")

    val parser = new CsvParser(settings)
    val stream = openStream(uri)
    val reader = new InputStreamReader(stream, encoding)
    try {
      parser.beginParsing(reader)
      val rows = ArrayBuffer.empty[Array[String]]
      var row = parser.parseNext()
      var read = 0
      while (row != null && read < InferRowLimit) {
        rows += row
        read += 1
        row = parser.parseNext()
      }
      parser.stopParsing()
      val attributeTypes = inferSchemaFromRows(rows.iterator.map(_.asInstanceOf[Array[Any]]))
      val header =
        if (hasHeader)
          Option(parser.getContext.headers())
            .getOrElse((1 to attributeTypes.length).map(i => s"column-$i").toArray)
        else
          (1 to attributeTypes.length).map(i => s"column-$i").toArray
      val pairs = header.indices.map { i =>
        val attributeType =
          if (i < attributeTypes.length) attributeTypes(i) else AttributeType.STRING
        (header(i), attributeType)
      }
      pairs.foldLeft(Schema()) { case (s, (name, t)) => s.add(name, t) }
    } finally reader.close()
  }

  // ---------------------------------------------------------------------------
  // JSONL
  // ---------------------------------------------------------------------------

  private def inferJsonl(
      uri: URI,
      encoding: Charset,
      overrides: InferenceOverrides
  ): InferenceResult = {
    val flatten = overrides.flatten.getOrElse(false)
    val stream = openStream(uri)
    val reader = new BufferedReader(new InputStreamReader(stream, encoding))
    try {
      val fieldNames = scala.collection.mutable.LinkedHashSet[String]()
      val rows = ArrayBuffer.empty[Map[String, String]]
      val lines = reader.lines().iterator().asScala.take(InferRowLimit)
      lines.foreach { line =>
        if (line != null && line.trim.nonEmpty) {
          val root: JsonNode = objectMapper.readTree(line)
          if (root.isObject) {
            val fields = JSONToMap(root, flatten = flatten)
            fields.keys.foreach(fieldNames += _)
            rows += fields
          }
        }
      }
      val orderedNames = fieldNames.toList
      val schema = buildJsonSchema(orderedNames, rows.toSeq)
      InferenceResult(
        format = SmartFileFormat.JSONL,
        schema = schema,
        flatten = Some(flatten)
      )
    } finally reader.close()
  }

  // ---------------------------------------------------------------------------
  // JSON (single object or array of objects)
  // ---------------------------------------------------------------------------

  private def inferJson(
      uri: URI,
      encoding: Charset,
      overrides: InferenceOverrides
  ): InferenceResult = {
    val flatten = overrides.flatten.getOrElse(false)
    val stream = openStream(uri)
    val reader = new InputStreamReader(stream, encoding)
    try {
      val root = objectMapper.readTree(reader)
      val rows = ArrayBuffer.empty[Map[String, String]]
      val fieldNames = scala.collection.mutable.LinkedHashSet[String]()

      val objectNodes: Iterator[JsonNode] =
        if (root.isArray) root.elements().asScala
        else if (root.isObject) Iterator.single(root)
        else Iterator.empty

      var count = 0
      while (objectNodes.hasNext && count < InferRowLimit) {
        val node = objectNodes.next()
        if (node.isObject) {
          val fields = JSONToMap(node, flatten = flatten)
          fields.keys.foreach(fieldNames += _)
          rows += fields
          count += 1
        }
      }

      val schema = buildJsonSchema(fieldNames.toList, rows.toSeq)
      InferenceResult(
        format = SmartFileFormat.JSON,
        schema = schema,
        flatten = Some(flatten)
      )
    } finally reader.close()
  }

  private def buildJsonSchema(orderedNames: List[String], rows: Seq[Map[String, String]]): Schema = {
    if (orderedNames.isEmpty) return Schema()
    val attributeTypes = inferSchemaFromRows(rows.iterator.map { row =>
      orderedNames.map(name => row.getOrElse(name, null)).toArray[Any]
    })
    val attrs = orderedNames.indices.map { i =>
      val t =
        if (i < attributeTypes.length) attributeTypes(i) else AttributeType.STRING
      new Attribute(orderedNames(i), t)
    }
    Schema(attrs.toList)
  }

  // ---------------------------------------------------------------------------
  // Arrow
  // ---------------------------------------------------------------------------

  private def inferArrow(uri: URI): InferenceResult = {
    val file = DocumentFactory.openReadonlyDocument(uri).asFile()
    val allocator = new RootAllocator()
    val schema = Using
      .Manager { use =>
        val channel = use(Files.newByteChannel(file.toPath, StandardOpenOption.READ))
        val reader = use(new ArrowFileReader(channel, allocator))
        ArrowUtils.toTexeraSchema(reader.getVectorSchemaRoot.getSchema)
      }
      .getOrElse(throw new RuntimeException(s"Failed to read Arrow schema from $uri"))
    InferenceResult(format = SmartFileFormat.ARROW, schema = schema)
  }

  // ---------------------------------------------------------------------------
  // Parquet
  // ---------------------------------------------------------------------------

  private def inferParquet(uri: URI): InferenceResult = {
    val file = DocumentFactory.openReadonlyDocument(uri).asFile()
    val reader = ParquetUtils.openReader(file)
    try {
      val parquetSchema = reader.getFooter.getFileMetaData.getSchema
      InferenceResult(format = SmartFileFormat.PARQUET, schema = ParquetUtils.toTexeraSchema(parquetSchema))
    } finally reader.close()
  }

  // ---------------------------------------------------------------------------
  // Excel
  // ---------------------------------------------------------------------------

  private def inferExcel(uri: URI, overrides: InferenceOverrides): InferenceResult = {
    val file = DocumentFactory.openReadonlyDocument(uri).asFile()
    val workbook = WorkbookFactory.create(file, null, true) // read-only
    try {
      val sheetNames = (0 until workbook.getNumberOfSheets).map(workbook.getSheetName).toList
      val targetSheet: Sheet = overrides.sheetName
        .flatMap(name => Option(workbook.getSheet(name)))
        .getOrElse(workbook.getSheetAt(0))
      val hasHeader = overrides.hasHeader.getOrElse(true)

      val rowIter = targetSheet.iterator().asScala
      val sampled = rowIter.take(InferRowLimit + 1).toList
      if (sampled.isEmpty) {
        return InferenceResult(
          format = SmartFileFormat.EXCEL,
          schema = Schema(),
          sheetName = Some(targetSheet.getSheetName),
          availableSheetNames = sheetNames,
          csvHasHeader = Some(hasHeader)
        )
      }

      val columnCount = sampled.map(_.getLastCellNum.toInt).max
      val rowsAsStrings: List[Array[String]] = sampled.map { row =>
        (0 until columnCount).map(c => cellToString(row.getCell(c))).toArray
      }

      val header: Array[String] =
        if (hasHeader && rowsAsStrings.nonEmpty)
          rowsAsStrings.head.zipWithIndex.map {
            case (s, i) => if (s == null || s.isEmpty) s"column-${i + 1}" else s
          }
        else (1 to columnCount).map(i => s"column-$i").toArray

      val dataRows = if (hasHeader) rowsAsStrings.drop(1) else rowsAsStrings
      val attributeTypes = inferSchemaFromRows(dataRows.iterator.map(_.asInstanceOf[Array[Any]]))

      val schema = header.indices.foldLeft(Schema()) { (s, i) =>
        val t = if (i < attributeTypes.length) attributeTypes(i) else AttributeType.STRING
        s.add(header(i), t)
      }

      InferenceResult(
        format = SmartFileFormat.EXCEL,
        schema = schema,
        sheetName = Some(targetSheet.getSheetName),
        availableSheetNames = sheetNames,
        csvHasHeader = Some(hasHeader)
      )
    } finally workbook.close()
  }

  private def cellToString(cell: Cell): String = {
    if (cell == null) return null
    cell.getCellType match {
      case CellType.STRING => cell.getStringCellValue
      case CellType.BOOLEAN => String.valueOf(cell.getBooleanCellValue)
      case CellType.NUMERIC =>
        if (DateUtil.isCellDateFormatted(cell))
          new java.sql.Timestamp(cell.getDateCellValue.getTime).toString
        else {
          val d = cell.getNumericCellValue
          if (d == d.toLong.toDouble) d.toLong.toString else d.toString
        }
      case CellType.FORMULA =>
        cellToString(safelyEvaluate(cell))
      case CellType.BLANK | CellType._NONE | CellType.ERROR => null
      case _                                                => null
    }
  }

  private def safelyEvaluate(cell: Cell): Cell = {
    try {
      val evaluator = cell.getSheet.getWorkbook.getCreationHelper.createFormulaEvaluator()
      evaluator.evaluateInCell(cell)
    } catch {
      case _: Throwable => cell
    }
  }

  // ---------------------------------------------------------------------------
  // Plain text
  // ---------------------------------------------------------------------------

  private def inferText(): InferenceResult =
    InferenceResult(
      format = SmartFileFormat.TEXT,
      schema = Schema(List(new Attribute("line", AttributeType.STRING)))
    )

  private def inferImage(): InferenceResult =
    InferenceResult(
      format = SmartFileFormat.IMAGE,
      schema = Schema()
        .add("image", AttributeType.BINARY)
        .add("format", AttributeType.STRING)
        .add("width", AttributeType.INTEGER)
        .add("height", AttributeType.INTEGER)
    )

  // ---------------------------------------------------------------------------
  // I/O helpers
  // ---------------------------------------------------------------------------

  private def openStream(uri: URI): InputStream =
    DocumentFactory.openReadonlyDocument(uri).asInputStream()

  private def readSampleBytes(uri: URI): Array[Byte] = {
    val stream = openStream(uri)
    try {
      val buffer = new Array[Byte](SampleByteCount)
      var totalRead = 0
      var lastRead = 0
      while (totalRead < buffer.length && {
               lastRead = stream.read(buffer, totalRead, buffer.length - totalRead); lastRead
             } > 0) {
        totalRead += lastRead
      }
      if (totalRead == buffer.length) buffer else buffer.take(totalRead)
    } finally stream.close()
  }

  private def readSampleText(uri: URI, charset: Charset): String =
    new String(readSampleBytes(uri), charset)
}
