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
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.poi.ss.usermodel.{Workbook, WorkbookFactory}
import org.apache.texera.amber.core.executor.SourceOperatorExecutor
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.{AttributeTypeUtils, Schema, TupleLike}
import org.apache.texera.amber.operator.source.scan.FolderInputResolver
import org.apache.texera.amber.util.{ArrowUtils, ImageFormatUtils, JSONUtils}
import org.apache.texera.amber.util.JSONUtils.{JSONToMap, objectMapper}

import java.io.{BufferedReader, ByteArrayInputStream, InputStreamReader}
import java.net.URI
import java.nio.file.{Files, StandardOpenOption}
import javax.imageio.ImageIO
import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters._

class SmartFileSourceOpExec(descString: String) extends SourceOperatorExecutor {

  private val desc: SmartFileSourceOpDesc =
    objectMapper.readValue(descString, classOf[SmartFileSourceOpDesc])

  private var inference: InferenceResult = _
  private var schema: Schema = _
  private val resources = scala.collection.mutable.ListBuffer.empty[AutoCloseable]
  private var tupleSource: Iterator[TupleLike] = Iterator.empty

  private def closeableOf(fn: () => Unit): AutoCloseable =
    new AutoCloseable { override def close(): Unit = fn() }

  override def open(): Unit = {
    inference = desc.runInference()
    schema = desc.withOptionalSourceFile(inference.schema)
    tupleSource = openReader()
  }

  override def produceTuple(): Iterator[TupleLike] = {
    var it = tupleSource.drop(desc.offset.getOrElse(0))
    if (desc.limit.isDefined) it = it.take(desc.limit.get)
    it
  }

  override def close(): Unit = {
    resources.foreach { c =>
      try c.close()
      catch { case _: Throwable => /* swallow on shutdown */ }
    }
    resources.clear()
  }

  // ---------------------------------------------------------------------------
  // Per-format readers
  // ---------------------------------------------------------------------------

  private def openReader(): Iterator[TupleLike] = {
    val input = FolderInputResolver.resolve(new URI(desc.fileName.get))
    input.files.iterator.flatMap { file =>
      val rows = inference.format match {
        case SmartFileFormat.CSV | SmartFileFormat.TSV => csvReader(file.uri)
        case SmartFileFormat.JSONL                     => jsonlReader(file.uri)
        case SmartFileFormat.JSON                      => jsonReader(file.uri)
        case SmartFileFormat.ARROW                     => arrowReader(file.uri)
        case SmartFileFormat.PARQUET                   => parquetReader(file.uri)
        case SmartFileFormat.EXCEL                     => excelReader(file.uri)
        case SmartFileFormat.IMAGE                     => imageReader(file.uri)
        case SmartFileFormat.TEXT                      => textReader(file.uri)
        case SmartFileFormat.AUTO =>
          throw new IllegalStateException("AUTO should have been resolved by inferencer")
      }
      if (desc.includeSourceFile) rows.map(appendSourceFile(_, file.displayName)) else rows
    }
  }

  private def appendSourceFile(tuple: TupleLike, displayName: String): TupleLike =
    TupleLike(tuple.getFields :+ displayName)

  // CSV / TSV ----------------------------------------------------------------

  private def csvReader(uri: URI): Iterator[TupleLike] = {
    val delimiter = inference.csvDelimiter
      .flatMap(_.headOption)
      .getOrElse(if (inference.format == SmartFileFormat.TSV) '\t' else ',')
    val hasHeader = inference.csvHasHeader.getOrElse(true)
    val stream = DocumentFactory.openReadonlyDocument(uri).asInputStream()
    val reader = new InputStreamReader(stream, desc.fileEncoding.getCharset)
    resources += reader

    val format = new CsvFormat()
    format.setDelimiter(delimiter)
    format.setLineSeparator("\n")
    format.setComment('\u0000')
    val settings = new CsvParserSettings()
    settings.setMaxCharsPerColumn(-1)
    settings.setFormat(format)
    settings.setHeaderExtractionEnabled(hasHeader)
    settings.setNullValue("")
    val parser = new CsvParser(settings)
    parser.beginParsing(reader)
    resources += closeableOf(() => parser.stopParsing())

    new Iterator[TupleLike] {
      private var nextRow: Array[String] = parser.parseNext()
      override def hasNext: Boolean = nextRow != null
      override def next(): TupleLike = {
        val row = nextRow
        nextRow = parser.parseNext()
        try {
          TupleLike(
            ArraySeq.unsafeWrapArray(
              AttributeTypeUtils.parseFields(row.asInstanceOf[Array[Any]], schema)
            ): _*
          )
        } catch {
          case _: Throwable => null
        }
      }
    }.filter(_ != null)
  }

  // JSONL --------------------------------------------------------------------

  private def jsonlReader(uri: URI): Iterator[TupleLike] = {
    val stream = DocumentFactory.openReadonlyDocument(uri).asInputStream()
    val br = new BufferedReader(new InputStreamReader(stream, desc.fileEncoding.getCharset))
    resources += br
    val flatten = inference.flatten.getOrElse(false)
    val names = schema.getAttributeNames

    br.lines().iterator().asScala
      .flatMap { line =>
        if (line == null || line.trim.isEmpty) None
        else {
          try {
            val node = objectMapper.readTree(line)
            if (!node.isObject) None
            else Some(buildTupleFromJsonObject(node, names, flatten))
          } catch {
            case _: Throwable => None
          }
        }
      }
  }

  // JSON ---------------------------------------------------------------------

  private def jsonReader(uri: URI): Iterator[TupleLike] = {
    val stream = DocumentFactory.openReadonlyDocument(uri).asInputStream()
    val reader = new InputStreamReader(stream, desc.fileEncoding.getCharset)
    resources += reader
    val flatten = inference.flatten.getOrElse(false)
    val names = schema.getAttributeNames

    val root = objectMapper.readTree(reader)
    val nodes: Iterator[JsonNode] =
      if (root.isArray) root.elements().asScala
      else if (root.isObject) Iterator.single(root)
      else Iterator.empty

    nodes.flatMap { node =>
      if (!node.isObject) None
      else
        try Some(buildTupleFromJsonObject(node, names, flatten))
        catch { case _: Throwable => None }
    }
  }

  private def buildTupleFromJsonObject(
      node: JsonNode,
      names: List[String],
      flatten: Boolean
  ): TupleLike = {
    val fields = JSONToMap(node, flatten).withDefaultValue(null)
    val parsed = names.map { name =>
      AttributeTypeUtils.parseField(fields(name), schema.getAttribute(name).getType)
    }
    TupleLike(parsed: _*)
  }

  // Arrow --------------------------------------------------------------------

  private def arrowReader(uri: URI): Iterator[TupleLike] = {
    val file = DocumentFactory.openReadonlyDocument(uri).asFile()
    val allocator = new RootAllocator()
    val channel = Files.newByteChannel(file.toPath, StandardOpenOption.READ)
    val arrowReader = new ArrowFileReader(channel, allocator)
    val vectorRoot: VectorSchemaRoot = arrowReader.getVectorSchemaRoot
    resources += vectorRoot
    resources += arrowReader
    resources += allocator
    resources += closeableOf(() => channel.close())

    new Iterator[TupleLike] {
      private var idx = 0
      override def hasNext: Boolean = {
        if (vectorRoot.getRowCount > idx) true
        else if (arrowReader.loadNextBatch()) { idx = 0; vectorRoot.getRowCount > 0 }
        else false
      }
      override def next(): TupleLike = {
        val tuple = ArrowUtils.getTexeraTuple(idx, vectorRoot)
        idx += 1
        tuple
      }
    }
  }

  // Parquet ------------------------------------------------------------------

  private def parquetReader(uri: URI): Iterator[TupleLike] = {
    val file = DocumentFactory.openReadonlyDocument(uri).asFile()
    val handle = ParquetUtils.openRecords(file)
    resources += closeableOf(() => handle.close())

    val parquetSchema = handle.schema
    val attributeNames = schema.getAttributeNames
    val parquetIndex: Map[String, Int] =
      (0 until parquetSchema.getFieldCount).map(i => parquetSchema.getType(i).getName -> i).toMap

    handle.records.map { group =>
      val values = attributeNames.map { name =>
        parquetIndex.get(name) match {
          case Some(i) =>
            val raw = ParquetUtils.readField(group, i, parquetSchema)
            try AttributeTypeUtils.parseField(raw, schema.getAttribute(name).getType)
            catch { case _: Throwable => raw }
          case None => null
        }
      }
      TupleLike(values: _*)
    }
  }

  // Excel --------------------------------------------------------------------

  private def excelReader(uri: URI): Iterator[TupleLike] = {
    val file = DocumentFactory.openReadonlyDocument(uri).asFile()
    val workbook: Workbook = WorkbookFactory.create(file, null, true)
    resources += workbook
    val sheet = inference.sheetName
      .flatMap(name => Option(workbook.getSheet(name)))
      .getOrElse(workbook.getSheetAt(0))
    val hasHeader = inference.csvHasHeader.getOrElse(true)
    val attributeNames = schema.getAttributeNames

    val rowIter = sheet.iterator().asScala
    val dataRows = if (hasHeader && rowIter.hasNext) { rowIter.next(); rowIter } else rowIter

    dataRows.map { row =>
      val values = attributeNames.indices.map { i =>
        val cell = row.getCell(i)
        val raw = readExcelCell(cell)
        try AttributeTypeUtils.parseField(raw, schema.getAttributes(i).getType)
        catch { case _: Throwable => raw }
      }
      TupleLike(values: _*)
    }
  }

  private def readExcelCell(cell: org.apache.poi.ss.usermodel.Cell): Any = {
    import org.apache.poi.ss.usermodel.{CellType, DateUtil}
    if (cell == null) return null
    cell.getCellType match {
      case CellType.STRING  => cell.getStringCellValue
      case CellType.BOOLEAN => java.lang.Boolean.valueOf(cell.getBooleanCellValue)
      case CellType.NUMERIC =>
        if (DateUtil.isCellDateFormatted(cell))
          new java.sql.Timestamp(cell.getDateCellValue.getTime)
        else {
          val d = cell.getNumericCellValue
          if (d == d.toLong.toDouble) java.lang.Long.valueOf(d.toLong)
          else java.lang.Double.valueOf(d)
        }
      case CellType.FORMULA =>
        try {
          val evaluator = cell.getSheet.getWorkbook.getCreationHelper.createFormulaEvaluator()
          val evaluated = evaluator.evaluate(cell)
          evaluated.getCellType match {
            case CellType.STRING  => evaluated.getStringValue
            case CellType.BOOLEAN => java.lang.Boolean.valueOf(evaluated.getBooleanValue)
            case CellType.NUMERIC =>
              val d = evaluated.getNumberValue
              if (d == d.toLong.toDouble) java.lang.Long.valueOf(d.toLong)
              else java.lang.Double.valueOf(d)
            case _ => null
          }
        } catch {
          case _: Throwable => null
        }
      case _ => null
    }
  }

  // Images -------------------------------------------------------------------

  private def imageReader(uri: URI): Iterator[TupleLike] = {
    val stream = DocumentFactory.openReadonlyDocument(uri).asInputStream()
    val bytes =
      try stream.readAllBytes()
      finally stream.close()
    val image = ImageIO.read(new ByteArrayInputStream(bytes))
    val format = ImageFormatUtils
      .detectFormat(bytes)
      .orElse(ImageFormatUtils.extensionFormat(uri.getPath))
      .getOrElse("unknown")
    val width = Option(image).map(image => Int.box(image.getWidth)).orNull
    val height = Option(image).map(image => Int.box(image.getHeight)).orNull
    Iterator.single(TupleLike(bytes, format, width, height))
  }

  // Plain text ---------------------------------------------------------------

  private def textReader(uri: URI): Iterator[TupleLike] = {
    val stream = DocumentFactory.openReadonlyDocument(uri).asInputStream()
    val br = new BufferedReader(new InputStreamReader(stream, desc.fileEncoding.getCharset))
    resources += br
    br.lines().iterator().asScala.map(line => TupleLike(line))
  }

  // Keep the JSONUtils import live (used transitively by JSONToMap/objectMapper above).
  locally(JSONUtils)
}
