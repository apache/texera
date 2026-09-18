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

package org.apache.texera.amber.operator.source.scan.json

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.JsonNode
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.inferSchemaFromRows
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.StandaloneCodeGenerator.SourceFilePlaceholder
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.{JSONToMap, objectMapper}

import java.io._
import java.net.URI
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.jdk.CollectionConverters.IteratorHasAsScala

class JSONLScanSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  @JsonProperty(required = true, defaultValue = "false")
  @JsonPropertyDescription("flatten nested objects and arrays")
  var flatten: Boolean = false

  fileTypeName = Option("JSONL")

  override def standaloneSourcePath(): Option[String] = fileName

  override def generateStandaloneCode(): String = {
    val enc = fileEncoding.toString.replace("_", "-").toLowerCase

    // The executor drops and takes on the RAW lines, before any of them is
    // parsed, so a line outside the window is never read as JSON and an
    // unparseable one there costs nothing. Reading the whole file first and
    // slicing the frame would end the export on a line the workflow skipped.
    val windowed = offset.exists(_ > 0) || limit.isDefined

    // read_json parses a JSON number into a float before any dtype it is handed
    // can apply, so a LONG past 2^53 is already rounded by the time the column
    // exists: 9007199254740993 arrives as ...992, a value the file never held.
    // Reading the lines a second time with Python's own parser, which keeps an
    // integer exact, is the only way to put the column back.
    val longColumns: Seq[String] =
      Try(sourceSchema()).toOption.toSeq.flatMap(
        _.getAttributes.filter(_.getType == AttributeType.LONG).map(_.getName)
      )

    // The lines are read into the script whenever something below needs them,
    // and read_json is then fed from those rather than from the file, so the
    // file is opened once either way.
    val readsLines = windowed || longColumns.nonEmpty
    val dropped = offset.filter(_ > 0).fold("_lines")(o => s"_lines[$o:]")
    val taken = limit.fold(dropped)(l => s"$dropped[:${l.max(0)}]")

    val readArgs = scala.collection.mutable.ArrayBuffer[String]()
    readArgs += (if (readsLines) s"""io.StringIO("".join($taken))""" else SourceFilePlaceholder)
    readArgs += "lines=True"
    // Text already decoded by the open above carries no encoding of its own.
    if (!readsLines) readArgs += s"""encoding=${pyStringLiteral(enc)}"""

    // JSON has no timestamp of its own, so both readers infer from the text and
    // do not infer alike: the schema below tries TIMESTAMP and parses what it
    // can, while pd.read_json guesses from the COLUMN NAME (anything ending
    // "_at" or "_time", anything called "date") and leaves the rest as text.
    // Naming the columns this operator decided were timestamps settles both
    // halves — the ones it misses and the ones it would have taken on its own.
    // An unreadable schema leaves the argument off rather than failing the
    // export.
    val dateColumns: Seq[String] =
      Try(sourceSchema()).toOption.toSeq.flatMap(
        _.getAttributes
          .filter(_.getType == AttributeType.TIMESTAMP)
          .map(a => pyStringLiteral(a.getName))
      )
    readArgs += s"convert_dates=[${dateColumns.mkString(", ")}]"

    val readExpr = s"pd.read_json(${readArgs.mkString(", ")})"
    val baseExpr =
      if (flatten) s"pd.json_normalize($readExpr.to_dict('records'))"
      else readExpr

    val lines = scala.collection.mutable.ArrayBuffer[String]()
    if (readsLines) {
      lines += s"""with open($SourceFilePlaceholder, "r", encoding=${pyStringLiteral(
        enc
      )}) as _f:"""
      lines += "    _lines = _f.readlines()"
    }
    lines += s"out1df = $baseExpr"

    if (longColumns.nonEmpty) {
      lines += s"_records = [json.loads(_l) for _l in $taken]"
      longColumns.foreach { name =>
        val nameLit = pyStringLiteral(name)
        // Flattening joins a nested key to its parent with a dot, so the schema's
        // name is a path into the record rather than a key of it. Without
        // flattening it is a key, and a key is free to hold a dot of its own.
        val valueExpr =
          if (flatten) s"_texera_json_value(_r, $nameLit)" else s"_r.get($nameLit)"
        lines += s"""out1df[$nameLit] = pd.array([$valueExpr for _r in _records], dtype="Int64")"""
      }
    }

    lines.mkString("\n")
  }

  override def standaloneHelpers(): Seq[String] =
    if (flatten && longColumnCount > 0) Seq(JSONLScanSourceOpDesc.JsonValueAtPath) else Seq.empty

  override def standaloneImports(): Seq[String] = {
    val windowed = offset.exists(_ > 0) || limit.isDefined
    val longs = longColumnCount
    (if (windowed || longs > 0) Seq("import io") else Seq.empty) ++
      (if (longs > 0) Seq("import json") else Seq.empty)
  }

  /** How many columns the schema types LONG, or none when it cannot be read. */
  private def longColumnCount: Int =
    Try(sourceSchema()).toOption
      .map(_.getAttributes.count(_.getType == AttributeType.LONG))
      .getOrElse(0)

  @throws[IOException]
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {

    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.source.scan.json.JSONLScanSourceOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withParallelizable(true)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
  }

  override def sourceSchema(): Schema = {
    require(
      fileResolved(),
      "No file selected. Please select a valid .jsonl file from the 'File' dropdown in the right panel."
    )

    val uri = new URI(fileName.get)
    if (uri.getScheme == "file") {
      require(
        new java.io.File(uri).isFile,
        "The selected item is a folder or does not exist. Please select an actual .jsonl file from the 'File' dropdown."
      )
    }
    val stream = DocumentFactory.openReadonlyDocument(uri).asInputStream()
    val reader = new BufferedReader(new InputStreamReader(stream, fileEncoding.getCharset))
    var fieldNames = Set[String]()

    val allFields: ArrayBuffer[Map[String, String]] = ArrayBuffer()

    val startOffset = offset.getOrElse(0)
    val endOffset =
      startOffset + limit.getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT)
    reader
      .lines()
      .iterator()
      .asScala
      .slice(startOffset, endOffset)
      .foreach(line => {
        val root: JsonNode = objectMapper.readTree(line)
        if (root.isObject) {
          val fields: Map[String, String] = JSONToMap(root, flatten = flatten)
          fieldNames = fieldNames.++(fields.keySet)
          allFields += fields
        }
      })

    val sortedFieldNames = fieldNames.toList.sorted
    reader.close()

    val attributeTypes = inferSchemaFromRows(allFields.iterator.map(fields => {
      val result = ArrayBuffer[Object]()
      for (fieldName <- sortedFieldNames) {
        if (fields.contains(fieldName)) {
          result += fields(fieldName)
        } else {
          result += null
        }
      }
      result.toArray
    }))

    Schema().add(sortedFieldNames.indices.map { i =>
      new Attribute(sortedFieldNames(i), attributeTypes(i))
    })

  }
}

object JSONLScanSourceOpDesc {

  /**
    * A flattened column's name read back out of the record it came from.
    *
    * Only the columns an exact re-read has to rebuild need this, and only when
    * flattening is on: the name is then a dotted path rather than a key. A path
    * the record does not hold reads as nothing, which is what the flattened
    * frame carries there.
    */
  val JsonValueAtPath: String =
    """def _texera_json_value(record, path):
      |    for part in path.split("."):
      |        if not isinstance(record, dict) or part not in record:
      |            return None
      |        record = record[part]
      |    return record""".stripMargin
}
