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
    // Under flattening the schema names a nested value for the column the
    // flattening is about to build, and read_json is asked about that name
    // while the file still holds the object around it. It finds no such column,
    // converts nothing, and the value reaches the plan as text, where a sort
    // puts a 2025 date before a 2024 one. Those columns are converted once the
    // frame that holds them exists, below.
    if (!flatten) readArgs += s"convert_dates=[${dateColumns.mkString(", ")}]"

    val readExpr = s"pd.read_json(${readArgs.mkString(", ")})"
    // json_normalize opens a nested object and leaves a nested array whole, so
    // an array arrived as one column holding a list where the executor had
    // already given each element a column of its own. The flattening the
    // executor does is done here instead, and json_normalize is left the frame
    // to build out of records that are flat by then.
    val baseExpr =
      if (flatten)
        s"pd.json_normalize([_texera_json_flatten(_r) for _r in $readExpr.to_dict('records')])"
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
      // Flattened or not, a column's name is a key of the record the lookup
      // below reads, because a flattened record is flattened first.
      val parsed = if (flatten) "_texera_json_flatten(json.loads(_l))" else "json.loads(_l)"
      lines += s"_records = [$parsed for _l in $taken]"
      longColumns.foreach { name =>
        val nameLit = pyStringLiteral(name)
        lines += s"""out1df[$nameLit] = pd.array([_r.get($nameLit) for _r in _records], dtype="Int64")"""
      }
    }

    if (flatten) {
      // A format is inferred for each value on its own, the way this operator's
      // own parser reads each value on its own, so a column whose lines wrote
      // the same instant two ways still converts whole.
      dateColumns.foreach { nameLit =>
        lines += s"""out1df[$nameLit] = pd.to_datetime(out1df[$nameLit], format="mixed")"""
      }
    }

    // pandas has no plain boolean that carries a hole, so a record missing the
    // key widens the column to floats, and a later cast to text read 1.0 and 0.0
    // where the executor has true and false. The nullable boolean carries both
    // values and the hole. A column with no hole arrives as bool already and is
    // left alone.
    val booleanColumns: Seq[String] =
      Try(sourceSchema()).toOption.toSeq.flatMap(
        _.getAttributes
          .filter(_.getType == AttributeType.BOOLEAN)
          .map(a => pyStringLiteral(a.getName))
      )
    booleanColumns.foreach { nameLit =>
      lines += s"""if out1df[$nameLit].dtype == "float64":"""
      lines += s"""    out1df[$nameLit] = out1df[$nameLit].astype("boolean")"""
    }

    // A JSONL file states no column order, so the schema this operator infers
    // sorts the names it found and the rows the workflow sees follow that
    // order. read_json keeps the order the first record happened to use, which
    // is the same columns in a different order, and column order is what a
    // positional read downstream and a file export both go by.
    lines += "out1df = out1df[sorted(out1df.columns)]"

    lines.mkString("\n")
  }

  override def standaloneHelpers(): Seq[String] =
    if (flatten) Seq(JSONLScanSourceOpDesc.JsonFlatten) else Seq.empty

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
    // A window of no rows is still a window on this file, and the file's columns
    // do not depend on how many of its rows were asked for. Reading the sample
    // through the limit left a Limit of 0 nothing to infer from, and the operator
    // declared a schema of no columns at all.
    val endOffset =
      startOffset + limit.filter(_ > 0).getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT)
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
    * One record flattened the way `JSONUtils.JSONToMap` flattens it, so the
    * names the two paths give a nested value are the same names.
    *
    * A nested object joins its key to its parent's with a dot, and an array
    * element takes its parent's name followed by its position counted from one:
    * `{"items": [{"id": 1}, {"id": 2}]}` is `items1.id` and `items2.id` on both
    * sides. A value nested no deeper keeps its own key.
    */
  val JsonFlatten: String =
    """def _texera_json_flatten(record):
      |    flat = {}
      |    stack = [(record, "")]
      |    while stack:
      |        node, parent = stack.pop()
      |        if isinstance(node, dict):
      |            for key, child in node.items():
      |                path = parent + "." + key if parent else key
      |                if isinstance(child, (dict, list)):
      |                    stack.append((child, path))
      |                else:
      |                    flat[path] = child
      |        elif isinstance(node, list):
      |            for index, child in enumerate(node):
      |                stack.append((child, parent + str(index + 1)))
      |        elif parent:
      |            flat[parent] = node
      |    return flat""".stripMargin
}
