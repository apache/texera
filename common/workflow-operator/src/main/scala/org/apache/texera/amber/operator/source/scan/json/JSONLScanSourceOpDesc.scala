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

    // The executor reads every value as the text Jackson gives it, so a column
    // this operator types STRING holds that text whatever the JSON held: 35.0 is
    // "35.0", true is "true", and a null is "null", which is also why one null
    // makes its column STRING. read_json keeps the values typed and the null a
    // null. These columns are rebuilt from the lines the way LONG ones are.
    val stringColumns: Seq[String] =
      Try(sourceSchema()).toOption.toSeq.flatMap(
        _.getAttributes.filter(_.getType == AttributeType.STRING).map(_.getName)
      )

    // The lines are read into the script whenever something below needs them,
    // and read_json is then fed from those rather than from the file, so the
    // file is opened once either way.
    val readsLines = windowed || longColumns.nonEmpty || stringColumns.nonEmpty
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
    Try(sourceSchema()).toOption.foreach { schema =>
      lines += StandaloneCodeGenerator.typeAnEmptyRead("out1df", schema)
    }

    if (longColumns.nonEmpty || stringColumns.nonEmpty) {
      // Flattened or not, a column's name is a key of the record the lookup
      // below reads, because a flattened record is flattened first.
      val parsed = if (flatten) "_texera_json_flatten(json.loads(_l))" else "json.loads(_l)"
      lines += s"_records = [$parsed for _l in $taken]"
      longColumns.foreach { name =>
        val nameLit = pyStringLiteral(name)
        lines += s"""out1df[$nameLit] = pd.array([_r.get($nameLit) for _r in _records], dtype="Int64")"""
      }
      // A missing key is the executor's null. A key holding null is its "null".
      stringColumns.foreach { name =>
        val nameLit = pyStringLiteral(name)
        lines += s"""out1df[$nameLit] = pd.Series([_texera_json_text(_r[$nameLit]) if $nameLit in _r else None for _r in _records], index=out1df.index, dtype="object")"""
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

    // read_json infers each column's type from its values, which is not how this
    // operator types it: a DOUBLE whose values are all whole, 3.0 among them, is
    // read as int64, and an INTEGER missing a key is widened to float. Both reach
    // the next operator as the other type, where 3 and 3.0 print differently. The
    // schema's type is asked for, the INTEGER in the nullable width that keeps a
    // hole.
    Try(sourceSchema()).toOption.toSeq.flatMap(_.getAttributes).foreach { attribute =>
      val nameLit = pyStringLiteral(attribute.getName)
      attribute.getType match {
        case AttributeType.DOUBLE =>
          lines += s"""out1df[$nameLit] = out1df[$nameLit].astype("float64")"""
        case AttributeType.INTEGER =>
          lines += s"""out1df[$nameLit] = out1df[$nameLit].astype("Int32")"""
        case _ =>
      }
    }

    // A JSONL file states no column order, so the schema this operator infers
    // sorts the names it found and the rows the workflow sees follow that
    // order. read_json keeps the order the first record happened to use, which
    // is the same columns in a different order, and column order is what a
    // positional read downstream and a file export both go by.
    //
    // Unflattened, the executor also passes over a nested object or array, so
    // the key is no column of its own, where read_json keeps it as one holding
    // the dict or the list. The schema names the columns the executor has.
    val columns =
      Try(sourceSchema()).toOption.map(_.getAttributes.map(a => pyStringLiteral(a.getName)))
    lines += columns.fold("out1df = out1df[sorted(out1df.columns)]")(names =>
      s"out1df = out1df[[${names.mkString(", ")}]]"
    )

    lines.mkString("\n")
  }

  override def standaloneHelpers(): Seq[String] =
    (if (flatten) Seq(JSONLScanSourceOpDesc.JsonFlatten) else Seq.empty) ++
      (if (columnCount(AttributeType.STRING) > 0) Seq(JSONLScanSourceOpDesc.JsonText)
       else Seq.empty)

  override def standaloneImports(): Seq[String] = {
    val windowed = offset.exists(_ > 0) || limit.isDefined
    val longs = columnCount(AttributeType.LONG)
    val strings = columnCount(AttributeType.STRING)
    (if (windowed || longs > 0 || strings > 0) Seq("import io") else Seq.empty) ++
      (if (longs > 0 || strings > 0) Seq("import json") else Seq.empty) ++
      (if (strings > 0) Seq("import decimal") else Seq.empty)
  }

  /** How many columns the schema gives this type, or none when it cannot be read. */
  private def columnCount(attributeType: AttributeType): Int =
    Try(sourceSchema()).toOption
      .map(_.getAttributes.count(_.getType == attributeType))
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

  /**
    * One JSON value as the text Jackson's `asText` gives it, which is what the
    * executor reads every value as: `null` for a null, `true` and `false` in
    * lower case, and a float the way Java prints a double, in E notation outside
    * 10^-3 to 10^7. An object or an array is skipped by the executor, so it is
    * no value here either.
    */
  val JsonText: String =
    """def _texera_json_text(value):
      |    if value is None:
      |        return "null"
      |    if isinstance(value, bool):
      |        return "true" if value else "false"
      |    if isinstance(value, int):
      |        return str(value)
      |    if isinstance(value, float):
      |        if value != value:
      |            return "NaN"
      |        if value in (float("inf"), float("-inf")):
      |            return "Infinity" if value > 0 else "-Infinity"
      |        if value == 0 or 1e-3 <= abs(value) < 1e7:
      |            return repr(value)
      |        sign, digits, exponent = decimal.Decimal(repr(value)).as_tuple()
      |        power = len(digits) + exponent - 1
      |        kept = "".join(map(str, digits)).rstrip("0") or "0"
      |        return ("-" if sign else "") + kept[0] + "." + (kept[1:] or "0") + "E" + str(power)
      |    if isinstance(value, str):
      |        return value
      |    return None""".stripMargin
}
