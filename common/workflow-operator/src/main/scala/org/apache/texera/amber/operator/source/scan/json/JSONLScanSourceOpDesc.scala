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

  override def generateStandaloneCode(): String = {
    val basename = sourceBasename(fileName.getOrElse(""))
    val enc = fileEncoding.toString.replace("_", "-").toLowerCase

    val readArgs = scala.collection.mutable.ArrayBuffer[String]()
    readArgs += pyStringLiteral(basename)
    readArgs += "lines=True"
    readArgs += s"""encoding=${pyStringLiteral(enc)}"""

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

    if (offset.isEmpty) limit.foreach(l => readArgs += s"nrows=$l")

    val readExpr = s"pd.read_json(${readArgs.mkString(", ")})"
    val baseExpr =
      if (flatten) s"pd.json_normalize($readExpr.to_dict('records'))"
      else readExpr

    val lines = scala.collection.mutable.ArrayBuffer[String]()
    lines += s"out1df = $baseExpr"

    (offset, limit) match {
      case (Some(o), Some(l)) =>
        lines += s"out1df = out1df.iloc[$o:${o + l}].reset_index(drop=True)"
      case (Some(o), None) =>
        lines += s"out1df = out1df.iloc[$o:].reset_index(drop=True)"
      case _ =>
    }

    lines.mkString("\n")
  }

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
