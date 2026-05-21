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

package org.apache.texera.amber.operator.source.scan.csvOld

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.inferSchemaFromRows
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.IOException
import java.net.URI
import java.nio.file.Paths

class CSVOldScanSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("delimiter to separate each line into fields")
  var customDelimiter: Option[String] = Some(",")

  @JsonProperty(defaultValue = "true")
  @JsonSchemaTitle("Header")
  @JsonPropertyDescription("whether the CSV file contains a header line")
  var hasHeader: Boolean = true

  fileTypeName = Option("CSVOld")

  @throws[IOException]
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    // fill in default values
    if (customDelimiter.get.isEmpty) {
      customDelimiter = Option(",")
    }
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.source.scan.csvOld.CSVOldScanSourceOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
  }

  override def generateStandaloneCode(): String = {
    val rawPath = fileName.getOrElse("")
    val basename = Paths.get(new URI(rawPath).getPath).getFileName.toString
    val sep = customDelimiter.getOrElse(",")
    val encoding = fileEncoding.toString.replace("_", "-").toLowerCase
    val headerArg = if (hasHeader) "0" else "None"

    val args = scala.collection.mutable.ArrayBuffer[String]()
    args += s"""filepath_or_buffer="$basename""""
    args += s"""sep="$sep""""
    args += s"""encoding="$encoding""""
    args += s"header=$headerArg"

    offset.foreach { o =>
      if (hasHeader) args += s"skiprows=range(1, ${o + 1})"
      else args += s"skiprows=$o"
    }
    limit.foreach(l => args += s"nrows=$l")

    val readCall = s"out1df = pd.read_csv(${args.mkString(", ")})"

    if (hasHeader) readCall
    else
      s"""$readCall
         |out1df.columns = [f"column-{i + 1}" for i in range(len(out1df.columns))]""".stripMargin
  }

  override def sourceSchema(): Schema = {
    if (customDelimiter.isEmpty || !fileResolved()) {
      return null
    }
    // infer schema from the first few lines of the file
    val file = DocumentFactory.openReadonlyDocument(new URI(fileName.get)).asFile()
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = customDelimiter.get.charAt(0)
    }
    var reader: CSVReader =
      CSVReader.open(file, fileEncoding.getCharset.name())(CustomFormat)
    val firstRow: Array[String] = reader.iterator.next().toArray
    reader.close()

    // reopen the file to read from the beginning
    reader = CSVReader.open(file, fileEncoding.getCharset.name())(CustomFormat)

    val startOffset = offset.getOrElse(0) + (if (hasHeader) 1 else 0)
    val endOffset =
      startOffset + limit.getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT)
    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
      reader.iterator
        .slice(startOffset, endOffset)
        .map(seq => seq.toArray)
    )

    reader.close()

    // build schema based on inferred AttributeTypes
    Schema().add(firstRow.indices.map { i =>
      new Attribute(
        if (hasHeader) firstRow(i) else s"column-${i + 1}",
        attributeTypeList(i)
      )
    })

  }

}
