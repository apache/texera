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

package org.apache.texera.amber.operator.source.scan.csv

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.inferSchemaFromRows
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.IOException
import java.net.URI
import java.nio.file.Paths

class ParallelCSVScanSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  // Almost anything — a leading newline is an accepted path divergence; see
  // CSVScanSourceOpDesc.
  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("delimiter to separate each line into fields")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @JsonSchemaInject(json = """
{
  "examples": [","]
}
""")
  var customDelimiter: Option[String] = None

  @JsonProperty(defaultValue = "true")
  @JsonSchemaTitle("Header")
  @JsonPropertyDescription("whether the CSV file contains a header line")
  var hasHeader: Boolean = true

  fileTypeName = Option("CSV")

  @throws[IOException]
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    // fill in default values
    if (customDelimiter.forall(_.isEmpty)) {
      customDelimiter = Option(",")
    }

    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.source.scan.csv.ParallelCSVScanSourceOpExec",
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

  override def generateStandaloneCode(): String = {
    val rawPath = fileName.getOrElse("")
    val basename = Paths.get(new URI(rawPath).getPath).getFileName.toString
    // First character, empty means comma — the same resolution the reader below does —
    // and escaped, so every value the field accepts survives being spliced into Python.
    // See CSVScanSourceOpDesc for what handing pandas the raw value did.
    val sep = customDelimiter.filter(_.nonEmpty).getOrElse(",").charAt(0).toString
    val encoding = fileEncoding.toString.replace("_", "-").toLowerCase
    val headerArg = if (hasHeader) "0" else "None"

    val args = scala.collection.mutable.ArrayBuffer[String]()
    args += s"""filepath_or_buffer=${pyStringLiteral(basename)}"""
    args += s"sep=${pyStringLiteral(sep)}"
    args += s"""encoding=${pyStringLiteral(encoding)}"""
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
    val delimiterChar = customDelimiter.filter(_.nonEmpty).getOrElse(",").charAt(0)
    require(
      fileResolved(),
      "No file selected. Please select a valid .csv file from the 'File' dropdown in the right panel."
    )

    val uri = new URI(fileName.get)
    if (uri.getScheme == "file") {
      require(
        new java.io.File(uri).isFile,
        "The selected item is a folder or does not exist. Please select an actual .csv file from the 'File' dropdown."
      )
    }
    val file = DocumentFactory.openReadonlyDocument(uri).asFile()
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = delimiterChar

    }
    var reader: CSVReader = CSVReader.open(file)(CustomFormat)
    val firstRow: Array[String] = reader.iterator.next().toArray
    reader.close()

    // reopen the file to read from the beginning
    reader = CSVReader.open(file.toPath.toString)(CustomFormat)
    if (hasHeader)
      reader.readNext()

    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
      reader.iterator
        .take(limit.getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT))
        .map(seq => seq.toArray)
    )

    reader.close()

    // build schema based on inferred AttributeTypes.
    // Auto-rename blank header positions to `column-N` so empty CSV headers
    // (e.g. a trailing comma) do not propagate empty attribute names to
    // downstream Iceberg/Parquet writers, which reject them.
    Schema().add(firstRow.indices.map { i =>
      new Attribute(
        if (hasHeader && firstRow(i).nonEmpty) firstRow(i) else s"column-${i + 1}",
        attributeTypeList(i)
      )
    })

  }

}
