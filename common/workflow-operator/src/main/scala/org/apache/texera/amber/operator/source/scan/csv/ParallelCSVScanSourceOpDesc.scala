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
import org.apache.texera.amber.operator.StandaloneCodeGenerator.SourceFilePlaceholder
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.IOException
import java.net.URI
import scala.util.Try

class ParallelCSVScanSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  // One character -- see CSVScanSourceOpDesc.
  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("single character separating the fields on each line")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @JsonSchemaInject(json = """{ "maxLength": 1, "examples": [","] }""")
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

  override def standaloneSourcePath(): Option[String] = fileName

  override def generateStandaloneCode(): String = {
    // First character, empty means comma — the same resolution the reader below does —
    // and escaped, so every value the field accepts survives being spliced into Python.
    // See CSVScanSourceOpDesc for what handing pandas the raw value did.
    val sep = customDelimiter.filter(_.nonEmpty).getOrElse(",").charAt(0).toString
    val encoding = fileEncoding.toString.replace("_", "-").toLowerCase
    val headerArg = if (hasHeader) "0" else "None"

    val args = scala.collection.mutable.ArrayBuffer[String]()
    args += s"filepath_or_buffer=$SourceFilePlaceholder"
    args += s"sep=${pyStringLiteral(sep)}"
    args += s"""encoding=${pyStringLiteral(encoding)}"""
    args += s"header=$headerArg"

    // The block reader nulls an omitted field and leaves every other text alone,
    // so only an empty field is missing here. pandas reads a list of words as
    // missing by default, "NA" and "null" among them, which turned a column
    // holding the country code NA into nulls. See CSVScanSourceOpDesc: both
    // halves are needed, one to stop the words and one to keep the blank null.
    args += "keep_default_na=False"
    args += """na_values=[""]"""

    // Ask for the schema's own type wherever pandas would infer another one.
    // The two halves of this operator disagree about a blank: sourceSchema reads
    // with scala-csv, where a blank is "" and types its column STRING, while the
    // executor nulls it and parses the rest as that STRING. pandas infers a number
    // instead, so a column of ids came back as floats, one past 2^53 rounded:
    // 9007199254740993 as ...992. A LONG needs the nullable integer for the same
    // reason. By position, as in CSVScanSourceOpDesc: under a blank header the
    // schema's `column-2` is pandas' `Unnamed: 1`, and pandas passes over a
    // name it has no column for, so the type was never applied.
    val dtypes: Seq[String] =
      Try(sourceSchema()).toOption.toSeq.flatMap(
        _.getAttributes.zipWithIndex
          .flatMap {
            case (a, i) =>
              val pandasType = a.getType match {
                case AttributeType.LONG   => Some("Int64")
                case AttributeType.STRING => Some("string")
                case _                    => None
              }
              pandasType.map(t => s"""$i: "$t"""")
          }
      )
    if (dtypes.nonEmpty) args += s"dtype={${dtypes.mkString(", ")}}"

    // Limit and offset are inherited fields the parallel reader never reads:
    // ParallelCSVScanSourceOpExec.open carves the file into byte ranges and
    // leaves both as TODOs. Slicing here gave the export fewer rows than the
    // workflow produced, so the window is dropped and said to be dropped.
    val ignoredWindow =
      if (offset.isEmpty && limit.isEmpty) Seq.empty
      else
        Seq(
          "# NOTE: this operator's limit and offset are ignored, as the parallel CSV reader ignores them."
        )

    val readCall = s"out1df = pd.read_csv(${args.mkString(", ")})"

    // The schema's own names, which every downstream operator was configured
    // against: sourceSchema below rewrites a blank header to `column-N`, where
    // pandas writes `Unnamed: 1`. Taken by position, so a header the user really
    // did spell `Unnamed: 1` survives too. See CSVScanSourceOpDesc.
    val schemaNames: Seq[String] =
      Try(sourceSchema()).toOption.toSeq
        .flatMap(_.getAttributes.map(a => pyStringLiteral(a.getName)))

    val body =
      if (schemaNames.nonEmpty)
        s"""$readCall
           |out1df.columns = [${schemaNames.mkString(", ")}]""".stripMargin
      else if (hasHeader) readCall
      else
        // Unresolved file: fall back to Texera's headerless naming.
        s"""$readCall
           |out1df.columns = [f"column-{i + 1}" for i in range(len(out1df.columns))]""".stripMargin

    (ignoredWindow :+ body).mkString("\n")
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

    // A window of no rows is still a window on this file, and the file's columns
    // do not depend on how many of its rows were asked for. Reading the sample
    // through the limit left a Limit of 0 nothing to infer from, and the types
    // came back empty while the header below still asked each column for one.
    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
      reader.iterator
        .take(limit.filter(_ > 0).getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT))
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
