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

class CSVOldScanSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  // One character -- see CSVScanSourceOpDesc.
  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("single character separating the fields on each line")
  @JsonSchemaInject(json = """{ "maxLength": 1, "examples": [","] }""")
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

    // This reader has NO missing value at all: scala-csv hands an omitted field
    // back as the empty string, and every other text stands for itself, so a
    // blank cell is "" and even widens its column to STRING. pandas instead
    // reads a list of words as missing by default, "NA" and "null" among them,
    // and reads a blank as NaN. Dropping the default list settles both, and no
    // na_values is named — where the other CSV readers null a blank, this one
    // keeps it. See CSVScanSourceOpDesc.
    args += "keep_default_na=False"

    // Name the columns this operator inferred as timestamps, so pandas parses
    // the same ones instead of leaving them as text. By position, header or
    // not, since a blank header is not yet the schema's name. See
    // CSVScanSourceOpDesc.
    val dateColumns: Seq[String] =
      Try(sourceSchema()).toOption.toSeq.flatMap(
        _.getAttributes.zipWithIndex
          .filter(_._1.getType == AttributeType.TIMESTAMP)
          .map(_._2.toString)
      )
    if (dateColumns.nonEmpty) args += s"parse_dates=[${dateColumns.mkString(", ")}]"

    // Read a LONG column as the nullable integer, so a hole does not widen it
    // through a float and round the values it carries. By position, as the
    // dates are. See CSVScanSourceOpDesc.
    val longColumns: Seq[String] =
      Try(sourceSchema()).toOption.toSeq.flatMap(
        _.getAttributes.zipWithIndex
          .filter(_._1.getType == AttributeType.LONG)
          .map { case (_, i) => s"""$i: "Int64"""" }
      )
    if (longColumns.nonEmpty) args += s"dtype={${longColumns.mkString(", ")}}"

    // Clamped, as in the newer CSV scan: pandas rejects a negative `nrows` where
    // the executor's `take` keeps no rows, and only the editor refuses one.
    offset.map(_.max(0)).foreach { o =>
      // Counted in Long past the header, as in the newer CSV scan.
      if (hasHeader) args += s"skiprows=range(1, ${o.toLong + 1})"
      else args += s"skiprows=$o"
    }
    limit.map(_.max(0)).foreach(l => args += s"nrows=$l")

    val readCall = s"out1df = pd.read_csv(${args.mkString(", ")})"

    // The schema's own names, which every downstream operator was configured
    // against: sourceSchema below rewrites a blank header to `column-N`, where
    // pandas writes `Unnamed: 1`. Taken by position, so a header the user really
    // did spell `Unnamed: 1` survives too. See CSVScanSourceOpDesc.
    val schemaNames: Seq[String] =
      Try(sourceSchema()).toOption.toSeq
        .flatMap(_.getAttributes.map(a => pyStringLiteral(a.getName)))

    if (schemaNames.nonEmpty)
      s"""$readCall
         |out1df.columns = [${schemaNames.mkString(", ")}]""".stripMargin
    else if (hasHeader) readCall
    else
      // Unresolved file: fall back to Texera's headerless naming.
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
    var reader: CSVReader =
      CSVReader.open(file, fileEncoding.getCharset.name())(CustomFormat)
    val firstRow: Array[String] = reader.iterator.next().toArray
    reader.close()

    // reopen the file to read from the beginning
    reader = CSVReader.open(file, fileEncoding.getCharset.name())(CustomFormat)

    val startOffset = offset.getOrElse(0) + (if (hasHeader) 1 else 0)
    // A window of no rows is still a window on this file, and the file's columns
    // do not depend on how many of its rows were asked for. Reading the sample
    // through the limit left a Limit of 0 nothing to infer from, and the types
    // came back empty while the header below still asked each column for one.
    val endOffset =
      startOffset + limit.filter(_ > 0).getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT)
    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
      reader.iterator
        .slice(startOffset, endOffset)
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
