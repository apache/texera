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

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.inferSchemaFromRows
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpExec
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.{IOException, InputStreamReader}
import java.net.URI
import java.nio.file.Paths

class CSVScanSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  // Almost anything: the delimiter reaches the parser as `charAt(0)` and empty is read
  // as the default comma, so a longer value is not refused, it just has its tail
  // ignored. The one value that does fail is a leading NEWLINE — measured, both readers
  // here take it, but pandas refuses it outright ("Specified \n as separator"), so the
  // exported script would die on a config the platform runs. Excluded through a
  // lookahead rather than an `^$` alternative because `$` in Java and Python also
  // matches before a trailing newline, which would let `"\n"` back in; the lookahead
  // reads the same in all three engines. `examples` gives a legal sample to whatever
  // needs one.
  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("delimiter to separate each line into fields")
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  @JsonSchemaInject(json = """
{
  "pattern": "^(?!\\n)[\\s\\S]*$",
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
          "org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
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
    val stream = DocumentFactory.openReadonlyDocument(uri).asInputStream()
    val inputReader =
      new InputStreamReader(stream, fileEncoding.getCharset)

    val csvFormat = new CsvFormat()
    csvFormat.setDelimiter(delimiterChar)
    csvFormat.setLineSeparator("\n")
    val csvSetting = new CsvParserSettings()
    csvSetting.setMaxCharsPerColumn(-1)
    val maxColumns = CSVScanSourceOpExec.getMaxColumns
    csvSetting.setMaxColumns(maxColumns)
    csvSetting.setFormat(csvFormat)
    csvSetting.setHeaderExtractionEnabled(hasHeader)
    csvSetting.setNullValue("")
    val parser = new CsvParser(csvSetting)
    parser.beginParsing(inputReader)

    var data: Array[Array[String]] = Array()
    val readLimit = limit.getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT)
    for (_ <- 0 until readLimit) {
      val row = CSVScanSourceOpExec.parseNextRow(parser, maxColumns)
      if (row != null) {
        data = data :+ row
      }
    }
    parser.stopParsing()
    inputReader.close()

    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
      data.iterator.asInstanceOf[Iterator[Array[Any]]]
    )

    val header: Array[String] =
      if (hasHeader)
        Option(parser.getContext.headers())
          .getOrElse((1 to attributeTypeList.length).map(i => "column-" + i).toArray)
      else (1 to attributeTypeList.length).map(i => "column-" + i).toArray

    header.indices.foldLeft(Schema()) { (schema, i) =>
      // Auto-rename blank header positions to `column-N` so empty CSV headers
      // (e.g. a trailing comma) do not propagate empty attribute names to
      // downstream Iceberg/Parquet writers, which reject them.
      val name = Option(header(i)).filter(_.nonEmpty).getOrElse(s"column-${i + 1}")
      schema.add(name, attributeTypeList(i))
    }

  }

  override def generateStandaloneCode(): String = {
    // Strip to just the basename. The standalone script assumes the CSV
    // lives in the same directory as the script (Texera's resolved URIs
    // can't be used directly outside the system).
    val rawPath = fileName.getOrElse("")
    val basename = Paths.get(new URI(rawPath).getPath).getFileName.toString

    // Resolve the delimiter the same way the parser above does — first character, empty
    // means comma — and escape it. Every value the field accepts has to survive this:
    // pandas reads a separator longer than one character as a REGULAR EXPRESSION, and a
    // backslash spliced raw produced `sep="\"`, which is not valid Python at all.
    val sep = customDelimiter.filter(_.nonEmpty).getOrElse(",").charAt(0).toString
    // Texera's encoding enum uses values like UTF_8; pandas expects utf-8.
    val encoding = fileEncoding.toString.replace("_", "-").toLowerCase
    val headerArg = if (hasHeader) "0" else "None"

    val args = scala.collection.mutable.ArrayBuffer[String]()
    args += s"""filepath_or_buffer=${pyStringLiteral(basename)}"""
    args += s"sep=${pyStringLiteral(sep)}"
    args += s"""encoding=${pyStringLiteral(encoding)}"""
    args += s"header=$headerArg"

    offset.foreach { o =>
      // With a header, skip offset rows after row 0; without, skip offset rows from the start.
      if (hasHeader) args += s"skiprows=range(1, ${o + 1})"
      else args += s"skiprows=$o"
    }
    limit.foreach(l => args += s"nrows=$l")

    val readCall = s"out1df = pd.read_csv(${args.mkString(", ")})"

    if (hasHeader) readCall
    else {
      // Match Texera's fallback column naming when there's no header
      s"""$readCall
         |out1df.columns = [f"column-{i + 1}" for i in range(len(out1df.columns))]""".stripMargin
    }
  }
}
