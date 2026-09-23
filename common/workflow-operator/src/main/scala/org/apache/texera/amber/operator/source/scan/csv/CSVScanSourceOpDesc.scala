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
import org.apache.texera.amber.operator.StandaloneCodeGenerator.SourceFilePlaceholder
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpExec
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.{IOException, InputStreamReader}
import java.net.URI
import scala.util.Try

class CSVScanSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  // One character: every reader narrows this with charAt(0), because univocity's
  // setDelimiter and scala-csv's DefaultCSVFormat both take a Char.
  //
  // `examples` names a delimiter the fixture's rows do not contain, so the
  // verification config generator does not pick one that parses them ragged.
  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("single character separating the fields on each line")
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
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
    // No setNullValue here, so a blank cell reads as null exactly as it does at
    // execution time (CSVScanSourceOpExec builds its parser without one). Reading it
    // as "" instead made inferField fall through to STRING, which typed a numeric
    // column by its one empty cell rather than by its values.
    val parser = new CsvParser(csvSetting)
    parser.beginParsing(inputReader)

    var data: Array[Array[String]] = Array()
    // A window of no rows is still a window on this file, and the file's columns
    // do not depend on how many of its rows were asked for. Reading the sample
    // through the limit left a Limit of 0 nothing to infer from, and the operator
    // declared a schema of no columns at all.
    val readLimit = limit.filter(_ > 0).getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT)
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

  override def standaloneSourcePath(): Option[String] = fileName

  override def generateStandaloneCode(): String = {
    // Resolve the delimiter the same way the parser above does — first character, empty
    // means comma — and escape it. Every value the field accepts has to survive this:
    // pandas reads a separator longer than one character as a REGULAR EXPRESSION, and a
    // backslash spliced raw produced `sep="\"`, which is not valid Python at all.
    val sep = customDelimiter.filter(_.nonEmpty).getOrElse(",").charAt(0).toString
    // Texera's encoding enum uses values like UTF_8; pandas expects utf-8.
    val encoding = fileEncoding.toString.replace("_", "-").toLowerCase
    val headerArg = if (hasHeader) "0" else "None"

    val args = scala.collection.mutable.ArrayBuffer[String]()
    args += s"filepath_or_buffer=$SourceFilePlaceholder"
    args += s"sep=${pyStringLiteral(sep)}"
    args += s"""encoding=${pyStringLiteral(encoding)}"""
    args += s"header=$headerArg"

    // The parser above sets no null value, so only an empty field is null and every other
    // text stands for itself. pandas instead reads a list of words as missing by default,
    // "NA" and "null" among them, which turned a column holding the country code NA into
    // nulls. Both halves are needed: dropping the default list stops the words, and naming
    // the empty string keeps the blank cell null.
    args += "keep_default_na=False"
    args += """na_values=[""]"""

    // A CSV carries no types, so both readers infer, and they do not infer
    // alike: the schema above tries TIMESTAMP and parses what it can, while
    // pd.read_csv leaves a date column as text. Name the columns this operator
    // decided were timestamps so pandas parses the same ones. They are named by
    // position, header or not, because the schema's names are not pandas' until
    // the rename below: a blank header is `column-2` here and `Unnamed: 1`
    // there, and asking for `column-2` ended the read. pandas takes an integer
    // here as a position even where a header spells one. A schema that cannot be
    // read (an unresolved file) leaves the argument off rather than failing the
    // export.
    val dateColumns: Seq[String] =
      Try(sourceSchema()).toOption.toSeq.flatMap(
        _.getAttributes.zipWithIndex
          .filter(_._1.getType == AttributeType.TIMESTAMP)
          .map(_._2.toString)
      )
    if (dateColumns.nonEmpty) args += s"parse_dates=[${dateColumns.mkString(", ")}]"

    // A LONG column holding a null has to be asked for, or pandas widens it
    // through a float to carry the hole: 9007199254740993 comes back as ...992,
    // a value the file never held and the executor never produced. Int64 is the
    // nullable integer, so the hole costs the column nothing. INTEGER needs none
    // of this — every int32 is exact in a float64. By position, as the dates are.
    val longColumns: Seq[String] =
      Try(sourceSchema()).toOption.toSeq.flatMap(
        _.getAttributes.zipWithIndex
          .filter(_._1.getType == AttributeType.LONG)
          .map { case (_, i) => s"""$i: "Int64"""" }
      )
    if (longColumns.nonEmpty) args += s"dtype={${longColumns.mkString(", ")}}"

    // Clamped: the property editor refuses a negative, but a plan posted to the API
    // can still carry one, and pandas rejects a negative `nrows` outright where the
    // executor's `take` simply keeps no rows.
    offset.map(_.max(0)).foreach { o =>
      // With a header, skip offset rows after row 0; without, skip offset rows from the start.
      // The end of the range is counted in Long: the largest offset the operator
      // accepts overflows an Int on the way past the header, and the range came
      // out empty, skipping nothing where the executor's `drop` keeps no rows.
      if (hasHeader) args += s"skiprows=range(1, ${o.toLong + 1})"
      else args += s"skiprows=$o"
    }
    limit.map(_.max(0)).foreach(l => args += s"nrows=$l")

    val readCall = s"out1df = pd.read_csv(${args.mkString(", ")})"

    // The schema's own names, which every downstream operator was configured
    // against. They differ from pandas' in both directions: a blank header is
    // `column-2` here and `Unnamed: 1` there, and a header the user really did
    // spell `Unnamed: 1` is kept. Matching the placeholder against the index
    // cannot tell those two apart when they coincide; taking the names by
    // position can.
    val schemaNames: Seq[String] =
      Try(sourceSchema()).toOption.toSeq
        .flatMap(_.getAttributes.map(a => pyStringLiteral(a.getName)))

    if (schemaNames.nonEmpty)
      s"""$readCall
         |out1df.columns = [${schemaNames.mkString(", ")}]""".stripMargin
    else if (hasHeader) readCall
    else {
      // Unresolved file: fall back to Texera's headerless naming.
      s"""$readCall
         |out1df.columns = [f"column-{i + 1}" for i in range(len(out1df.columns))]""".stripMargin
    }
  }
}
