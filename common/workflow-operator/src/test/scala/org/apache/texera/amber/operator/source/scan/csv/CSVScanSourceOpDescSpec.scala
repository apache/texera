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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.TextNode
import com.github.fge.jsonschema.main.JsonSchemaFactory
import org.apache.texera.amber.core.storage.FileResolver
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.operator.{LogicalOp, TestOperators}
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.csvOld.CSVOldScanSourceOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class CSVScanSourceOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var csvScanSourceOpDesc: CSVScanSourceOpDesc = _
  var parallelCsvScanSourceOpDesc: ParallelCSVScanSourceOpDesc = _
  before {
    csvScanSourceOpDesc = new CSVScanSourceOpDesc()
    parallelCsvScanSourceOpDesc = new ParallelCSVScanSourceOpDesc()
  }

  private val delimiterOwners: List[(String, Class[_ <: LogicalOp])] = List(
    "CSV" -> classOf[CSVScanSourceOpDesc],
    "parallel CSV" -> classOf[ParallelCSVScanSourceOpDesc],
    "old CSV" -> classOf[CSVOldScanSourceOpDesc]
  )

  private def delimiterSchema(opDescClass: Class[_ <: LogicalOp]): JsonNode =
    OperatorMetadataGenerator
      .generateOperatorJsonSchema(opDescClass)
      .path("properties")
      .path("customDelimiter")

  // The property editor validates a stored delimiter against the schema, so validate
  // the same way rather than restating the bound the schema declares.
  private def schemaValidates(propertySchema: JsonNode, delimiter: String): Boolean =
    JsonSchemaFactory
      .byDefault()
      .getJsonSchema(propertySchema)
      .validate(TextNode.valueOf(delimiter))
      .isSuccess

  // Writes a CSV whose header row has an empty column (the third position),
  // e.g. `id,name,,age`, and returns the absolute path.
  private def writeCsvWithEmptyHeader(): String = {
    val tmpFile = Files.createTempFile("empty-header-", ".csv")
    tmpFile.toFile.deleteOnExit()
    Files.write(
      tmpFile,
      "id,name,,age\n1,Alice,x,30\n2,Bob,y,25\n".getBytes(StandardCharsets.UTF_8)
    )
    tmpFile.toString
  }

  // Writes a three-column `;`-separated CSV and returns the absolute path.
  private def writeSemicolonCsv(): String = {
    val tmpFile = Files.createTempFile("semicolon-", ".csv")
    tmpFile.toFile.deleteOnExit()
    Files.write(
      tmpFile,
      "id;name;age\n1;Alice;30\n2;Bob;25\n".getBytes(StandardCharsets.UTF_8)
    )
    tmpFile.toString
  }

  private def columnNames(opDesc: ScanSourceOpDesc, path: String): List[String] = {
    opDesc.fileName = Some(path)
    opDesc.setResolvedFileName(FileResolver.resolve(path))
    opDesc.sourceSchema().getAttributes.map(_.getName).toList
  }

  // Writes a CSV whose `big` column holds values past 2^53 and one omitted cell,
  // and returns the absolute path.
  private def writeNullableLongCsv(): String = {
    val tmpFile = Files.createTempFile("nullable-long-", ".csv")
    tmpFile.toFile.deleteOnExit()
    Files.write(
      tmpFile,
      "id,big\n1,9007199254740993\n2,\n3,9007199254740995\n".getBytes(StandardCharsets.UTF_8)
    )
    tmpFile.toString
  }

  // Writes a CSV holding the country code NA and an omitted field, and returns
  // the absolute path.
  private def writeNaCsv(): String = {
    val tmpFile = Files.createTempFile("na-code-", ".csv")
    tmpFile.toFile.deleteOnExit()
    Files.write(tmpFile, "code,note\nNA,x\n,y\n".getBytes(StandardCharsets.UTF_8))
    tmpFile.toString
  }

  // Writes a headered CSV with six data rows and returns the absolute path.
  private def writeSixRowCsv(): String = {
    val tmpFile = Files.createTempFile("six-row-", ".csv")
    tmpFile.toFile.deleteOnExit()
    Files.write(
      tmpFile,
      "id,name\n1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n".getBytes(StandardCharsets.UTF_8)
    )
    tmpFile.toString
  }

  // Writes a numeric column with one blank cell and returns the absolute path.
  private def writeCsvWithBlankNumericCell(): String = {
    val tmpFile = Files.createTempFile("blank-cell-", ".csv")
    tmpFile.toFile.deleteOnExit()
    Files.write(
      tmpFile,
      "id,measure\n1,2.5\n2,\n3,4.5\n".getBytes(StandardCharsets.UTF_8)
    )
    tmpFile.toString
  }

  it should "infer a numeric column as DOUBLE even when one of its cells is blank" in {
    val path = writeCsvWithBlankNumericCell()
    csvScanSourceOpDesc.fileName = Some(path)
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))

    // A blank used to read as "" while inferring and as null while executing. The ""
    // fell through inferField to STRING, so one empty cell retyped the whole column
    // and every downstream numeric operator then received strings.
    assert(
      csvScanSourceOpDesc.sourceSchema().getAttribute("measure").getType == AttributeType.DOUBLE
    )
  }

  it should "infer schema from single-line-data csv" in {

    parallelCsvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesSmallCsvPath)
    parallelCsvScanSourceOpDesc.customDelimiter = Some(",")
    parallelCsvScanSourceOpDesc.hasHeader = true
    parallelCsvScanSourceOpDesc.setResolvedFileName(
      FileResolver.resolve(parallelCsvScanSourceOpDesc.fileName.get)
    )
    val inferredSchema: Schema = parallelCsvScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 14)
    assert(inferredSchema.getAttribute("Order ID").getType == AttributeType.INTEGER)
    assert(inferredSchema.getAttribute("Unit Price").getType == AttributeType.DOUBLE)

  }

  it should "infer schema from headerless single-line-data csv" in {

    parallelCsvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesHeaderlessSmallCsvPath)
    parallelCsvScanSourceOpDesc.customDelimiter = Some(",")
    parallelCsvScanSourceOpDesc.hasHeader = false
    parallelCsvScanSourceOpDesc.setResolvedFileName(
      FileResolver.resolve(parallelCsvScanSourceOpDesc.fileName.get)
    )

    val inferredSchema: Schema = parallelCsvScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 14)
    assert(inferredSchema.getAttribute("column-10").getType == AttributeType.DOUBLE)
    assert(inferredSchema.getAttribute("column-7").getType == AttributeType.INTEGER)
  }

  it should "infer schema from multi-line-data csv" in {

    csvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesSmallMultiLineCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.hasHeader = true
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(csvScanSourceOpDesc.fileName.get))

    val inferredSchema: Schema = csvScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 14)
    assert(inferredSchema.getAttribute("Order ID").getType == AttributeType.INTEGER)
    assert(inferredSchema.getAttribute("Unit Price").getType == AttributeType.DOUBLE)
  }

  it should "infer schema from headerless multi-line-data csv" in {

    csvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesHeaderlessSmallCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.hasHeader = false
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(csvScanSourceOpDesc.fileName.get))

    val inferredSchema: Schema = csvScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 14)
    assert(inferredSchema.getAttribute("column-10").getType == AttributeType.DOUBLE)
    assert(inferredSchema.getAttribute("column-7").getType == AttributeType.INTEGER)
  }

  it should "infer schema from headerless multi-line-data csv with custom delimiter" in {

    csvScanSourceOpDesc.fileName =
      Some(TestOperators.CountrySalesSmallMultiLineCustomDelimiterCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(";")
    csvScanSourceOpDesc.hasHeader = false
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(csvScanSourceOpDesc.fileName.get))

    val inferredSchema: Schema = csvScanSourceOpDesc.sourceSchema()

    assert(inferredSchema.getAttributes.length == 14)
    assert(inferredSchema.getAttribute("column-10").getType == AttributeType.DOUBLE)
    assert(inferredSchema.getAttribute("column-7").getType == AttributeType.INTEGER)
  }

  it should "create one worker with multi-line-data csv" in {

    csvScanSourceOpDesc.fileName =
      Some(TestOperators.CountrySalesSmallMultiLineCustomDelimiterCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(";")
    csvScanSourceOpDesc.hasHeader = false
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(csvScanSourceOpDesc.fileName.get))

    assert(
      !csvScanSourceOpDesc
        .getPhysicalOp(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)
        .parallelizable
    )
  }

  // The name is left to the translator, which is the only thing that can see a
  // second source wanting it. What the operator owes is the path and the name it
  // would like.
  it should "offer the csv basename and read the file by placeholder" in {
    csvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesSmallMultiLineCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.hasHeader = true
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(csvScanSourceOpDesc.fileName.get))

    val code = csvScanSourceOpDesc.generateStandaloneCode()

    assert(code.contains("filepath_or_buffer=sourceFile"))
    assert(csvScanSourceOpDesc.standaloneSourcePath() == csvScanSourceOpDesc.fileName)
    assert(
      csvScanSourceOpDesc.standaloneSourceName().contains("country_sales_small_multi_line.csv")
    )
    assert(!code.contains("base64.b64decode"))
    assert(!code.contains("io.BytesIO"))
  }

  it should "offer the unresolved csv basename" in {
    csvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesSmallMultiLineCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.hasHeader = true

    val code = csvScanSourceOpDesc.generateStandaloneCode()

    assert(code.contains("filepath_or_buffer=sourceFile"))
    assert(
      csvScanSourceOpDesc.standaloneSourceName().contains("country_sales_small_multi_line.csv")
    )
    assert(!code.contains("base64.b64decode"))
    assert(!code.contains("io.BytesIO"))
  }

  // Only the property editor refuses a negative window; a plan posted to the API
  // arrives with one intact. pandas rejects a negative nrows outright, where the
  // executor's take just keeps no rows, so the export asks for the empty window.
  it should "ask pandas for the empty window a negative limit means to the executor" in {
    csvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesSmallMultiLineCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.hasHeader = true
    csvScanSourceOpDesc.limit = Some(-1)
    csvScanSourceOpDesc.offset = Some(-1)

    val code = csvScanSourceOpDesc.generateStandaloneCode()

    assert(code.contains("nrows=0"))
    assert(code.contains("skiprows=range(1, 1)"))
  }

  // The largest offset the operator accepts is an Int, and the row past the
  // header is not. Added as Ints the range ran to a negative and came out empty,
  // so pandas skipped nothing where the executor's drop keeps no rows.
  it should "count the skipped range past what an Int holds" in {
    csvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesSmallMultiLineCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.hasHeader = true
    csvScanSourceOpDesc.offset = Some(Int.MaxValue)

    assert(csvScanSourceOpDesc.generateStandaloneCode().contains("skiprows=range(1, 2147483648)"))
  }

  // The parser sets no null value, so only an empty field is null. pandas reads a list of
  // words as missing by default, which turned the country code NA into a null.
  it should "read only an empty field as null, the way the parser does" in {
    csvScanSourceOpDesc.fileName = Some(TestOperators.CountrySalesSmallMultiLineCsvPath)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.hasHeader = true

    val code = csvScanSourceOpDesc.generateStandaloneCode()

    assert(code.contains("keep_default_na=False"))
    assert(code.contains("""na_values=[""]"""))
  }

  // sourceSchema names a blank header column-N; pandas names it "Unnamed: N". A downstream
  // operator asks for the name the schema gave, so the frame has to carry that one, and by
  // position rather than by matching the placeholder: a header the user really did spell
  // "Unnamed: 1" is kept, and matching cannot tell the two apart where they coincide.
  it should "give the frame the names the schema gives it" in {
    val path = writeCsvWithEmptyHeader()
    csvScanSourceOpDesc.fileName = Some(path)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.hasHeader = true
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))

    val code = csvScanSourceOpDesc.generateStandaloneCode()

    assert(code.contains("""out1df.columns = ["id", "name", "column-3", "age"]"""))
  }

  // A null is what forces the widening: pandas carries the hole in a float, and a
  // long past 2^53 does not survive the trip. 9007199254740993 came back as ...992,
  // a value the file never held and the executor never produced. Int64 is the
  // nullable integer, so the column keeps both its values and its hole.
  it should "read a nullable long as an exact integer, the way the parser does" in {
    val path = writeNullableLongCsv()
    csvScanSourceOpDesc.fileName = Some(path)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))

    // This reader's blank is a null, which inferField passes over, so the column
    // keeps the type its values have.
    assert(csvScanSourceOpDesc.sourceSchema().getAttribute("big").getType == AttributeType.LONG)

    val code = csvScanSourceOpDesc.generateStandaloneCode()
    assert(code.contains("""dtype={1: "Int64"}"""))
    // `id` has no hole, so a float would carry it exactly. Only the column the
    // schema calls LONG is asked for.
    assert(!code.contains("""0: "Int64""""))
  }

  // The schema calls a blank header `column-2` and pandas calls it `Unnamed: 1`
  // until the rename, so a date column asked for by the schema's name ended the
  // read on a missing column. By position, the two agree.
  it should "ask pandas for a date under a blank header by its position" in {
    val tmpFile = Files.createTempFile("blank-date-header-", ".csv")
    tmpFile.toFile.deleteOnExit()
    Files.write(tmpFile, "id,\n1,2024-01-01 00:00:00\n".getBytes(StandardCharsets.UTF_8))
    val path = tmpFile.toString
    csvScanSourceOpDesc.fileName = Some(path)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.hasHeader = true
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))

    assert(
      csvScanSourceOpDesc.sourceSchema().getAttribute("column-2").getType ==
        AttributeType.TIMESTAMP
    )

    val code = csvScanSourceOpDesc.generateStandaloneCode()
    assert(code.contains("parse_dates=[1]"))
    assert(code.contains("""out1df.columns = ["id", "column-2"]"""))
  }

  it should "ask pandas for a date under a blank header by its position for old CSV" in {
    val tmpFile = Files.createTempFile("blank-date-header-", ".csv")
    tmpFile.toFile.deleteOnExit()
    Files.write(tmpFile, "id,\n1,2024-01-01 00:00:00\n".getBytes(StandardCharsets.UTF_8))
    val path = tmpFile.toString
    val oldCsvScanSourceOpDesc = new CSVOldScanSourceOpDesc()
    oldCsvScanSourceOpDesc.fileName = Some(path)
    oldCsvScanSourceOpDesc.customDelimiter = Some(",")
    oldCsvScanSourceOpDesc.hasHeader = true
    oldCsvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))

    assert(
      oldCsvScanSourceOpDesc.sourceSchema().getAttribute("column-2").getType ==
        AttributeType.TIMESTAMP
    )
    assert(oldCsvScanSourceOpDesc.generateStandaloneCode().contains("parse_dates=[1]"))
  }

  // pandas raises nothing for a type asked of a name it has no column for, so
  // under a blank header the type was dropped without a word and the column
  // was inferred as floats, rounding 9007199254740993 to ...992.
  it should "ask pandas for a type under a blank header by its position for parallel CSV" in {
    val tmpFile = Files.createTempFile("blank-long-header-", ".csv")
    tmpFile.toFile.deleteOnExit()
    Files.write(
      tmpFile,
      "id,\n1,9007199254740993\n2,\n3,9007199254740995\n".getBytes(StandardCharsets.UTF_8)
    )
    val path = tmpFile.toString
    parallelCsvScanSourceOpDesc.fileName = Some(path)
    parallelCsvScanSourceOpDesc.customDelimiter = Some(",")
    parallelCsvScanSourceOpDesc.hasHeader = true
    parallelCsvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))

    assert(
      parallelCsvScanSourceOpDesc.sourceSchema().getAttribute("column-2").getType ==
        AttributeType.STRING
    )
    assert(parallelCsvScanSourceOpDesc.generateStandaloneCode().contains("""dtype={1: "string"}"""))
  }

  // sourceSchema reads this operator's file with scala-csv, which hands a blank
  // back as "", so one blank cell types the whole column STRING — while the
  // executor's block reader nulls the blank and parses the rest as that STRING.
  // pandas sees the blank as missing and infers a number instead, which read a
  // column of ids back as floats and rounded 9007199254740993 to ...992.
  it should "read a parallel CSV column the schema typed STRING as text" in {
    val path = writeNullableLongCsv()
    parallelCsvScanSourceOpDesc.fileName = Some(path)
    parallelCsvScanSourceOpDesc.customDelimiter = Some(",")
    parallelCsvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))

    assert(
      parallelCsvScanSourceOpDesc.sourceSchema().getAttribute("big").getType ==
        AttributeType.STRING
    )

    val exec = new ParallelCSVScanSourceOpExec(
      objectMapper.writeValueAsString(parallelCsvScanSourceOpDesc)
    )
    exec.open()
    val rows =
      try exec.produceTuple().map(_.getFields.toList).toList
      finally exec.close()
    assert(rows.map(_(1)) == List("9007199254740993", null, "9007199254740995"))

    assert(
      parallelCsvScanSourceOpDesc
        .generateStandaloneCode()
        .contains("""dtype={1: "string"}""")
    )
  }

  // The block reader nulls an omitted field and leaves every other text alone, so
  // "NA" is the country code it says it is. pandas reads it as missing by default,
  // so the export read a column of codes as a column of nulls.
  it should "read only an empty field as null for parallel CSV, the way its reader does" in {
    val path = writeNaCsv()
    parallelCsvScanSourceOpDesc.fileName = Some(path)
    parallelCsvScanSourceOpDesc.customDelimiter = Some(",")
    parallelCsvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))

    val exec = new ParallelCSVScanSourceOpExec(
      objectMapper.writeValueAsString(parallelCsvScanSourceOpDesc)
    )
    exec.open()
    val rows =
      try exec.produceTuple().map(_.getFields.toList).toList
      finally exec.close()
    assert(rows == List(List("NA", "x"), List(null, "y")))

    val code = parallelCsvScanSourceOpDesc.generateStandaloneCode()
    assert(code.contains("keep_default_na=False"))
    assert(code.contains("""na_values=[""]"""))
  }

  it should "give the parallel CSV frame the names the schema gives it" in {
    val path = writeCsvWithEmptyHeader()
    parallelCsvScanSourceOpDesc.fileName = Some(path)
    parallelCsvScanSourceOpDesc.customDelimiter = Some(",")
    parallelCsvScanSourceOpDesc.hasHeader = true
    parallelCsvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))

    assert(
      parallelCsvScanSourceOpDesc
        .generateStandaloneCode()
        .contains("""out1df.columns = ["id", "name", "column-3", "age"]""")
    )
  }

  // ParallelCSVScanSourceOpExec.open carves the file into byte ranges and leaves
  // limit and offset as TODOs, so the window the panel offers never reaches the
  // rows. Slicing in the export handed back fewer rows than the workflow did.
  it should "leave limit and offset out of the parallel CSV read, as its reader ignores them" in {
    val path = writeSixRowCsv()
    parallelCsvScanSourceOpDesc.fileName = Some(path)
    parallelCsvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))
    parallelCsvScanSourceOpDesc.customDelimiter = Some(",")
    parallelCsvScanSourceOpDesc.offset = Some(2)
    parallelCsvScanSourceOpDesc.limit = Some(2)

    val exec = new ParallelCSVScanSourceOpExec(
      objectMapper.writeValueAsString(parallelCsvScanSourceOpDesc)
    )
    exec.open()
    val rowsRead =
      try exec.produceTuple().size
      finally exec.close()
    assert(rowsRead == 6)

    val code = parallelCsvScanSourceOpDesc.generateStandaloneCode()
    assert(!code.contains("skiprows"))
    assert(!code.contains("nrows"))
    assert(code.startsWith("# NOTE: this operator's limit and offset are ignored"))
  }

  it should "use comma as the default delimiter when customDelimiter is not set for parallel CSV" in {
    parallelCsvScanSourceOpDesc.customDelimiter = None

    parallelCsvScanSourceOpDesc.getPhysicalOp(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)

    assert(parallelCsvScanSourceOpDesc.customDelimiter.contains(","))
  }

  it should "use comma as the default delimiter when customDelimiter is empty string for parallel CSV" in {
    parallelCsvScanSourceOpDesc.customDelimiter = Some("")

    parallelCsvScanSourceOpDesc.getPhysicalOp(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)

    assert(parallelCsvScanSourceOpDesc.customDelimiter.contains(","))
  }

  it should "use comma as the default delimiter when customDelimiter is not set for CSV" in {
    csvScanSourceOpDesc.customDelimiter = None

    csvScanSourceOpDesc.getPhysicalOp(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)

    assert(csvScanSourceOpDesc.customDelimiter.contains(","))
  }

  it should "use comma as the default delimiter when customDelimiter is empty string for CSV" in {
    csvScanSourceOpDesc.customDelimiter = Some("")

    csvScanSourceOpDesc.getPhysicalOp(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)

    assert(csvScanSourceOpDesc.customDelimiter.contains(","))
  }

  it should "auto-rename empty CSV column headers to column-N" in {
    val path = writeCsvWithEmptyHeader()
    csvScanSourceOpDesc.fileName = Some(path)
    csvScanSourceOpDesc.customDelimiter = Some(",")
    csvScanSourceOpDesc.hasHeader = true
    csvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))

    val names = csvScanSourceOpDesc.sourceSchema().getAttributes.map(_.getName).toList
    assert(names == List("id", "name", "column-3", "age"))
  }

  it should "auto-rename empty CSV column headers to column-N for parallel CSV" in {
    val path = writeCsvWithEmptyHeader()
    parallelCsvScanSourceOpDesc.fileName = Some(path)
    parallelCsvScanSourceOpDesc.customDelimiter = Some(",")
    parallelCsvScanSourceOpDesc.hasHeader = true
    parallelCsvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))

    val names = parallelCsvScanSourceOpDesc.sourceSchema().getAttributes.map(_.getName).toList
    assert(names == List("id", "name", "column-3", "age"))
  }

  it should "auto-rename empty CSV column headers to column-N for old CSV" in {
    val path = writeCsvWithEmptyHeader()
    val oldCsvScanSourceOpDesc = new CSVOldScanSourceOpDesc()
    oldCsvScanSourceOpDesc.fileName = Some(path)
    oldCsvScanSourceOpDesc.customDelimiter = Some(",")
    oldCsvScanSourceOpDesc.hasHeader = true
    oldCsvScanSourceOpDesc.setResolvedFileName(FileResolver.resolve(path))

    val names = oldCsvScanSourceOpDesc.sourceSchema().getAttributes.map(_.getName).toList
    assert(names == List("id", "name", "column-3", "age"))
  }

  it should "declare the delimiter as a single character on every CSV scan" in {
    delimiterOwners.foreach {
      case (name, opDescClass) =>
        withClue(s"$name: ") {
          val propertySchema = delimiterSchema(opDescClass)
          assert(propertySchema.path("type").asText() == "string")
          assert(propertySchema.path("maxLength").asInt() == 1)
        }
    }
  }

  it should "validate an empty or one-character delimiter and refuse a longer one" in {
    delimiterOwners.foreach {
      case (name, opDescClass) =>
        withClue(s"$name: ") {
          val propertySchema = delimiterSchema(opDescClass)
          // Empty stays valid: the field is optional and every reader resolves an
          // empty delimiter to a comma, so clearing it must not be an error.
          assert(schemaValidates(propertySchema, ""))
          assert(schemaValidates(propertySchema, ","))
          assert(schemaValidates(propertySchema, ";"))
          assert(!schemaValidates(propertySchema, ",;"))
          assert(!schemaValidates(propertySchema, ";abc"))
        }
    }
  }

  it should "read a multi-character delimiter as its first character" in {
    // What the constraint gives up: a saved workflow holding a longer delimiter now
    // shows as invalid in the property editor. Its run is unchanged, which is what
    // this pins -- the characters past the first never reached a parser.
    val path = writeSemicolonCsv()
    val csv = new CSVScanSourceOpDesc()
    csv.customDelimiter = Some(";abc")
    val parallelCsv = new ParallelCSVScanSourceOpDesc()
    parallelCsv.customDelimiter = Some(";abc")
    val oldCsv = new CSVOldScanSourceOpDesc()
    oldCsv.customDelimiter = Some(";abc")

    assert(columnNames(csv, path) == List("id", "name", "age"))
    assert(columnNames(parallelCsv, path) == List("id", "name", "age"))
    assert(columnNames(oldCsv, path) == List("id", "name", "age"))
  }

  // The limit bounded the sample the inference reads as well as the rows the
  // operator emits, so a Limit of 0 had nothing to infer from. The three readers
  // then failed differently on the same file: these two declared a schema of no
  // columns at all, and the old one threw, its header still asking each column
  // for a type the empty sample could not give. A file's columns do not depend
  // on how many of its rows were asked for.
  it should "keep the file's columns when the window asks for no rows" in {
    val path = writeSemicolonCsv()
    val csv = new CSVScanSourceOpDesc()
    csv.customDelimiter = Some(";")
    csv.limit = Some(0)
    val parallelCsv = new ParallelCSVScanSourceOpDesc()
    parallelCsv.customDelimiter = Some(";")
    parallelCsv.limit = Some(0)
    val oldCsv = new CSVOldScanSourceOpDesc()
    oldCsv.customDelimiter = Some(";")
    oldCsv.limit = Some(0)

    assert(columnNames(csv, path) == List("id", "name", "age"))
    assert(columnNames(parallelCsv, path) == List("id", "name", "age"))
    assert(columnNames(oldCsv, path) == List("id", "name", "age"))
  }

}
