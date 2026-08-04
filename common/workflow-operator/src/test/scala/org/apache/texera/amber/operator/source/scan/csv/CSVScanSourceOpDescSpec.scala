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
          // The upper bound is what the property editor validates a stored delimiter
          // against, so one character is the longest it calls valid and anything
          // longer -- `,;`, `;abc` -- is what it starts flagging.
          assert(propertySchema.path("maxLength").asInt() == 1)
          // Deliberately no lower bound: the field is optional and every reader
          // resolves an empty delimiter to a comma, so clearing it stays valid.
          assert(propertySchema.path("minLength").isMissingNode)
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

}
