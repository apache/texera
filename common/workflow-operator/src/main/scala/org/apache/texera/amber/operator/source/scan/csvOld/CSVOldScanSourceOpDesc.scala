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
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.inferSchemaFromRows
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.operator.metadata.annotations.UIWidget
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.csv.CsvScanSupport
import org.apache.texera.amber.operator.{StandaloneCodeGenerator, StandaloneHelpers}

import java.io.IOException

class CSVOldScanSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("single character separating the fields on each line")
  @JsonSchemaInject(json = UIWidget.UIWidgetCharDelimiter)
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
    if (customDelimiter.get.isEmpty) {
      customDelimiter = Option(",")
    }
    sourcePhysicalOp(
      workflowId,
      executionId,
      "org.apache.texera.amber.operator.source.scan.csvOld.CSVOldScanSourceOpExec"
    )
  }

  override def standaloneSourcePath(): Option[String] = fileName

  override def standaloneHelpers(): Seq[String] = Seq(StandaloneHelpers.AttributeCasts)

  override def generateStandaloneCode(): String =
    CsvScanSupport.generateStandaloneCode(
      this,
      customDelimiter,
      hasHeader,
      dtypeByType = Map(AttributeType.LONG -> "Int64", AttributeType.TIMESTAMP -> "object"),
      includeNaValuesBlank = false,
      includeWindow = true,
      declaredSchemaBody = (readCall, declared) =>
        s"$readCall\n${StandaloneCodeGenerator.readAsDeclared("out1df", declared)}"
    )

  override def sourceSchema(): Schema = {
    val (uri, delimiterChar) = CsvScanSupport.resolveCsvUriAndDelimiter(this, customDelimiter)
    val file = DocumentFactory.openReadonlyDocument(uri).asFile()
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = delimiterChar
    }
    var reader: CSVReader =
      CSVReader.open(file, fileEncoding.getCharset.name())(CustomFormat)
    val firstRow: Array[String] = reader.iterator.next().toArray
    reader.close()

    reader = CSVReader.open(file, fileEncoding.getCharset.name())(CustomFormat)

    val header = if (hasHeader) 1 else 0
    val windowRows = reader.iterator
      .drop(header)
      .drop(windowOffset)
      .take(inferSampleSize)
      .map(_.toArray[Any])
      .toSeq
    reader.close()

    val sampleRows =
      if (windowRows.nonEmpty) windowRows
      else {
        val fromStart = CSVReader.open(file, fileEncoding.getCharset.name())(CustomFormat)
        try fromStart.iterator.drop(header).take(INFER_READ_LIMIT).map(_.toArray[Any]).toSeq
        finally fromStart.close()
      }
    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(sampleRows.iterator)

    CsvScanSupport.buildSchemaFromFirstRow(firstRow, attributeTypeList, hasHeader)
  }
}
