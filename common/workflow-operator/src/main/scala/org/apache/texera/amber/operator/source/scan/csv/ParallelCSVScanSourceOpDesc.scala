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
import org.apache.texera.amber.core.storage.DocumentFactory
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.inferSchemaFromRows
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.operator.metadata.annotations.UIWidget
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.operator.{StandaloneCodeGenerator, StandaloneHelpers}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral

import java.io.IOException

class ParallelCSVScanSourceOpDesc extends ScanSourceOpDesc with StandaloneCodeGenerator {

  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("single character separating the fields on each line")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @JsonSchemaInject(json = UIWidget.UIWidgetCharDelimiter)
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
    if (customDelimiter.forall(_.isEmpty)) {
      customDelimiter = Option(",")
    }
    CsvScanSupport.csvPhysicalOp(
      this,
      workflowId,
      executionId,
      "org.apache.texera.amber.operator.source.scan.csv.ParallelCSVScanSourceOpExec",
      parallelizable = true
    )
  }

  override def standaloneSourcePath(): Option[String] = fileName

  override def standaloneHelpers(): Seq[String] = Seq(StandaloneHelpers.AttributeCasts)

  override def generateStandaloneCode(): String = {
    val ignoredWindow =
      if (offset.isEmpty && limit.isEmpty) Seq.empty
      else
        Seq(
          "# NOTE: this operator's limit and offset are ignored, as the parallel CSV reader ignores them."
        )
    CsvScanSupport.generateStandaloneCode(
      this,
      customDelimiter,
      hasHeader,
      dtypeByType = Map(AttributeType.LONG -> "Int64", AttributeType.STRING -> "string"),
      includeNaValuesBlank = true,
      includeWindow = false,
      declaredSchemaBody = { (readCall, declared) =>
        val names = declared.getAttributes.map(a => pyStringLiteral(a.getName))
        Seq(
          readCall,
          s"out1df.columns = [${names.mkString(", ")}]",
          StandaloneCodeGenerator.parseDoubles("out1df", declared)
        ).filter(_.nonEmpty).mkString("\n")
      },
      ignoredWindowPrefix = ignoredWindow
    )
  }

  override def sourceSchema(): Schema = {
    val (uri, delimiterChar) = CsvScanSupport.resolveCsvUriAndDelimiter(this, customDelimiter)
    val file = DocumentFactory.openReadonlyDocument(uri).asFile()
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = delimiterChar
    }
    var reader: CSVReader = CSVReader.open(file)(CustomFormat)
    val firstRow: Array[String] = reader.iterator.next().toArray
    reader.close()

    reader = CSVReader.open(file.toPath.toString)(CustomFormat)
    if (hasHeader) reader.readNext()

    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
      reader.iterator.take(inferSampleSize).map(seq => seq.toArray)
    )
    reader.close()

    CsvScanSupport.buildSchemaFromFirstRow(firstRow, attributeTypeList, hasHeader)
  }
}
