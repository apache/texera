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

package org.apache.texera.amber.operator.source.scan.smart

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{OutputPort, PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.io.IOException
import java.net.URI

class SmartFileSourceOpDesc extends ScanSourceOpDesc {

  @JsonProperty(defaultValue = "AUTO")
  @JsonSchemaTitle("Format")
  @JsonPropertyDescription("override automatic format detection")
  var formatOverride: SmartFileFormat = SmartFileFormat.AUTO

  @JsonProperty
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("CSV/TSV delimiter (auto-detected if empty)")
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  var customDelimiter: Option[String] = None

  @JsonProperty
  @JsonSchemaTitle("Has Header")
  @JsonPropertyDescription("first row contains column names (CSV/TSV/Excel)")
  @JsonDeserialize(contentAs = classOf[java.lang.Boolean])
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  var hasHeader: Option[Boolean] = None

  @JsonProperty
  @JsonSchemaTitle("Excel Sheet Name")
  @JsonPropertyDescription("for Excel files; leave empty to use the first sheet")
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  var sheetName: Option[String] = None

  @JsonProperty
  @JsonSchemaTitle("Flatten Nested JSON")
  @JsonPropertyDescription("flatten nested JSON objects and arrays into dot-notation columns")
  @JsonDeserialize(contentAs = classOf[java.lang.Boolean])
  @JsonInclude(JsonInclude.Include.NON_ABSENT)
  var flatten: Option[Boolean] = None

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Include Source File")
  @JsonPropertyDescription("append a source file column when reading folders")
  var includeSourceFile: Boolean = false

  @JsonProperty(defaultValue = "source_file")
  @JsonSchemaTitle("Source File Column")
  @JsonPropertyDescription("column name used when source file output is enabled")
  var sourceFileAttribute: String = "source_file"

  fileTypeName = Option("Smart")

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Smart Source",
      operatorDescription =
        "Auto-detects file format and schema for a file or a folder of similar files. Supports CSV, TSV, JSON, JSONL, Arrow, Parquet, Excel, images, and plain text.",
      operatorGroupName = OperatorGroupConstants.INPUT_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )

  @throws[IOException]
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.source.scan.smart.SmartFileSourceOpExec",
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
    if (!fileResolved()) return null
    withOptionalSourceFile(runInference().schema)
  }

  /** Run inference using the descriptor's own fields as overrides. */
  def runInference(): InferenceResult = {
    val overrides = InferenceOverrides(
      format = Option(formatOverride),
      delimiter = customDelimiter.flatMap(_.headOption),
      hasHeader = hasHeader,
      sheetName = sheetName,
      flatten = flatten
    )
    SmartFileInferencer.infer(
      new URI(fileName.get),
      fileEncoding.getCharset,
      overrides
    )
  }

  def withOptionalSourceFile(schema: Schema): Schema =
    if (includeSourceFile) schema.add(sourceFileAttribute, org.apache.texera.amber.core.tuple.AttributeType.STRING)
    else schema
}
