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

package org.apache.texera.amber.operator.source.scan.file

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.metadata.annotations.HideAnnotation
import org.apache.texera.amber.operator.source.scan.text.TextSourceOpDesc
import org.apache.texera.amber.operator.source.scan.{
  FileAttributeType,
  FileDecodingMethod,
  ScanSourceOpDesc
}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.net.URI
import java.nio.file.Paths

@JsonIgnoreProperties(value = Array("limit", "offset", "fileEncoding"))
class FileScanSourceOpDesc
    extends ScanSourceOpDesc
    with TextSourceOpDesc
    with StandaloneCodeGenerator {
  @JsonProperty(defaultValue = "UTF_8", required = true)
  @JsonSchemaTitle("Encoding")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "binary")
    )
  )
  private val encoding: FileDecodingMethod = FileDecodingMethod.UTF_8

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Extract")
  val extract: Boolean = false

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Include Filename")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "extract"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    )
  )
  val outputFileName: Boolean = false

  fileTypeName = Option("")

  override def generateStandaloneCode(): String = {
    val rawPath = fileName.getOrElse("")
    val basename = Paths.get(new URI(rawPath).getPath).getFileName.toString
    val col = attributeName
    val enc = encoding.toString.replace("_", "-").toLowerCase
    val basenameLit = pyStringLiteral(basename)
    val colLit = pyStringLiteral(col)
    val encLit = pyStringLiteral(enc)
    val buf = scala.collection.mutable.ArrayBuffer[String]()

    if (extract)
      buf += s"""# WARNING: extract=true is not supported in standalone mode; provide the unarchived $basenameLit directly."""

    val isBinary =
      attributeType == FileAttributeType.BINARY || attributeType == FileAttributeType.LARGE_BINARY

    if (attributeType.isSingle) {
      val openArgs =
        if (isBinary) s"""$basenameLit, "rb""""
        else s"""$basenameLit, "r", encoding=$encLit"""
      val dfCols =
        if (outputFileName) s"""{"filename": $basenameLit, $colLit: [_f.read()]}"""
        else s"""{$colLit: [_f.read()]}"""
      buf += s"""with open($openArgs) as _f:"""
      buf += s"""    out1df = pd.DataFrame($dfCols)"""
    } else {
      val castExpr = attributeType match {
        case FileAttributeType.INTEGER   => "int(l.rstrip())"
        case FileAttributeType.LONG      => "int(l.rstrip())"
        case FileAttributeType.DOUBLE    => "float(l.rstrip())"
        case FileAttributeType.BOOLEAN   => """l.rstrip().lower() == "true""""
        case FileAttributeType.TIMESTAMP => "pd.Timestamp(l.rstrip())"
        case _                           => """l.rstrip("\n")"""
      }
      val hasSlice = fileScanOffset.isDefined || fileScanLimit.isDefined
      if (hasSlice) {
        val start = fileScanOffset.getOrElse(0)
        val sliceExpr = fileScanLimit match {
          case Some(l) => s"_lines[$start:${start + l}]"
          case None    => s"_lines[$start:]"
        }
        val dfCols =
          if (outputFileName) s"""{"filename": $basenameLit, $colLit: $sliceExpr}"""
          else s"""{$colLit: $sliceExpr}"""
        buf += s"""with open($basenameLit, "r", encoding=$encLit) as _f:"""
        buf += s"""    _lines = [$castExpr for l in _f]"""
        buf += s"""    out1df = pd.DataFrame($dfCols)"""
      } else {
        val dfCols =
          if (outputFileName)
            s"""{"filename": $basenameLit, $colLit: [$castExpr for l in _f]}"""
          else s"""{$colLit: [$castExpr for l in _f]}"""
        buf += s"""with open($basenameLit, "r", encoding=$encLit) as _f:"""
        buf += s"""    out1df = pd.DataFrame($dfCols)"""
      }
    }

    buf.mkString("\n")
  }

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
          "org.apache.texera.amber.operator.source.scan.file.FileScanSourceOpExec",
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
    var schema = Schema()
    if (outputFileName) {
      schema = schema.add("filename", AttributeType.STRING)
    }
    schema.add(attributeName, attributeType.getType)
  }
}
