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
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.StandaloneCodeGenerator.SourceFilePlaceholder
import org.apache.texera.amber.operator.metadata.annotations.HideAnnotation
import org.apache.texera.amber.operator.source.scan.text.TextSourceOpDesc
import org.apache.texera.amber.operator.source.scan.{FileDecodingMethod, ScanSourceOpDesc}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral

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
  var encoding: FileDecodingMethod = FileDecodingMethod.UTF_8

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

  override def standaloneSourcePath(): Option[String] = fileName

  override def generateStandaloneCode(): String = {
    val enc = encoding.toString.replace("_", "-").toLowerCase
    val colLit = pyStringLiteral(attributeName)
    val encLit = pyStringLiteral(enc)
    val buf = scala.collection.mutable.ArrayBuffer[String]()

    val readWholeOf =
      if (TextSourceOpDesc.isBinaryAttribute(attributeType)) "_f.read()"
      else s"_texera_decode(_f.read(), $encLit)"
    val linesOf = s"io.StringIO(_texera_decode(_f.read(), $encLit), newline=None)"

    if (extract) {
      buf += "_rows = []"
      buf ++= TextSourceOpDesc.buildZipExtractLines(SourceFilePlaceholder)
      TextSourceOpDesc.appendFileScanRows(
        buf,
        " " * 12,
        "_name",
        readWholeOf,
        linesOf,
        attributeType,
        outputFileName,
        fileScanOffset,
        fileScanLimit
      )
      TextSourceOpDesc.appendFileScanDataFrame(buf, attributeType, colLit, outputFileName)
    } else if (attributeType.isSingle) {
      val dfCols =
        if (outputFileName) s"""{"filename": $SourceFilePlaceholder, $colLit: [$readWholeOf]}"""
        else s"""{$colLit: [$readWholeOf]}"""
      buf += s"""with open($SourceFilePlaceholder, "rb") as _f:"""
      buf += s"""    out1df = pd.DataFrame($dfCols)"""
      buf ++= TextSourceOpDesc.parseTimestampColumn(attributeType, colLit)
    } else {
      val castExpr =
        TextSourceOpDesc.lineCastExpr(attributeType, "l.rstrip()", """l.rstrip("\n")""")
      val linesExpr =
        if (fileScanOffset.isEmpty && fileScanLimit.isEmpty) linesOf
        else TextSourceOpDesc.sliceLinesExpr(s"$linesOf.readlines()", fileScanOffset, fileScanLimit)
      val dfCols =
        if (outputFileName)
          s"""{"filename": $SourceFilePlaceholder, $colLit: [$castExpr for l in $linesExpr]}"""
        else s"""{$colLit: [$castExpr for l in $linesExpr]}"""
      buf += s"""with open($SourceFilePlaceholder, "rb") as _f:"""
      buf += s"""    out1df = pd.DataFrame($dfCols)"""
      buf ++= TextSourceOpDesc.parseTimestampColumn(attributeType, colLit)
    }

    buf.mkString("\n")
  }

  override def standaloneHelpers(): Seq[String] =
    TextSourceOpDesc.fileScanStandaloneHelpers(attributeType, FileScanOpDesc.DecodeHelper)

  override def standaloneImports(): Seq[String] =
    TextSourceOpDesc.fileScanStandaloneImports(attributeType, extract)

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    TextSourceOpDesc.buildPhysicalOp(
      this,
      workflowId,
      executionId,
      "org.apache.texera.amber.operator.source.scan.file.FileScanSourceOpExec"
    )

  override def sourceSchema(): Schema =
    TextSourceOpDesc.buildFileScanSchema(attributeName, attributeType, outputFileName)
}
