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

package org.apache.texera.amber.operator.source.scan.text

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.state.NoLoopVariable
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.StandaloneHelpers
import org.apache.texera.amber.operator.metadata.annotations.HideAnnotation
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.operator.source.scan.FileAttributeType
import org.apache.texera.amber.util.JSONUtils.objectMapper

/**
  * TextSourceOpDesc is a trait holding commonly used properties and functions used for variations of text input processing
  * Create new, identical limit and offset fields with additional annotations to make hideable binary attributes
  * and strings that are in SingleTuple mode will always read the entire input, so limit / offset are disabled in these cases
  */
trait TextSourceOpDesc {
  @JsonProperty(defaultValue = "string", required = true)
  @JsonSchemaTitle("Attribute Type")
  @JsonPropertyDescription(
    "The output field type. Each line becomes a separate tuple, except for " +
      "'single string' / 'binary' / 'large binary', where the entire text becomes one tuple."
  )
  var attributeType: FileAttributeType = FileAttributeType.STRING

  @JsonProperty(defaultValue = "line", required = true)
  @JsonSchemaTitle("Attribute Name")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @NoLoopVariable
  var attributeName: String = "line"

  @JsonProperty
  @JsonSchemaTitle("Limit (lines)")
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonPropertyDescription(
    "Maximum number of lines to output. Leave empty to read all lines. " +
      "(Ignored when reading the whole file as one tuple.)"
  )
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^binary$|^single string$"
      )
    )
  )
  var fileScanLimit: Option[Int] = None

  @JsonProperty
  @JsonSchemaTitle("Offset (lines)")
  @JsonPropertyDescription(
    "Number of lines to skip from the start before reading. " +
      "(Ignored when reading the whole file as one tuple.)"
  )
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^binary$|^single string$"
      )
    )
  )
  var fileScanOffset: Option[Int] = None
}

object TextSourceOpDesc {

  /** The call the per-line cast makes, over the loop variable the readers share.
    * A line is read as the BOOLEAN cast reads text, so the cast's helper serves both.
    */
  val BooleanParserCall: String = "_texera_cast_boolean(l)"

  /** A TIMESTAMP line is kept as text and parsed once the column is built, as
    * CSV and JSONL parse theirs. `pd.Timestamp` kept the nanoseconds and the
    * offset that the engine's DateParserUtils does not.
    */
  def parseTimestampColumn(attributeType: FileAttributeType, colLit: String): Seq[String] =
    if (attributeType == FileAttributeType.TIMESTAMP)
      Seq(s"out1df[$colLit] = _texera_text_to_timestamp(out1df[$colLit])")
    else Seq.empty

  /** The helpers the Python above calls for this attribute type. */
  def lineHelpers(attributeType: FileAttributeType): Seq[String] =
    attributeType match {
      case FileAttributeType.INTEGER | FileAttributeType.LONG | FileAttributeType.DOUBLE |
          FileAttributeType.BOOLEAN | FileAttributeType.TIMESTAMP =>
        Seq(StandaloneHelpers.AttributeCasts)
      case _ => Seq.empty
    }

  private[source] def isBinaryAttribute(attributeType: FileAttributeType): Boolean =
    attributeType == FileAttributeType.BINARY || attributeType == FileAttributeType.LARGE_BINARY

  private[source] def buildPhysicalOp(
      desc: SourceOperatorDescriptor,
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      execClassName: String
  ): PhysicalOp =
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        desc.operatorIdentifier,
        OpExecWithClassName(execClassName, objectMapper.writeValueAsString(desc))
      )
      .withInputPorts(desc.operatorInfo.inputPorts)
      .withOutputPorts(desc.operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ =>
          Map(desc.operatorInfo.outputPorts.head.id -> desc.sourceSchema())
        )
      )

  private[source] def buildFileScanSchema(
      attributeName: String,
      attributeType: FileAttributeType,
      outputFileName: Boolean
  ): Schema = {
    var schema = Schema()
    if (outputFileName) {
      schema = schema.add("filename", AttributeType.STRING)
    }
    schema.add(attributeName, attributeType.getType)
  }

  private[source] def lineCastExpr(
      attributeType: FileAttributeType,
      timestampExpr: String,
      defaultExpr: String
  ): String =
    attributeType match {
      case FileAttributeType.INTEGER   => "_texera_parse_int(l, 32)"
      case FileAttributeType.LONG      => "_texera_parse_int(l, 64)"
      case FileAttributeType.DOUBLE    => "_texera_java_double(l)"
      case FileAttributeType.BOOLEAN   => BooleanParserCall
      case FileAttributeType.TIMESTAMP => timestampExpr
      case _                           => defaultExpr
    }

  private[source] def sliceLinesExpr(
      baseExpr: String,
      offset: Option[Int],
      limit: Option[Int]
  ): String = {
    val dropped = offset.filter(_ > 0).fold(baseExpr)(o => s"$baseExpr[$o:]")
    limit.fold(dropped)(l => s"$dropped[:${l.max(0)}]")
  }

  private[source] def appendFileScanRows(
      buf: scala.collection.mutable.ArrayBuffer[String],
      indent: String,
      nameExpr: String,
      readWholeOf: String,
      linesOf: String,
      attributeType: FileAttributeType,
      emitFilename: Boolean,
      offset: Option[Int],
      limit: Option[Int]
  ): Unit = {
    if (attributeType.isSingle) {
      if (emitFilename) buf += s"${indent}_rows.append(($nameExpr, $readWholeOf))"
      else buf += s"${indent}_rows.append($readWholeOf)"
    } else {
      val castExpr = lineCastExpr(attributeType, "l.rstrip()", """l.rstrip("\n")""")
      val linesExpr =
        if (offset.isEmpty && limit.isEmpty) linesOf
        else sliceLinesExpr(s"$linesOf.readlines()", offset, limit)
      val row = if (emitFilename) s"($nameExpr, $castExpr)" else castExpr
      buf += s"${indent}_rows.extend($row for l in $linesExpr)"
    }
  }

  private[source] def buildZipExtractLines(sourceExpr: String): Seq[String] =
    Seq(
      s"with zipfile.ZipFile($sourceExpr) as _z:",
      "    for _name in _z.namelist():",
      """        if _name.startswith("__MACOSX"):""",
      "            continue",
      "        with _z.open(_name) as _f:"
    )

  private[source] def appendFileScanDataFrame(
      buf: scala.collection.mutable.ArrayBuffer[String],
      attributeType: FileAttributeType,
      colLit: String,
      emitFilename: Boolean
  ): Unit = {
    if (emitFilename) buf += s"""out1df = pd.DataFrame(_rows, columns=["filename", $colLit])"""
    else buf += s"""out1df = pd.DataFrame({$colLit: _rows})"""
    buf ++= parseTimestampColumn(attributeType, colLit)
  }

  private[source] def fileScanStandaloneHelpers(
      attributeType: FileAttributeType,
      decodeHelper: String
  ): Seq[String] =
    lineHelpers(attributeType) ++ (if (isBinaryAttribute(attributeType)) Seq.empty
                                   else Seq(decodeHelper))

  private[source] def fileScanStandaloneImports(
      attributeType: FileAttributeType,
      extract: Boolean
  ): Seq[String] = {
    val readsLines = !attributeType.isSingle && !isBinaryAttribute(attributeType)
    (if (readsLines) Seq("import io") else Seq.empty) ++
      (if (extract) Seq("import zipfile") else Seq.empty)
  }
}
